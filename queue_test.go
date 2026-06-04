package queue

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type emailPayload struct {
	To string `json:"to"`
}

var emails = NewTopic[emailPayload]("emails")

func TestTopicSend(t *testing.T) {
	var gotAuth, gotIdem, gotDelay string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/api/v3/topic/emails") {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		gotAuth = r.Header.Get("Authorization")
		gotIdem = r.Header.Get("Vqs-Idempotency-Key")
		gotDelay = r.Header.Get("Vqs-Delay-Seconds")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]string{"messageId": "m1"})
	}))
	defer srv.Close()

	client := NewClient(WithBaseURL(srv.URL), WithToken("tok"), WithoutDeploymentPinning())
	res, err := emails.Send(context.Background(), client, emailPayload{To: "a@b.com"},
		WithIdempotencyKey("k1"), WithDelay(60*time.Second))
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if res.MessageID != "m1" {
		t.Errorf("messageId = %q, want m1", res.MessageID)
	}
	if gotAuth != "Bearer tok" {
		t.Errorf("auth = %q", gotAuth)
	}
	if gotIdem != "k1" {
		t.Errorf("idempotency = %q", gotIdem)
	}
	if gotDelay != "60" {
		t.Errorf("delay = %q, want 60", gotDelay)
	}
}

func TestSendDuplicateIdempotencyKey(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
	}))
	defer srv.Close()

	client := NewClient(WithBaseURL(srv.URL), WithToken("tok"), WithoutDeploymentPinning())
	_, err := emails.Send(context.Background(), client, emailPayload{To: "a@b.com"})
	if !errors.Is(err, ErrDuplicateIdempotencyKey) {
		t.Fatalf("err = %v, want ErrDuplicateIdempotencyKey", err)
	}
}

// fakeQueue records lease operations during dispatch tests.
type fakeQueue struct {
	deleted  bool
	patched  bool
	patchVis int
}

func (f *fakeQueue) handler(t *testing.T) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodDelete:
			f.deleted = true
			w.WriteHeader(http.StatusNoContent)
		case http.MethodPatch:
			f.patched = true
			body, _ := io.ReadAll(r.Body)
			var p struct {
				VisibilityTimeoutSeconds int `json:"visibilityTimeoutSeconds"`
			}
			_ = json.Unmarshal(body, &p)
			f.patchVis = p.VisibilityTimeoutSeconds
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected lease method %s", r.Method)
		}
	})
}

func v2betaRequest(topic, consumer, msgID, receipt, body string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Ce-Type", cloudEventTypeV2Beta)
	req.Header.Set("Ce-Vqsqueuename", topic)
	req.Header.Set("Ce-Vqsconsumergroup", consumer)
	req.Header.Set("Ce-Vqsmessageid", msgID)
	req.Header.Set("Ce-Vqsreceipthandle", receipt)
	req.Header.Set("Ce-Vqsdeliverycount", "1")
	req.Header.Set("Content-Type", "application/json")
	return req
}

func newConsumerMux(t *testing.T, srvURL string) (*ServeMux, *Client) {
	client := NewClient(WithBaseURL(srvURL), WithToken("tok"), WithoutDeploymentPinning())
	mux := NewServeMux(WithConsumerClient(client), WithRefreshInterval(0))
	return mux, client
}

func TestDispatchInlineAck(t *testing.T) {
	fq := &fakeQueue{}
	srv := httptest.NewServer(fq.handler(t))
	defer srv.Close()

	mux, _ := newConsumerMux(t, srv.URL)

	var got emailPayload
	Handle(mux, emails, func(_ context.Context, m *Message[emailPayload]) error {
		got = m.Payload
		if m.Metadata.MessageID != "m1" || m.Metadata.Topic != "emails" {
			t.Errorf("metadata = %+v", m.Metadata)
		}
		return nil
	})

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, v2betaRequest("emails", "c1", "m1", "rh1", `{"to":"a@b.com"}`))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if got.To != "a@b.com" {
		t.Errorf("payload.To = %q", got.To)
	}
	if !fq.deleted {
		t.Errorf("expected message to be acked (deleted)")
	}
}

func TestDispatchRetryAfter(t *testing.T) {
	fq := &fakeQueue{}
	srv := httptest.NewServer(fq.handler(t))
	defer srv.Close()

	mux, _ := newConsumerMux(t, srv.URL)
	Handle(mux, emails, func(_ context.Context, _ *Message[emailPayload]) error {
		return RetryAfter(errors.New("rate limited"), 90*time.Second)
	})

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, v2betaRequest("emails", "c1", "m1", "rh1", `{"to":"a@b.com"}`))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	if !fq.patched || fq.patchVis != 90 {
		t.Errorf("expected visibility extended to 90, got patched=%v vis=%d", fq.patched, fq.patchVis)
	}
	if fq.deleted {
		t.Errorf("message should not be deleted on retry-after")
	}
}

func TestDispatchDefaultRetry(t *testing.T) {
	fq := &fakeQueue{}
	srv := httptest.NewServer(fq.handler(t))
	defer srv.Close()

	mux, _ := newConsumerMux(t, srv.URL)
	Handle(mux, emails, func(_ context.Context, _ *Message[emailPayload]) error {
		return errors.New("boom")
	})

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, v2betaRequest("emails", "c1", "m1", "rh1", `{"to":"a@b.com"}`))

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	if fq.deleted || fq.patched {
		t.Errorf("default retry must not ack or extend (deleted=%v patched=%v)", fq.deleted, fq.patched)
	}
}

func TestDispatchDrop(t *testing.T) {
	fq := &fakeQueue{}
	srv := httptest.NewServer(fq.handler(t))
	defer srv.Close()

	mux, _ := newConsumerMux(t, srv.URL)
	Handle(mux, emails, func(_ context.Context, _ *Message[emailPayload]) error {
		return Drop(errors.New("permanent"))
	})

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, v2betaRequest("emails", "c1", "m1", "rh1", `{"to":"a@b.com"}`))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	if !fq.deleted {
		t.Errorf("drop must ack (delete) the message")
	}
}

func TestHealthcheck(t *testing.T) {
	mux := NewServeMux(WithRefreshInterval(0))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusOK || rec.Body.String() != "ok" {
		t.Fatalf("healthcheck: code=%d body=%q", rec.Code, rec.Body.String())
	}
}

func TestNoMatchingSubscriber(t *testing.T) {
	mux := NewServeMux(WithRefreshInterval(0))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, v2betaRequest("unknown", "c1", "m1", "rh1", `{}`))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
}

func TestClassify(t *testing.T) {
	if d, _ := classify(nil); d != dispositionAck {
		t.Errorf("nil should ack")
	}
	if d, after := classify(RetryAfter(nil, 5*time.Second)); d != dispositionRetryAfter || after != 5*time.Second {
		t.Errorf("retry-after misclassified: %v %v", d, after)
	}
	if d, _ := classify(Drop(nil)); d != dispositionAck {
		t.Errorf("drop should ack")
	}
	if d, _ := classify(errors.New("x")); d != dispositionRetry {
		t.Errorf("plain error should retry")
	}
}
