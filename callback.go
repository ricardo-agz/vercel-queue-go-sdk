package queue

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"
)

const (
	cloudEventTypeV1Beta = "com.vercel.queue.v1beta"
	cloudEventTypeV2Beta = "com.vercel.queue.v2beta"
)

// parsedCallback holds the topic/consumer/message coordinates extracted from a
// queue callback, plus an inline payload when the callback carries one.
type parsedCallback struct {
	topic         string
	consumer      string
	messageID     string
	receiptHandle string
	deliveryCount int
	createdAt     time.Time
	body          []byte // inline payload; nil for metadata-only callbacks
	inline        bool
}

// isQueueCallback reports whether r looks like a queue callback (either format).
func isQueueCallback(r *http.Request) bool {
	if r.Header.Get("Ce-Type") == cloudEventTypeV2Beta {
		return true
	}
	return strings.Contains(r.Header.Get("Content-Type"), "application/cloudevents+json")
}

// parseCallback extracts callback coordinates from the request and raw body.
func parseCallback(r *http.Request, body []byte) (*parsedCallback, error) {
	if r.Header.Get("Ce-Type") == cloudEventTypeV2Beta {
		return parseV2Beta(r, body)
	}
	return parseV1Beta(body)
}

// parseV2Beta parses the binary content-mode callback (ce-* headers).
func parseV2Beta(r *http.Request, body []byte) (*parsedCallback, error) {
	topic := r.Header.Get("Ce-Vqsqueuename")
	consumer := r.Header.Get("Ce-Vqsconsumergroup")
	messageID := r.Header.Get("Ce-Vqsmessageid")
	if topic == "" || consumer == "" || messageID == "" {
		return nil, errors.New("queue: missing required ce-vqs* headers")
	}
	receipt := r.Header.Get("Ce-Vqsreceipthandle")
	pc := &parsedCallback{
		topic:         topic,
		consumer:      consumer,
		messageID:     messageID,
		receiptHandle: receipt,
		deliveryCount: atoiOr(r.Header.Get("Ce-Vqsdeliverycount"), 1),
		createdAt:     parseTime(r.Header.Get("Ce-Vqscreatedat")),
	}
	// A receipt handle present means the payload is delivered inline.
	if receipt != "" {
		pc.body = body
		pc.inline = true
	}
	return pc, nil
}

// parseV1Beta parses the JSON CloudEvent callback. It never carries an inline
// payload, so the message must be fetched by id.
func parseV1Beta(body []byte) (*parsedCallback, error) {
	if len(body) == 0 {
		return nil, errors.New("queue: empty request body")
	}
	var ce struct {
		Type string `json:"type"`
		Data struct {
			QueueName     string `json:"queueName"`
			ConsumerGroup string `json:"consumerGroup"`
			MessageID     string `json:"messageId"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &ce); err != nil {
		return nil, errors.New("queue: failed to parse CloudEvent body")
	}
	if ce.Type != cloudEventTypeV1Beta {
		return nil, errors.New("queue: unexpected CloudEvent type " + ce.Type)
	}
	if ce.Data.QueueName == "" || ce.Data.ConsumerGroup == "" || ce.Data.MessageID == "" {
		return nil, errors.New("queue: missing required CloudEvent data fields")
	}
	return &parsedCallback{
		topic:     ce.Data.QueueName,
		consumer:  ce.Data.ConsumerGroup,
		messageID: ce.Data.MessageID,
	}, nil
}
