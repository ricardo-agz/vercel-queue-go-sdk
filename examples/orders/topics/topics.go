// Package topics declares the queue topics shared by the producer (web) and
// consumer (worker) services. Declaring them here gives the topic name and the
// payload type a single source of truth.
package topics

import "github.com/vercel/queue-go"

// EmailPayload is the message body for the emails topic.
type EmailPayload struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
}

// ReportPayload is the message body for the reports topic.
type ReportPayload struct {
	ReportID string `json:"reportId"`
}

var (
	// Emails is the topic for outbound transactional email jobs.
	Emails = queue.NewTopic[EmailPayload]("emails")
	// Reports is the topic for report-generation jobs.
	Reports = queue.NewTopic[ReportPayload]("reports")
)
