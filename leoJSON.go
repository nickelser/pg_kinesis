package main

type leoJSON struct {
	ID                   string      `json:"id"`    // BOT name
	Event                string      `json:"event"` // QUEUE name
	Payload              defaultJSON `json:"payload"`
	EventSourceTimestamp int64       `json:"event_source_timestamp"` // Time of origination of payload source
	Timestamp            int64       `json:"timestamp"`              // Time of execution
}
