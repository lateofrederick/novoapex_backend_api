package queue

import "context"

// OutboundJob is the outbound-queue payload: every sender persists an
// outbound_messages row first, then publishes only its id
// (outboundQueue.add('send-message', {outboundMessageId})).
type OutboundJob struct {
	OutboundMessageID string `json:"outboundMessageId"`
}

// OutboundTaskID is the single dedup key for sending one outbound row. Every
// producer — first publish and outbox-sweep recovery alike — uses it, so a
// sweep that races an in-flight (queued, retrying or active) send collapses
// into the existing task instead of double-sending the message.
func OutboundTaskID(outboundMessageID string) string {
	return "outbound:" + outboundMessageID
}

// PublishOutbound enqueues the send for an already-persisted outbound row.
func PublishOutbound(ctx context.Context, pub Publisher, outboundMessageID string) error {
	return pub.Enqueue(ctx, QOutbound, TaskOutboundSend,
		OutboundJob{OutboundMessageID: outboundMessageID},
		&EnqueueOpts{TaskID: OutboundTaskID(outboundMessageID)})
}
