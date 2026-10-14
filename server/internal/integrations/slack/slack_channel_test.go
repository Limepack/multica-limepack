package slack

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/slack-go/slack/slackevents"

	"github.com/multica-ai/multica/server/internal/integrations/channel"
)

type autopilotTestHandler func(context.Context, AutopilotEvent) (bool, error)

func (f autopilotTestHandler) HandleAutopilotEvent(ctx context.Context, event AutopilotEvent) (bool, error) {
	return f(ctx, event)
}

func TestAutopilotReportPersistsBeforeACKAndSkipsChat(t *testing.T) {
	for _, inner := range []any{
		&slackevents.MessageEvent{Channel: "C123", User: "UALICE", TimeStamp: "1700000000.000100", Text: "<@UBOT> checkout fails"},
		&slackevents.AppMentionEvent{Channel: "C123", User: "UALICE", TimeStamp: "1700000000.000100", Text: "<@UBOT> checkout fails"},
	} {
		var calls []string
		c := &slackChannel{
			botUserID: "UBOT",
			autopilot: autopilotTestHandler(func(_ context.Context, event AutopilotEvent) (bool, error) {
				calls = append(calls, "persist")
				if event.MessageTS != "1700000000.000100" || event.ChannelID != "C123" {
					t.Fatalf("incorrect report: %+v", event)
				}
				return true, nil
			}),
			handler: func(context.Context, channel.InboundMessage) error {
				calls = append(calls, "chat")
				return nil
			},
		}
		if err := c.processEventsAPI(context.Background(), eventsAPI(inner), compileMentionRe("UBOT"), func() { calls = append(calls, "ack") }); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(calls, []string{"persist", "ack"}) {
			t.Fatalf("calls = %v; matched reports must not reach chat", calls)
		}
	}
}

func TestAutopilotFailureLeavesEnvelopeUnacknowledged(t *testing.T) {
	want := errors.New("durable intake unavailable")
	var acked, chatted bool
	c := &slackChannel{
		autopilot: autopilotTestHandler(func(context.Context, AutopilotEvent) (bool, error) { return true, want }),
		handler:   func(context.Context, channel.InboundMessage) error { chatted = true; return nil },
	}
	inner := &slackevents.MessageEvent{Channel: "C123", User: "UALICE", TimeStamp: "1700000000.000100", Text: "checkout fails"}
	err := c.processEventsAPI(context.Background(), eventsAPI(inner), nil, func() { acked = true })
	if !errors.Is(err, want) || acked || chatted {
		t.Fatalf("err=%v acked=%v chatted=%v; failed intake must reconnect without ACK/chat", err, acked, chatted)
	}
}

func TestUnmatchedReportsAndDMsPreserveACKFirstChat(t *testing.T) {
	for _, inner := range []any{
		&slackevents.MessageEvent{Channel: "COTHER", User: "UALICE", TimeStamp: "1700000000.000100", Text: "<@UBOT> hello"},
		&slackevents.AppMentionEvent{Channel: "COTHER", User: "UALICE", TimeStamp: "1700000000.000100", Text: "<@UBOT> hello"},
		&slackevents.MessageEvent{Channel: "D123", User: "UALICE", TimeStamp: "1700000000.000100", Text: "hello", ChannelType: "im"},
	} {
		var calls []string
		c := &slackChannel{
			botUserID: "UBOT",
			autopilot: autopilotTestHandler(func(context.Context, AutopilotEvent) (bool, error) { return false, nil }),
			handler: func(_ context.Context, msg channel.InboundMessage) error {
				calls = append(calls, "chat")
				if !msg.AddressedToBot {
					t.Fatal("normal DM/mention lost addressing")
				}
				return nil
			},
		}
		if err := c.processEventsAPI(context.Background(), eventsAPI(inner), compileMentionRe("UBOT"), func() { calls = append(calls, "ack") }); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(calls, []string{"ack", "chat"}) {
			t.Fatalf("calls = %v, want ordinary ACK-first behavior", calls)
		}
	}
}

func TestThreadAndSystemEventsNeverReachAutopilot(t *testing.T) {
	for _, inner := range []any{
		(*slackevents.MessageEvent)(nil),
		(*slackevents.AppMentionEvent)(nil),
		&slackevents.MessageEvent{Channel: "C123", User: "UALICE", TimeStamp: "1700000000.000100", ThreadTimeStamp: "1699999999.000100", Text: "<@UBOT> follow up"},
		&slackevents.AppMentionEvent{Channel: "C123", User: "UALICE", TimeStamp: "1700000000.000100", ThreadTimeStamp: "1699999999.000100", Text: "<@UBOT> follow up"},
		&slackevents.MessageEvent{Channel: "C123", User: "UALICE", TimeStamp: "1700000000.000100", Text: "changed", SubType: "message_changed"},
		&slackevents.MessageEvent{Channel: "C123", User: "UALICE", TimeStamp: "1700000000.000100", Text: "bot", BotID: "BOTHER"},
	} {
		var acked bool
		c := &slackChannel{
			botUserID: "UBOT",
			autopilot: autopilotTestHandler(func(context.Context, AutopilotEvent) (bool, error) {
				t.Fatal("ineligible event reached Autopilot")
				return false, nil
			}),
			handler: func(context.Context, channel.InboundMessage) error {
				if !acked {
					t.Fatal("chat before ACK")
				}
				return nil
			},
		}
		if err := c.processEventsAPI(context.Background(), eventsAPI(inner), compileMentionRe("UBOT"), func() { acked = true }); err != nil || !acked {
			t.Fatalf("ordinary event err=%v acked=%v", err, acked)
		}
	}
}
