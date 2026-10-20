package slack

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	slackapi "github.com/slack-go/slack"
	"github.com/slack-go/slack/slackevents"
)

func completionReactionEvent() *slackevents.ReactionAddedEvent {
	return &slackevents.ReactionAddedEvent{User: "UALICE", Reaction: "white_check_mark", EventTimestamp: "1700000001.000100",
		Item: slackevents.Item{Type: "message", Channel: "C123", Timestamp: "1700000000.000100"}}
}

type fakeAutopilotReactionClient struct {
	user  *slackapi.User
	item  slackapi.ReactedItem
	err   error
	reads int
}

func (f *fakeAutopilotReactionClient) GetUserInfoContext(context.Context, string) (*slackapi.User, error) {
	return f.user, nil
}
func (f *fakeAutopilotReactionClient) GetReactionsContext(_ context.Context, ref slackapi.ItemRef, _ slackapi.GetReactionsParameters) (slackapi.ReactedItem, error) {
	f.reads++
	if ref.Channel != "C123" || ref.Timestamp != "1700000000.000101" {
		return slackapi.ReactedItem{}, errors.New("wrong reaction message")
	}
	return f.item, f.err
}

func TestResolveAutopilotReactionChecksActorAndExactMessage(t *testing.T) {
	event := AutopilotReactionEvent{ChannelID: "C123", UserID: "UHUMAN", MessageTS: "1700000000.000101"}
	for _, name := range []string{"root", "solution", "bot", "app", "deleted actor", "missing message", "wrong channel", "wrong timestamp", "retry"} {
		t.Run(name, func(t *testing.T) {
			api := &fakeAutopilotReactionClient{user: &slackapi.User{ID: event.UserID}, item: slackapi.ReactedItem{Item: slackapi.Item{Type: "message", Channel: event.ChannelID, Message: &slackapi.Message{Msg: slackapi.Msg{Timestamp: event.MessageTS}}}}}
			switch name {
			case "solution":
				api.item.Message.ThreadTimestamp = "1700000000.000100"
			case "bot":
				api.user.IsBot = true
			case "app":
				api.user.IsAppUser = true
			case "deleted actor":
				api.user.Deleted = true
			case "missing message":
				api.err = errors.New("message_not_found")
			case "wrong channel":
				api.item.Channel = "COTHER"
			case "wrong timestamp":
				api.item.Message.Timestamp = "1700000000.000999"
			case "retry":
				api.err = errors.New("ratelimited")
			}
			target, err := resolveAutopilotReaction(context.Background(), api, "UBOT", event)
			if name == "retry" {
				if err == nil {
					t.Fatal("Slack failure must retry")
				}
				return
			}
			want := name == "root" || name == "solution"
			if err != nil || target.Exists != want || target.Human != want {
				t.Fatalf("target=%+v err=%v", target, err)
			}
			if want {
				root := event.MessageTS
				if name == "solution" {
					root = "1700000000.000100"
				}
				if target.ThreadTS != root {
					t.Fatalf("thread=%s want=%s", target.ThreadTS, root)
				}
			} else if (name == "bot" || name == "app" || name == "deleted actor") && api.reads != 0 {
				t.Fatal("nonhuman actor caused message read")
			}
		})
	}
}

func TestParseAutopilotCompletionStatusOptIn(t *testing.T) {
	route := validAutopilotRoute()
	for _, key := range []string{"", "ai_done", "AI Done", "ai-done", " too_long_status_key_that_is_not_valid"} {
		route.CompletionStatusKey = key
		raw, _ := json.Marshal([]AutopilotRoute{route})
		got, err := ParseAutopilotRoutes(string(raw))
		valid := key == "" || key == "ai_done"
		if valid && (err != nil || len(got) != 1 || got[0].CompletionStatusKey != key) || !valid && err == nil {
			t.Fatalf("key=%q got=%+v err=%v", key, got, err)
		}
	}
}

func TestAutopilotCompletionReactionParser(t *testing.T) {
	for _, name := range []string{"white_check_mark", "heavy_check_mark", "checkmark"} {
		event := completionReactionEvent()
		event.Reaction = name
		parsed, ok := autopilotReactionFrom(eventsAPI(event), "UBOT")
		if !ok || parsed.Reaction != name || parsed.UserID != event.User || parsed.MessageTS != event.Item.Timestamp || parsed.EventTS != event.EventTimestamp {
			t.Fatalf("reaction %s parsed=%+v ok=%v", name, parsed, ok)
		}
	}
	for _, change := range []func(*slackevents.ReactionAddedEvent){
		func(e *slackevents.ReactionAddedEvent) { e.Reaction = "eyes" },
		func(e *slackevents.ReactionAddedEvent) { e.User = "UBOT" },
		func(e *slackevents.ReactionAddedEvent) { e.User = "" },
		func(e *slackevents.ReactionAddedEvent) { e.Item.Type = "file" },
		func(e *slackevents.ReactionAddedEvent) { e.Item.Channel = "D123" },
		func(e *slackevents.ReactionAddedEvent) { e.Item.Timestamp = "bad" },
		func(e *slackevents.ReactionAddedEvent) { e.EventTimestamp = "" },
	} {
		event := completionReactionEvent()
		change(event)
		if _, ok := autopilotReactionFrom(eventsAPI(event), "UBOT"); ok {
			t.Fatalf("accepted reaction: %+v", event)
		}
	}
	if _, ok := autopilotReactionFrom(eventsAPI(&slackevents.ReactionRemovedEvent{}), "UBOT"); ok {
		t.Fatal("reaction removal must never close or reopen an issue")
	}
}

type autopilotReactionTestHandler struct {
	autopilotTestHandler
	reaction func(context.Context, AutopilotReactionEvent) (bool, error)
}

func (h autopilotReactionTestHandler) HandleAutopilotReaction(ctx context.Context, event AutopilotReactionEvent) (bool, error) {
	return h.reaction(ctx, event)
}

func TestAutopilotReactionPersistsBeforeACK(t *testing.T) {
	for _, fail := range []bool{false, true} {
		var calls []string
		want := errors.New("durable reaction intake unavailable")
		c := &slackChannel{botUserID: "UBOT", autopilot: autopilotReactionTestHandler{
			autopilotTestHandler: func(context.Context, AutopilotEvent) (bool, error) {
				t.Fatal("reaction reached report handler")
				return false, nil
			},
			reaction: func(context.Context, AutopilotReactionEvent) (bool, error) {
				calls = append(calls, "persist")
				if fail {
					return true, want
				}
				return true, nil
			},
		}}
		err := c.processEventsAPI(context.Background(), eventsAPI(completionReactionEvent()), nil, func() { calls = append(calls, "ack") })
		expected := []string{"persist", "ack"}
		if fail {
			expected = []string{"persist"}
		}
		if !reflect.DeepEqual(calls, expected) || (fail && !errors.Is(err, want)) || (!fail && err != nil) {
			t.Fatalf("fail=%v calls=%v err=%v", fail, calls, err)
		}
	}
}
