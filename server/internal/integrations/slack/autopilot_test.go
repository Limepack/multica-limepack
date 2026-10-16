package slack

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/slack-go/slack"
	"github.com/slack-go/slack/slackevents"

	"github.com/multica-ai/multica/server/internal/integrations/channel"
)

func validAutopilotRoute() AutopilotRoute {
	return AutopilotRoute{
		WorkspaceID: "550e8400-e29b-41d4-a716-446655440000",
		AgentID:     "550e8400-e29b-41d4-a716-446655440001",
		AutopilotID: "550e8400-e29b-41d4-a716-446655440002",
		TriggerID:   "550e8400-e29b-41d4-a716-446655440003",
		TeamID:      "T1", ChannelID: "C123",
	}
}

func TestParseAutopilotRoutes(t *testing.T) {
	route := validAutopilotRoute()
	raw, err := json.Marshal([]AutopilotRoute{route})
	if err != nil {
		t.Fatal(err)
	}
	got, err := ParseAutopilotRoutes(string(raw))
	if err != nil || !reflect.DeepEqual(got, []AutopilotRoute{route}) {
		t.Fatalf("routes = %+v, error = %v", got, err)
	}
	for _, raw := range []string{"", " \n ", "[]"} {
		got, err := ParseAutopilotRoutes(raw)
		if err != nil || len(got) != 0 {
			t.Errorf("disabled routes %q = %+v, error = %v", raw, got, err)
		}
	}
}

func TestParseAutopilotRoutesRejectsMalformedConfig(t *testing.T) {
	route := validAutopilotRoute()
	raw, _ := json.Marshal([]AutopilotRoute{route})
	valid := string(raw)
	for name, raw := range map[string]string{
		"null":          "null",
		"object":        "{}",
		"trailing":      valid + " []",
		"trailing junk": valid + " garbage",
		"unknown field": strings.Replace(valid, `"team_id":`, `"typo":`, 1),
		"missing UUID":  strings.Replace(valid, route.WorkspaceID, "", 1),
		"invalid UUID":  strings.Replace(valid, route.AgentID, "not-a-uuid", 1),
		"compact UUID":  strings.Replace(valid, route.AutopilotID, strings.ReplaceAll(route.AutopilotID, "-", ""), 1),
		"zero UUID":     strings.Replace(valid, route.TriggerID, "00000000-0000-0000-0000-000000000000", 1),
		"invalid team":  strings.Replace(valid, `"T1"`, `"T1 "`, 1),
		"DM channel":    strings.Replace(valid, `"C123"`, `"D123"`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseAutopilotRoutes(raw); err == nil {
				t.Fatalf("accepted malformed configuration %q", raw)
			}
		})
	}
	for _, field := range []string{"workspace_id", "agent_id", "autopilot_id", "trigger_id"} {
		t.Run(field, func(t *testing.T) {
			var values map[string]any
			encoded, _ := json.Marshal(route)
			_ = json.Unmarshal(encoded, &values)
			values[field] = "bad"
			encoded, _ = json.Marshal([]any{values})
			if _, err := ParseAutopilotRoutes(string(encoded)); err == nil {
				t.Errorf("accepted invalid %s", field)
			}
		})
	}
}

func TestParseAutopilotRoutesRejectsDuplicateInstallationChannel(t *testing.T) {
	first := validAutopilotRoute()
	second := first
	second.AutopilotID = "550e8400-e29b-41d4-a716-446655440005"
	encoded, _ := json.Marshal([]AutopilotRoute{first, second})
	if _, err := ParseAutopilotRoutes(string(encoded)); err == nil {
		t.Fatal("accepted two targets for the same installation/team/channel")
	}
	second.AgentID = "550e8400-e29b-41d4-a716-446655440004"
	encoded, _ = json.Marshal([]AutopilotRoute{first, second})
	if _, err := ParseAutopilotRoutes(string(encoded)); err != nil {
		t.Fatalf("different agents can have different routes: %v", err)
	}
	second.TeamID = "T2"
	encoded, _ = json.Marshal([]AutopilotRoute{first, second})
	if _, err := ParseAutopilotRoutes(string(encoded)); err != nil {
		t.Fatalf("same channel ID in different teams should be valid: %v", err)
	}
}

func TestAutopilotInboundMessagePreservesMediaAndSource(t *testing.T) {
	report := AutopilotEvent{
		AppID: "A1", TeamID: "T1", ChannelID: "C123", UserID: "UALICE", MessageTS: "1700000000.000100", Text: "<@UBOT> checkout fails",
		Files: []AutopilotFile{
			{ID: "F1", DownloadURL: "https://files.slack.com/file.png"},
			{ID: "F2", DownloadURL: "https://example.com/file.png"},
		},
	}
	msg := AutopilotInboundMessage(report)
	if msg.MessageID != report.MessageTS || msg.Source.ThreadID != report.MessageTS || msg.Source.ChatID != report.ChannelID || msg.Source.SenderID != report.UserID || msg.Source.ChannelType != TypeSlack || msg.Source.ChatType != channel.ChatTypeGroup || msg.Text != report.Text {
		t.Fatalf("incorrect media envelope: %+v", msg)
	}
	var raw slackRawEvent
	if err := json.Unmarshal(msg.Raw, &raw); err != nil {
		t.Fatal(err)
	}
	if raw.APIAppID != report.AppID || raw.TeamID != report.TeamID || len(raw.Files) != 1 || raw.Files[0].ID != "F1" {
		t.Fatalf("incorrect raw media metadata: %+v", raw)
	}
}

func TestAutopilotInboundMessagePreservesReplyThread(t *testing.T) {
	report := AutopilotEvent{AppID: "A1", TeamID: "T1", ChannelID: "C123", UserID: "UALICE", MessageTS: "1700000000.000101", ThreadTS: "1700000000.000100", Text: "The failure also affects another browser."}
	msg := AutopilotInboundMessage(report)
	if msg.MessageID != report.MessageTS || msg.Source.ThreadID != report.ThreadTS || msg.ReplyTo == nil || msg.ReplyTo.MessageID != report.ThreadTS || msg.ReplyTo.RootID != report.ThreadTS {
		t.Fatalf("reply lost its report thread: %+v", msg)
	}
	encoded, err := json.Marshal(report)
	if err != nil || !strings.Contains(string(encoded), `"thread_ts":"1700000000.000100"`) {
		t.Fatalf("reply thread missing from durable payload: %s err=%v", encoded, err)
	}
}

func TestAutopilotEventFromMessageAndMentionMatch(t *testing.T) {
	files := []slack.File{
		{ID: "F1", Name: "bug.png", Mimetype: "image/png", Size: 123, URLPrivateDownload: "https://files.slack.com/files-pri/T1-F1/download/bug.png"},
		{ID: "F2", URLPrivateDownload: "https://example.com/external.png"},
		{ID: "F3"},
	}
	message := &slackevents.MessageEvent{
		Channel: "C123", User: "UALICE", TimeStamp: "1700000000.000100", Text: "<@UBOT> checkout fails", SubType: "file_share",
		Message: &slack.Msg{Files: files},
	}
	mention := &slackevents.AppMentionEvent{
		Channel: message.Channel, User: message.User, TimeStamp: message.TimeStamp, Text: message.Text, Files: files,
	}
	gotMessage, ok := autopilotEventFrom(eventsAPI(message), "UBOT")
	if !ok {
		t.Fatal("top-level human file_share was rejected")
	}
	gotMention, ok := autopilotEventFrom(eventsAPI(mention), "UBOT")
	if !ok || !reflect.DeepEqual(gotMessage, gotMention) {
		t.Fatalf("paired events differ: message=%+v, mention=%+v, accepted=%v", gotMessage, gotMention, ok)
	}
	if gotMessage.AppID != "A1" || gotMessage.TeamID != "T1" || gotMessage.Text != message.Text || len(gotMessage.Files) != 1 || gotMessage.Files[0].ID != "F1" || gotMessage.Files[0].Size != 123 {
		t.Fatalf("incorrect normalized event: %+v", gotMessage)
	}
}

func TestAutopilotEventFromReplyMessageAndMentionMatch(t *testing.T) {
	message := &slackevents.MessageEvent{Channel: "C123", User: "UALICE", TimeStamp: "1700000000.000101", ThreadTimeStamp: "1700000000.000100", Text: "<@UBOT> another detail"}
	mention := &slackevents.AppMentionEvent{Channel: message.Channel, User: message.User, TimeStamp: message.TimeStamp, ThreadTimeStamp: message.ThreadTimeStamp, Text: message.Text}
	gotMessage, messageOK := autopilotEventFrom(eventsAPI(message), "UBOT")
	gotMention, mentionOK := autopilotEventFrom(eventsAPI(mention), "UBOT")
	if !messageOK || !mentionOK || !reflect.DeepEqual(gotMessage, gotMention) || gotMessage.ThreadTS != message.ThreadTimeStamp || gotMessage.MessageTS != message.TimeStamp {
		t.Fatalf("reply pairs lost stable message/thread IDs: message=%+v (%v) mention=%+v (%v)", gotMessage, messageOK, gotMention, mentionOK)
	}
}

func TestAutopilotEventFromEligibleReports(t *testing.T) {
	base := slackevents.MessageEvent{Channel: "C123", User: "UALICE", TimeStamp: "1700000000.000100", Text: "checkout fails"}
	for _, tc := range []struct {
		name string
		edit func(*slackevents.MessageEvent)
		want bool
	}{
		{"top level", func(m *slackevents.MessageEvent) {}, true},
		{"root thread timestamp", func(m *slackevents.MessageEvent) { m.ThreadTimeStamp = m.TimeStamp }, true},
		{"private channel", func(m *slackevents.MessageEvent) { m.Channel = "G123" }, true},
		{"file only", func(m *slackevents.MessageEvent) {
			m.Text = ""
			m.Message = &slack.Msg{Files: []slack.File{{ID: "F1", URLPrivate: "https://files.slack.com/file.png"}}}
		}, true},
		{"reply", func(m *slackevents.MessageEvent) { m.ThreadTimeStamp = "1699999999.000100" }, true},
		{"file-only reply", func(m *slackevents.MessageEvent) {
			m.ThreadTimeStamp = "1699999999.000100"
			m.SubType = "file_share"
			m.Text = ""
			m.Message = &slack.Msg{Files: []slack.File{{ID: "F1", URLPrivate: "https://files.slack.com/file.png"}}}
		}, true},
		{"reply broadcast", func(m *slackevents.MessageEvent) {
			m.ThreadTimeStamp = "1699999999.000100"
			m.SubType = "thread_broadcast"
		}, true},
		{"own bot", func(m *slackevents.MessageEvent) { m.User = "UBOT" }, false},
		{"another bot", func(m *slackevents.MessageEvent) { m.BotID = "BOTHER" }, false},
		{"bot subtype", func(m *slackevents.MessageEvent) { m.SubType = "bot_message" }, false},
		{"edited", func(m *slackevents.MessageEvent) { m.SubType = "message_changed" }, false},
		{"deleted", func(m *slackevents.MessageEvent) { m.SubType = "message_deleted" }, false},
		{"broadcast", func(m *slackevents.MessageEvent) { m.SubType = "thread_broadcast" }, false},
		{"missing user", func(m *slackevents.MessageEvent) { m.User = "" }, false},
		{"missing timestamp", func(m *slackevents.MessageEvent) { m.TimeStamp = "" }, false},
		{"invalid timestamp", func(m *slackevents.MessageEvent) { m.TimeStamp = "not-a-timestamp" }, false},
		{"invalid reply timestamp", func(m *slackevents.MessageEvent) { m.ThreadTimeStamp = "not-a-timestamp" }, false},
		{"missing channel", func(m *slackevents.MessageEvent) { m.Channel = "" }, false},
		{"DM", func(m *slackevents.MessageEvent) { m.Channel = "D123" }, false},
		{"empty", func(m *slackevents.MessageEvent) { m.Text = " \n " }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			message := base
			tc.edit(&message)
			_, got := autopilotEventFrom(eventsAPI(&message), "UBOT")
			if got != tc.want {
				t.Errorf("accepted = %v, want %v", got, tc.want)
			}
		})
	}
	for _, event := range []slackevents.EventsAPIEvent{
		eventsAPI((*slackevents.MessageEvent)(nil)),
		eventsAPI((*slackevents.AppMentionEvent)(nil)),
		eventsAPI(nil),
	} {
		if _, ok := autopilotEventFrom(event, "UBOT"); ok {
			t.Error("accepted unsupported/nil event")
		}
	}
}
