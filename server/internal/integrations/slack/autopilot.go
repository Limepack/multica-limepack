package slack

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"regexp"
	"strings"

	slackapi "github.com/slack-go/slack"
	"github.com/slack-go/slack/slackevents"

	"github.com/multica-ai/multica/server/internal/integrations/channel"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// AutopilotRoute binds one Slack channel to an existing webhook trigger. The
// workspace and agent IDs identify the installation allowed to submit reports.
// Instructions remain on the Autopilot's editable Runbook.
type AutopilotRoute struct {
	WorkspaceID string `json:"workspace_id"`
	AgentID     string `json:"agent_id"`
	TeamID      string `json:"team_id"`
	ChannelID   string `json:"channel_id"`
	AutopilotID string `json:"autopilot_id"`
	TriggerID   string `json:"trigger_id"`
}

var (
	autopilotTeamID    = regexp.MustCompile(`^T[A-Z0-9]+$`)
	autopilotChannelID = regexp.MustCompile(`^[CG][A-Z0-9]+$`)
	autopilotTimestamp = regexp.MustCompile(`^[0-9]+\.[0-9]{6}$`)
)

// ParseAutopilotRoutes validates MULTICA_SLACK_AUTOPILOT_ROUTES before startup.
// An empty value disables automatic ingestion. Each installation/team/channel
// tuple can have only one target; separate agents can have separate routes.
func ParseAutopilotRoutes(raw string) ([]AutopilotRoute, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	if !strings.HasPrefix(raw, "[") {
		return nil, errors.New("slack: autopilot routes must be a JSON array")
	}
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	var routes []AutopilotRoute
	if err := decoder.Decode(&routes); err != nil {
		return nil, fmt.Errorf("slack: decode autopilot routes: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, errors.New("slack: autopilot routes must contain one JSON array")
	}
	seen := make(map[string]bool, len(routes))
	for i := range routes {
		route := &routes[i]
		for _, field := range []struct {
			name  string
			value *string
		}{
			{"workspace_id", &route.WorkspaceID},
			{"agent_id", &route.AgentID},
			{"autopilot_id", &route.AutopilotID},
			{"trigger_id", &route.TriggerID},
		} {
			parsed, err := util.ParseUUID(*field.value)
			if err != nil || len(*field.value) != 36 || parsed.Bytes == [16]byte{} || util.UUIDToString(parsed) != strings.ToLower(*field.value) {
				return nil, fmt.Errorf("slack: autopilot route %d has invalid %s", i, field.name)
			}
			*field.value = util.UUIDToString(parsed)
		}
		if !autopilotTeamID.MatchString(route.TeamID) || !autopilotChannelID.MatchString(route.ChannelID) {
			return nil, fmt.Errorf("slack: autopilot route %d has invalid team_id or channel_id", i)
		}
		key := route.WorkspaceID + ":" + route.AgentID + ":" + route.TeamID + ":" + route.ChannelID
		if seen[key] {
			return nil, fmt.Errorf("slack: duplicate autopilot route for %s", key)
		}
		seen[key] = true
	}
	return routes, nil
}

// AutopilotFile carries only Slack-hosted private files that the authenticated
// media resolver can fetch. DownloadURL never includes an access token.
type AutopilotFile struct {
	ID          string `json:"id"`
	Name        string `json:"name,omitempty"`
	Mimetype    string `json:"mimetype,omitempty"`
	Size        int64  `json:"size,omitempty"`
	DownloadURL string `json:"download_url"`
}

// AutopilotEvent is a new human report or thread reply. MessageTS is identical
// for Slack's message and app_mention pair and anchors durable deduplication.
// A different ThreadTS identifies a reply to an existing report.
type AutopilotEvent struct {
	AppID              string          `json:"app_id"`
	TeamID             string          `json:"team_id"`
	ChannelID          string          `json:"channel_id"`
	UserID             string          `json:"user_id"`
	MessageTS          string          `json:"message_ts"`
	ThreadTS           string          `json:"thread_ts,omitempty"`
	Text               string          `json:"text"`
	Files              []AutopilotFile `json:"files,omitempty"`
	AttachmentWarnings []string        `json:"attachment_warnings,omitempty"`
}

// AutopilotPermalink reads the canonical source link with the same installed
// bot credential. It runs off the ACK path and never returns that credential.
func AutopilotPermalink(ctx context.Context, installation db.ChannelInstallation, decrypt Decrypter, event AutopilotEvent) (string, error) {
	creds, err := decodeCredentials(installation.Config, decrypt)
	if err != nil {
		return "", fmt.Errorf("resolve Slack permalink credential: %w", err)
	}
	link, err := slackapi.New(creds.BotToken).GetPermalinkContext(ctx, &slackapi.PermalinkParameters{Channel: event.ChannelID, Ts: event.MessageTS})
	if err != nil {
		return "", fmt.Errorf("read Slack report permalink: %w", err)
	}
	parsed, err := url.Parse(link)
	if err != nil || parsed.Scheme != "https" || parsed.User != nil || !strings.HasSuffix(parsed.Hostname(), ".slack.com") ||
		!strings.HasPrefix(parsed.Path, "/archives/"+event.ChannelID+"/p") {
		return "", errors.New("Slack returned an invalid report permalink")
	}
	return link, nil
}

// AutopilotEventHandler resolves the route and its active installation, then
// durably records the delivery before returning handled=true. An error leaves
// the Socket Mode envelope unacknowledged so Slack can retry it.
type AutopilotEventHandler interface {
	HandleAutopilotEvent(context.Context, AutopilotEvent) (handled bool, err error)
}

// AutopilotInboundMessage reconstructs the media resolver's inbound envelope
// after the caller has verified native Slack provenance and the installation.
// It does not dispatch a chat or alter the stored report's original text.
func AutopilotInboundMessage(report AutopilotEvent) channel.InboundMessage {
	var files []slackRawFile
	for _, file := range report.Files {
		if isFetchableSlackFileURL(file.DownloadURL) {
			files = append(files, slackRawFile(file))
		}
	}
	threadTS := report.ThreadTS
	if threadTS == "" {
		threadTS = report.MessageTS
	}
	return buildInbound(slackevents.EventsAPIEvent{TeamID: report.TeamID, APIAppID: report.AppID}, buildInboundParams{
		eventType: "message",
		channelID: report.ChannelID,
		userID:    report.UserID,
		text:      report.Text,
		ts:        report.MessageTS,
		threadTS:  threadTS,
		chatType:  channel.ChatTypeGroup,
		addressed: true,
		files:     files,
	}, nil)
}

func autopilotEventFrom(e slackevents.EventsAPIEvent, botUserID string) (AutopilotEvent, bool) {
	var report AutopilotEvent
	var botID, threadTS, subtype string
	var files []slackRawFile
	switch m := e.InnerEvent.Data.(type) {
	case *slackevents.MessageEvent:
		if m == nil {
			return AutopilotEvent{}, false
		}
		report = AutopilotEvent{ChannelID: m.Channel, UserID: m.User, MessageTS: m.TimeStamp, Text: m.Text}
		botID, threadTS, subtype = m.BotID, m.ThreadTimeStamp, m.SubType
		if m.Message != nil {
			files = rawFilesFrom(m.Message.Files)
			if len(files) < len(m.Message.Files) {
				report.AttachmentWarnings = []string{fmt.Sprintf("%d attachments are external or still processing and could not be imported from Slack.", len(m.Message.Files)-len(files))}
			}
		}
	case *slackevents.AppMentionEvent:
		if m == nil {
			return AutopilotEvent{}, false
		}
		report = AutopilotEvent{ChannelID: m.Channel, UserID: m.User, MessageTS: m.TimeStamp, Text: m.Text}
		botID, threadTS = m.BotID, m.ThreadTimeStamp
		files = rawFilesFrom(m.Files)
		if len(files) < len(m.Files) {
			report.AttachmentWarnings = []string{fmt.Sprintf("%d attachments are external or still processing and could not be imported from Slack.", len(m.Files)-len(files))}
		}
	default:
		return AutopilotEvent{}, false
	}
	if !autopilotTeamID.MatchString(e.TeamID) || !autopilotChannelID.MatchString(report.ChannelID) || report.UserID == "" || !autopilotTimestamp.MatchString(report.MessageTS) || (threadTS != "" && !autopilotTimestamp.MatchString(threadTS)) || botID != "" || (botUserID != "" && report.UserID == botUserID) || (subtype != "" && subtype != "file_share" && !(subtype == "thread_broadcast" && threadTS != "" && threadTS != report.MessageTS)) || (strings.TrimSpace(report.Text) == "" && len(files) == 0 && len(report.AttachmentWarnings) == 0) {
		return AutopilotEvent{}, false
	}
	report.AppID, report.TeamID = e.APIAppID, e.TeamID
	report.ThreadTS = threadTS
	for _, file := range files {
		report.Files = append(report.Files, AutopilotFile(file))
	}
	return report, true
}
