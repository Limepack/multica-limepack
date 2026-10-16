package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/integrations/channel/engine"
	"github.com/multica-ai/multica/server/internal/integrations/slack"
	"github.com/multica-ai/multica/server/internal/service"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

const slackAutopilotEvent = "slack.bug_report"
const slackAutopilotReplyEvent = "slack.bug_report_reply"

// This provenance is written only by Socket Mode ingress. selectedHeadersJSON
// never accepts it from HTTP callers, so a public webhook cannot ask the Slack
// credential to download files from a forged event. Replays retain the marker.
const slackAutopilotProvenanceKey = "multica_slack_autopilot"

type slackAutopilotProvenance struct {
	InstallationID string `json:"installation_id"`
	AppID          string `json:"app_id"`
	TeamID         string `json:"team_id"`
	ChannelID      string `json:"channel_id"`
	MessageTS      string `json:"message_ts"`
}

type slackAutopilotEnvelope struct {
	Event        string               `json:"event"`
	EventPayload slack.AutopilotEvent `json:"eventPayload"`
}

// SlackAutopilotIntake routes only explicitly configured installation/channel
// tuples. It uses the existing durable webhook queue and trigger principal;
// reporters do not need to bind their Slack accounts to Multica.
type SlackAutopilotIntake struct {
	h         *Handler
	routes    []slack.AutopilotRoute
	media     engine.MediaResolver
	permalink func(context.Context, db.ChannelInstallation, slack.AutopilotEvent) (string, error)
}

func NewSlackAutopilotIntake(h *Handler, routes []slack.AutopilotRoute, media engine.MediaResolver, decrypt slack.Decrypter) *SlackAutopilotIntake {
	intake := &SlackAutopilotIntake{h: h, routes: append([]slack.AutopilotRoute(nil), routes...), media: media,
		permalink: func(ctx context.Context, installation db.ChannelInstallation, event slack.AutopilotEvent) (string, error) {
			return slack.AutopilotPermalink(ctx, installation, decrypt, event)
		},
	}
	if len(routes) > 0 {
		h.SlackAutopilot = intake
	}
	return intake
}

func (s *SlackAutopilotIntake) routeFor(event slack.AutopilotEvent, installation db.ChannelInstallation) (slack.AutopilotRoute, bool) {
	for _, route := range s.routes {
		if route.TeamID == event.TeamID && route.ChannelID == event.ChannelID &&
			route.WorkspaceID == uuidToString(installation.WorkspaceID) && route.AgentID == uuidToString(installation.AgentID) {
			return route, true
		}
	}
	return slack.AutopilotRoute{}, false
}

func (s *SlackAutopilotIntake) hasChannel(event slack.AutopilotEvent) bool {
	for _, route := range s.routes {
		if route.TeamID == event.TeamID && route.ChannelID == event.ChannelID {
			return true
		}
	}
	return false
}

func (s *SlackAutopilotIntake) loadRoute(ctx context.Context, route slack.AutopilotRoute, installation db.ChannelInstallation) (db.Autopilot, db.AutopilotTrigger, error) {
	apID, err := util.ParseUUID(route.AutopilotID)
	if err != nil {
		return db.Autopilot{}, db.AutopilotTrigger{}, err
	}
	triggerID, err := util.ParseUUID(route.TriggerID)
	if err != nil {
		return db.Autopilot{}, db.AutopilotTrigger{}, err
	}
	ap, err := s.h.Queries.GetAutopilotInWorkspace(ctx, db.GetAutopilotInWorkspaceParams{ID: apID, WorkspaceID: installation.WorkspaceID})
	if err != nil {
		return db.Autopilot{}, db.AutopilotTrigger{}, fmt.Errorf("load Slack autopilot: %w", err)
	}
	trigger, err := s.h.Queries.GetAutopilotTrigger(ctx, triggerID)
	if err != nil {
		return db.Autopilot{}, db.AutopilotTrigger{}, fmt.Errorf("load Slack autopilot trigger: %w", err)
	}
	if trigger.AutopilotID != ap.ID || trigger.Kind != "webhook" ||
		ap.ExecutionMode != "create_issue" || ap.AssigneeType != "agent" || ap.AssigneeID != installation.AgentID {
		return db.Autopilot{}, db.AutopilotTrigger{}, errors.New("Slack autopilot route ownership or execution mode mismatch")
	}
	return ap, trigger, nil
}

func (s *SlackAutopilotIntake) HandleAutopilotEvent(ctx context.Context, event slack.AutopilotEvent) (bool, error) {
	if !s.hasChannel(event) {
		return false, nil
	}
	if event.AppID == "" || event.UserID == "" || event.MessageTS == "" {
		return true, errors.New("Slack autopilot event is missing its authenticated identity")
	}
	// This bounds durable intake, not screenshots or agent work. The adapter
	// withholds ACK on failure so Slack can redeliver the same message.
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	installation, err := s.h.Queries.GetChannelInstallationByAppID(ctx, db.GetChannelInstallationByAppIDParams{
		ChannelType: string(slack.TypeSlack), AppID: event.AppID,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return true, fmt.Errorf("resolve Slack autopilot installation: %w", err)
	}
	route, matched := s.routeFor(event, installation)
	if !matched {
		return false, nil
	}
	public := slack.DecodePublicConfig(installation.Config)
	if public.AppID != event.AppID || public.TeamID != event.TeamID || public.BotUserID == event.UserID {
		return true, errors.New("Slack autopilot event does not match installation identity")
	}
	if installation.Status != "active" {
		return true, nil
	}
	ap, trigger, err := s.loadRoute(ctx, route, installation)
	if err != nil {
		return true, err
	}
	eventName := slackAutopilotEvent
	if isSlackAutopilotReply(event) {
		parent, parentErr := s.parentDelivery(ctx, ap, trigger, event)
		if errors.Is(parentErr, pgx.ErrNoRows) && !recentSlackAutopilotThread(event.ThreadTS, time.Now()) {
			return false, nil
		}
		if parentErr != nil && !errors.Is(parentErr, pgx.ErrNoRows) {
			return true, parentErr
		}
		if parentErr == nil {
			if _, err := nativeSlackAutopilotEvent(parent); err != nil {
				return false, nil
			}
		}
		eventName = slackAutopilotReplyEvent
	}
	body, err := json.Marshal(slackAutopilotEnvelope{Event: eventName, EventPayload: event})
	if err != nil {
		return true, fmt.Errorf("encode Slack autopilot event: %w", err)
	}
	if len(body) > maxWebhookBodyBytes {
		return true, errors.New("Slack autopilot event exceeds the webhook body limit")
	}
	provenance := slackAutopilotProvenance{InstallationID: uuidToString(installation.ID), AppID: event.AppID,
		TeamID: event.TeamID, ChannelID: event.ChannelID, MessageTS: event.MessageTS}
	headers, err := json.Marshal(map[string]any{slackAutopilotProvenanceKey: provenance})
	if err != nil {
		return true, err
	}
	_, _, err = s.h.persistInboundDelivery(ctx, persistDeliveryInput{
		WorkspaceID: ap.WorkspaceID, AutopilotID: ap.ID, TriggerID: trigger.ID,
		Provider: "generic", Event: eventName,
		DedupeKey:    "slack:" + event.TeamID + ":" + event.ChannelID + ":" + event.MessageTS,
		DedupeSource: "slack_message", SignatureStatus: sigStatusNotRequired,
		ContentType: "application/json", RawBody: body, SelectedHeaders: headers,
	})
	if err != nil {
		return true, fmt.Errorf("persist Slack autopilot delivery: %w", err)
	}
	if s.h.WebhookDeliveryWorker != nil {
		s.h.WebhookDeliveryWorker.Notify()
	}
	return true, nil
}

// PrepareIssueMedia runs after trigger admission, off the Slack ACK path. The
// delivery UUID owns upload intents until attachment binding commits with the
// issue, so retries use the same deterministic object keys without a fake Chat.
func (s *SlackAutopilotIntake) PrepareIssueMedia(ctx context.Context, ap db.Autopilot, run db.AutopilotRun) (service.AutopilotIssueMedia, error) {
	var out service.AutopilotIssueMedia
	if !run.WebhookDeliveryID.Valid {
		return out, nil
	}
	delivery, err := s.h.Queries.GetWebhookDeliveryInWorkspace(ctx, db.GetWebhookDeliveryInWorkspaceParams{
		ID: run.WebhookDeliveryID, WorkspaceID: ap.WorkspaceID,
	})
	if err != nil {
		return out, fmt.Errorf("read media delivery: %w", err)
	}
	var selected map[string]json.RawMessage
	if len(delivery.SelectedHeaders) == 0 {
		return out, nil
	}
	if err := json.Unmarshal(delivery.SelectedHeaders, &selected); err != nil {
		return out, nil
	}
	marker, native := selected[slackAutopilotProvenanceKey]
	if !native {
		return out, nil
	}
	var provenance slackAutopilotProvenance
	var envelope slackAutopilotEnvelope
	if err := json.Unmarshal(marker, &provenance); err != nil {
		return out, fmt.Errorf("decode native Slack provenance: %w", err)
	}
	if err := json.Unmarshal(delivery.RawBody, &envelope); err != nil {
		return out, fmt.Errorf("decode native Slack report: %w", err)
	}
	event := envelope.EventPayload
	if (envelope.Event != slackAutopilotEvent && envelope.Event != slackAutopilotReplyEvent) || delivery.AutopilotID != ap.ID || delivery.TriggerID != run.TriggerID ||
		provenance.AppID != event.AppID || provenance.TeamID != event.TeamID ||
		provenance.ChannelID != event.ChannelID || provenance.MessageTS != event.MessageTS {
		return out, errors.New("native Slack delivery provenance mismatch")
	}
	installationID, err := util.ParseUUID(provenance.InstallationID)
	if err != nil {
		return out, err
	}
	installation, err := s.h.Queries.GetChannelInstallationInWorkspace(ctx, db.GetChannelInstallationInWorkspaceParams{
		ID: installationID, WorkspaceID: ap.WorkspaceID, ChannelType: string(slack.TypeSlack),
	})
	if err != nil {
		return out, fmt.Errorf("resolve media installation: %w", err)
	}
	route, matched := s.routeFor(event, installation)
	public := slack.DecodePublicConfig(installation.Config)
	if !matched || installation.Status != "active" || public.AppID != event.AppID || public.TeamID != event.TeamID ||
		route.AutopilotID != uuidToString(ap.ID) || route.TriggerID != uuidToString(run.TriggerID) {
		return out, errors.New("native Slack route is no longer authorized")
	}
	_, trigger, err := s.loadRoute(ctx, route, installation)
	if err != nil {
		return out, err
	}
	if !trigger.CreatedByType.Valid || trigger.CreatedByType.String != "member" || !trigger.CreatedByID.Valid {
		return out, errors.New("Slack media has no authorized trigger principal")
	}
	if _, err := s.h.Queries.GetMemberByUserAndWorkspace(ctx, db.GetMemberByUserAndWorkspaceParams{
		UserID: trigger.CreatedByID, WorkspaceID: ap.WorkspaceID,
	}); err != nil {
		return out, fmt.Errorf("revalidate media principal: %w", err)
	}
	out.UploaderID = trigger.CreatedByID
	finalAttempt := delivery.DispatchAttempts+1 >= webhookWorkerMaxAttempts
	mediaCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	warnings := append([]string(nil), event.AttachmentWarnings...)
	permalink, err := s.permalink(mediaCtx, installation, event)
	if err != nil {
		if !finalAttempt {
			return out, err
		}
		warnings = append(warnings, "The original Slack message link could not be retrieved after five attempts; use the team, channel, and timestamp in the report payload.")
	}
	var eligible []slack.AutopilotFile
	oversized := 0
	for _, file := range event.Files {
		if file.Size > 20<<20 {
			oversized++
			continue
		}
		eligible = append(eligible, file)
	}
	if oversized > 0 {
		warnings = append(warnings, fmt.Sprintf("%d attachments exceeded the 20 MiB limit and were not imported.", oversized))
	}
	if len(eligible) > 10 {
		warnings = append(warnings, fmt.Sprintf("%d attachments exceeded the 10 file limit and were not imported.", len(eligible)-10))
		eligible = eligible[:10]
	}
	if s.media == nil && len(eligible) > 0 {
		warnings = append(warnings, "Attachments could not be imported because Multica object storage is unavailable.")
		eligible = nil
	}
	if permalink != "" {
		out.DescriptionAppendix = "Source: [Original Slack report](" + permalink + ")"
	}
	if len(warnings) > 0 {
		out.DescriptionAppendix += "\n\nAttachment import:\n- " + strings.Join(warnings, "\n- ")
	}
	if len(eligible) == 0 {
		return out, nil
	}
	event.Files = eligible
	msg := slack.AutopilotInboundMessage(event)
	if !s.media.HasMedia(msg) {
		return out, errors.New("Slack report attachments are not downloadable")
	}
	resolved := s.media.ResolveMedia(mediaCtx, engine.ResolvedInstallation{
		ID: installation.ID, WorkspaceID: installation.WorkspaceID, AgentID: installation.AgentID,
		InstallerUserID: installation.InstallerUserID, Active: true, Platform: installation,
	}, engine.ResolvedIdentity{UserID: out.UploaderID}, pgtype.UUID{}, delivery.ID, msg)
	if err := mediaCtx.Err(); err != nil {
		if finalAttempt {
			out.DescriptionAppendix += "\n\nAttachment import timed out after five attempts; the bug report text is preserved and screenshots are unavailable."
			return out, nil
		}
		return out, fmt.Errorf("resolve Slack report attachments: %w", err)
	}
	if len(resolved.MediaRefs) != len(eligible) {
		if finalAttempt {
			out.Refs = resolved.MediaRefs
			out.DescriptionAppendix += fmt.Sprintf("\n\nAttachment import: %d of %d eligible files were available after five attempts; the remaining files could not be imported.", len(resolved.MediaRefs), len(eligible))
			return out, nil
		}
		return out, fmt.Errorf("resolved %d of %d Slack report attachments", len(resolved.MediaRefs), len(eligible))
	}
	out.Refs = resolved.MediaRefs
	return out, nil
}
