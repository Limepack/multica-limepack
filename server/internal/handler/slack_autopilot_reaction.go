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
	"github.com/multica-ai/multica/server/internal/integrations/slack"
	"github.com/multica-ai/multica/server/internal/issuestatus"
	"github.com/multica-ai/multica/server/internal/service"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/protocol"
	slackapi "github.com/slack-go/slack"
)

const slackAutopilotReactionEvent = "slack.bug_report_completed"

var errSlackCompletionUnauthorized = errors.New("Slack completion route is no longer authorized")

type slackAutopilotReactionEnvelope struct {
	Event        string                       `json:"event"`
	EventPayload slack.AutopilotReactionEvent `json:"eventPayload"`
}

type slackCompletionRoute struct {
	installation db.ChannelInstallation
	route        slack.AutopilotRoute
	autopilot    db.Autopilot
	trigger      db.AutopilotTrigger
	principal    pgtype.UUID
}

func reactionReport(event slack.AutopilotReactionEvent) slack.AutopilotEvent {
	return slack.AutopilotEvent{AppID: event.AppID, TeamID: event.TeamID, ChannelID: event.ChannelID, UserID: event.UserID, MessageTS: event.MessageTS}
}

func (s *SlackAutopilotIntake) completionRoute(ctx context.Context, event slack.AutopilotReactionEvent, installationID pgtype.UUID) (slackCompletionRoute, error) {
	var out slackCompletionRoute
	installation, err := s.h.Queries.GetChannelInstallationByAppID(ctx, db.GetChannelInstallationByAppIDParams{ChannelType: string(slack.TypeSlack), AppID: event.AppID})
	if errors.Is(err, pgx.ErrNoRows) {
		return out, errSlackCompletionUnauthorized
	}
	if err != nil {
		return out, err
	}
	public := slack.DecodePublicConfig(installation.Config)
	route, matched := s.routeFor(reactionReport(event), installation)
	if !matched || route.CompletionStatusKey == "" || installation.Status != "active" ||
		(installationID.Valid && installation.ID != installationID) || public.AppID != event.AppID ||
		public.TeamID != event.TeamID || public.BotUserID == event.UserID {
		return out, errSlackCompletionUnauthorized
	}
	ap, trigger, err := s.loadRoute(ctx, route, installation)
	if err != nil {
		return out, err
	}
	if ap.Status != "active" || !trigger.Enabled {
		return out, errSlackCompletionUnauthorized
	}
	principal := service.ResolveAutopilotTriggerPrincipal(ctx, s.h.Queries, trigger.ID, ap.ID, ap.WorkspaceID)
	agent, err := s.h.Queries.GetAgentInWorkspace(ctx, db.GetAgentInWorkspaceParams{ID: installation.AgentID, WorkspaceID: ap.WorkspaceID})
	if err != nil {
		return out, err
	}
	if !principal.Valid || agent.ArchivedAt.Valid || !s.h.canInvokeAgent(ctx, agent, "member", uuidToString(principal), uuidToString(principal), uuidToString(ap.WorkspaceID)) {
		return out, errSlackCompletionUnauthorized
	}
	return slackCompletionRoute{installation: installation, route: route, autopilot: ap, trigger: trigger, principal: principal}, nil
}

func (s *SlackAutopilotIntake) HandleAutopilotReaction(ctx context.Context, event slack.AutopilotReactionEvent) (bool, error) {
	enabled := false
	for _, route := range s.routes {
		if route.TeamID == event.TeamID && route.ChannelID == event.ChannelID && route.CompletionStatusKey != "" {
			enabled = true
			break
		}
	}
	if !enabled || !slack.IsCompletionReaction(event.Reaction) {
		return false, nil
	}
	if event.AppID == "" || event.UserID == "" || event.MessageTS == "" || event.EventTS == "" {
		return true, errors.New("Slack completion event is missing its authenticated identity")
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	route, err := s.completionRoute(ctx, event, pgtype.UUID{})
	if errors.Is(err, errSlackCompletionUnauthorized) {
		return false, nil
	}
	if err != nil {
		return true, err
	}
	body, err := json.Marshal(slackAutopilotReactionEnvelope{Event: slackAutopilotReactionEvent, EventPayload: event})
	if err != nil {
		return true, err
	}
	headers, err := json.Marshal(map[string]any{slackAutopilotProvenanceKey: slackAutopilotProvenance{
		InstallationID: uuidToString(route.installation.ID), AppID: event.AppID, TeamID: event.TeamID, ChannelID: event.ChannelID, MessageTS: event.MessageTS,
	}})
	if err != nil {
		return true, err
	}
	_, _, err = s.h.persistInboundDelivery(ctx, persistDeliveryInput{
		WorkspaceID: route.autopilot.WorkspaceID, AutopilotID: route.autopilot.ID, TriggerID: route.trigger.ID,
		Provider: "generic", Event: slackAutopilotReactionEvent, DedupeSource: "slack_reaction",
		DedupeKey:       strings.Join([]string{"slack-reaction", event.TeamID, event.ChannelID, event.MessageTS, event.UserID, event.Reaction, event.EventTS}, ":"),
		SignatureStatus: sigStatusNotRequired, ContentType: "application/json", RawBody: body, SelectedHeaders: headers,
	})
	if err == nil && s.h.WebhookDeliveryWorker != nil {
		s.h.WebhookDeliveryWorker.Notify()
	}
	return true, err
}

func nativeSlackAutopilotReaction(delivery db.WebhookDelivery) (slack.AutopilotReactionEvent, pgtype.UUID, error) {
	var selected map[string]json.RawMessage
	var provenance slackAutopilotProvenance
	var envelope slackAutopilotReactionEnvelope
	if json.Unmarshal(delivery.SelectedHeaders, &selected) != nil || json.Unmarshal(selected[slackAutopilotProvenanceKey], &provenance) != nil ||
		json.Unmarshal(delivery.RawBody, &envelope) != nil {
		return slack.AutopilotReactionEvent{}, pgtype.UUID{}, errors.New("native Slack completion provenance required")
	}
	event := envelope.EventPayload
	id, err := util.ParseUUID(provenance.InstallationID)
	if err != nil || !id.Valid || envelope.Event != slackAutopilotReactionEvent || delivery.Event != envelope.Event ||
		!slack.IsCompletionReaction(event.Reaction) || event.UserID == "" || event.EventTS == "" ||
		provenance.AppID != event.AppID || provenance.TeamID != event.TeamID || provenance.ChannelID != event.ChannelID || provenance.MessageTS != event.MessageTS {
		return slack.AutopilotReactionEvent{}, pgtype.UUID{}, errors.New("native Slack completion provenance mismatch")
	}
	return event, id, nil
}

func (s *SlackAutopilotIntake) processReaction(ctx context.Context, delivery db.WebhookDelivery) error {
	w := s.h.WebhookDeliveryWorker
	event, installationID, err := nativeSlackAutopilotReaction(delivery)
	if err != nil {
		return w.complete(ctx, delivery, deliveryStatusFailed, pgtype.UUID{}, err.Error())
	}
	route, err := s.completionRoute(ctx, event, installationID)
	if errors.Is(err, errSlackCompletionUnauthorized) {
		return w.complete(ctx, delivery, deliveryStatusIgnored, pgtype.UUID{}, err.Error())
	}
	if err != nil {
		return w.retryOrFail(ctx, delivery, err)
	}
	if route.autopilot.ID != delivery.AutopilotID || route.autopilot.WorkspaceID != delivery.WorkspaceID || route.trigger.ID != delivery.TriggerID {
		return w.complete(ctx, delivery, deliveryStatusFailed, pgtype.UUID{}, "Slack completion route ownership mismatch")
	}
	lookupCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	target, err := s.reactionTarget(lookupCtx, route.installation, event)
	cancel()
	if err != nil {
		if err.Error() == "missing_scope" {
			return w.complete(ctx, delivery, deliveryStatusFailed, pgtype.UUID{}, "Slack completion requires reactions:read and users:read scopes")
		}
		var rateLimit *slackapi.RateLimitedError
		if errors.As(err, &rateLimit) {
			_, err := s.h.Queries.DeferClaimedWebhookDelivery(ctx, db.DeferClaimedWebhookDeliveryParams{
				ID: delivery.ID, LeaseToken: delivery.LeaseToken,
				AvailableAt: pgtype.Timestamptz{Time: time.Now().Add(max(rateLimit.RetryAfter, 5*time.Second)), Valid: true},
			})
			_, err = handleWebhookLeaseMutation("defer rate-limited Slack completion", delivery, err)
			return err
		}
		return w.retryOrFail(ctx, delivery, err)
	}
	if !target.Human || !target.Exists || target.ThreadTS == "" {
		return w.complete(ctx, delivery, deliveryStatusIgnored, pgtype.UUID{}, "Slack completion requires a human reaction on a tracked message")
	}
	report := reactionReport(event)
	report.ThreadTS = target.ThreadTS
	parent, err := s.parentDelivery(ctx, route.autopilot, route.trigger, report)
	if errors.Is(err, pgx.ErrNoRows) {
		if recentSlackAutopilotThread(target.ThreadTS, time.Now()) {
			return s.deferReply(ctx, delivery)
		}
		return w.complete(ctx, delivery, deliveryStatusIgnored, pgtype.UUID{}, "Slack completion thread is not tracked")
	}
	if err != nil {
		return w.retryOrFail(ctx, delivery, err)
	}
	root, err := nativeSlackAutopilotEvent(parent)
	if err != nil || isSlackAutopilotReply(root) || root.AppID != event.AppID || root.TeamID != event.TeamID || root.ChannelID != event.ChannelID || root.MessageTS != target.ThreadTS {
		return w.complete(ctx, delivery, deliveryStatusFailed, pgtype.UUID{}, "Slack completion parent provenance mismatch")
	}
	if parent.Status == deliveryStatusQueued {
		return s.deferReply(ctx, delivery)
	}
	if parent.Status != deliveryStatusDispatched {
		return w.complete(ctx, delivery, deliveryStatusIgnored, pgtype.UUID{}, "Slack completion parent did not create an issue")
	}
	run, err := s.h.Queries.GetAutopilotRunByWebhookDelivery(ctx, parent.ID)
	if err != nil {
		return w.retryOrFail(ctx, delivery, err)
	}
	if !run.IssueID.Valid {
		return w.complete(ctx, delivery, deliveryStatusIgnored, run.ID, "Slack completion parent has no issue")
	}
	if err := s.commitReaction(ctx, delivery, run, event, installationID); errors.Is(err, service.ErrWebhookDeliveryLeaseLost) {
		return nil
	} else if err != nil {
		return w.retryOrFail(ctx, delivery, err)
	}
	return nil
}

func (s *SlackAutopilotIntake) commitReaction(ctx context.Context, delivery db.WebhookDelivery, run db.AutopilotRun, event slack.AutopilotReactionEvent, installationID pgtype.UUID) error {
	tx, err := s.h.TxStarter.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	qtx := s.h.Queries.WithTx(tx)
	if _, err := qtx.LockWebhookDeliveryForDispatch(ctx, db.LockWebhookDeliveryForDispatchParams{ID: delivery.ID, WorkspaceID: delivery.WorkspaceID, LeaseToken: delivery.LeaseToken}); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return service.ErrWebhookDeliveryLeaseLost
		}
		return err
	}
	local := *s.h
	local.Queries = qtx
	intake := *s
	intake.h = &local
	route, err := intake.completionRoute(ctx, event, installationID)
	if err != nil {
		return err
	}
	if route.autopilot.ID != delivery.AutopilotID || route.trigger.ID != delivery.TriggerID || route.autopilot.WorkspaceID != delivery.WorkspaceID {
		return errSlackCompletionUnauthorized
	}
	if err := assertIssueStatusStillActive(ctx, qtx, delivery.WorkspaceID, route.route.CompletionStatusKey); err != nil {
		return err
	}
	status, err := issuestatus.Resolve(ctx, qtx, delivery.WorkspaceID, route.route.CompletionStatusKey)
	if err != nil {
		return err
	}
	if status.Category != issuestatus.CategoryDone {
		return errors.New("Slack completion status must belong to the done category")
	}
	issue, err := qtx.LockIssueForDescriptionUpdate(ctx, db.LockIssueForDescriptionUpdateParams{ID: run.IssueID, WorkspaceID: delivery.WorkspaceID})
	if err != nil {
		return err
	}
	complete := func(state, reason string) error {
		_, err := qtx.CompleteClaimedWebhookDelivery(ctx, db.CompleteClaimedWebhookDeliveryParams{ID: delivery.ID, LeaseToken: delivery.LeaseToken, Status: state, AutopilotRunID: run.ID, Error: pgtype.Text{String: reason, Valid: reason != ""}})
		return err
	}
	if issue.OriginType.String != "autopilot" || issue.OriginID != route.autopilot.ID || issue.AssigneeType.String != "agent" || issue.AssigneeID != route.installation.AgentID {
		if err := complete(deliveryStatusIgnored, "Slack completion issue ownership changed"); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}
	if issue.Status == status.Key || issuestatus.NewResolver(issue.WorkspaceID).Category(ctx, qtx, issue.Status) == issuestatus.CategoryClosed {
		if err := complete(deliveryStatusIgnored, "Slack completion does not change this issue status"); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}
	tasks, err := qtx.ListTasksByIssue(ctx, issue.ID)
	if err != nil {
		return err
	}
	for _, task := range tasks {
		var contextData struct {
			WakeupID string `json:"wakeup_id"`
		}
		_ = json.Unmarshal(task.Context, &contextData)
		active := task.Status == "queued" || task.Status == "dispatched" || task.Status == "running" || task.Status == "waiting_local_directory" || (task.Status == "deferred" && contextData.WakeupID == "")
		if active {
			if _, err := qtx.DeferClaimedWebhookDelivery(ctx, db.DeferClaimedWebhookDeliveryParams{ID: delivery.ID, LeaseToken: delivery.LeaseToken, AvailableAt: pgtype.Timestamptz{Time: time.Now().Add(5 * time.Second), Valid: true}}); err != nil {
				return err
			}
			return tx.Commit(ctx)
		}
	}
	updated, err := qtx.UpdateIssueStatus(ctx, db.UpdateIssueStatusParams{ID: issue.ID, WorkspaceID: issue.WorkspaceID, Status: status.Key})
	if err != nil {
		return err
	}
	cancelled, err := service.StopClosedIssueWakeups(ctx, qtx, updated)
	if err != nil {
		return err
	}
	content := fmt.Sprintf("Marked %s after <@%s> added :%s: to Slack message %s/%s.", status.Name, event.UserID, event.Reaction, event.ChannelID, event.MessageTS)
	created, err := qtx.CreateComment(ctx, db.CreateCommentParams{ID: delivery.ID, IssueID: issue.ID, WorkspaceID: issue.WorkspaceID, AuthorType: "system", AuthorID: pgtype.UUID{Valid: true}, Type: "system", Content: content, SuppressedAgentIds: []pgtype.UUID{route.installation.AgentID}})
	if err != nil {
		return err
	}
	if err := complete(deliveryStatusDispatched, ""); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	updated.Revision = created.IssueRevision
	response := issueToResponse(updated, s.h.getIssuePrefix(ctx, issue.WorkspaceID))
	s.h.fillStatusCategory(ctx, issue.WorkspaceID, &response)
	s.h.publish(protocol.EventIssueUpdated, uuidToString(issue.WorkspaceID), "system", "", map[string]any{"issue": response, "status_changed": true, "prev_status": issue.Status, "prev_title": issue.Title, "creator_type": issue.CreatorType, "creator_id": uuidToString(issue.CreatorID)})
	comment := commentToResponse(created.Comment(), nil, nil)
	comment.IssueRevision = created.IssueRevision
	s.h.publish(protocol.EventCommentCreated, uuidToString(issue.WorkspaceID), "system", "", map[string]any{"comment": comment, "issue_title": issue.Title, "issue_status": status.Key, "issue_revision": created.IssueRevision})
	s.h.broadcastCancelledWakeups(ctx, issue.WorkspaceID, cancelled)
	s.h.processChildEvents(ctx, updated.ParentIssueID)
	return nil
}
