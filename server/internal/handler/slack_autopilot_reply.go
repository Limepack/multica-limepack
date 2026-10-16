package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/internal/integrations/slack"
	"github.com/multica-ai/multica/server/internal/service"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

func isSlackAutopilotReply(event slack.AutopilotEvent) bool {
	return event.ThreadTS != "" && event.ThreadTS != event.MessageTS
}

func recentSlackAutopilotThread(ts string, now time.Time) bool {
	seconds, err := strconv.ParseInt(strings.SplitN(ts, ".", 2)[0], 10, 64)
	return err == nil && now.Sub(time.Unix(seconds, 0)) <= 10*time.Minute && now.Sub(time.Unix(seconds, 0)) >= -time.Minute
}

func (s *SlackAutopilotIntake) parentDelivery(ctx context.Context, ap db.Autopilot, trigger db.AutopilotTrigger, event slack.AutopilotEvent) (db.WebhookDelivery, error) {
	parent, err := s.h.Queries.GetWebhookDeliveryByTriggerAndDedupe(ctx, db.GetWebhookDeliveryByTriggerAndDedupeParams{
		TriggerID: trigger.ID, DedupeKey: pgtype.Text{String: "slack:" + event.TeamID + ":" + event.ChannelID + ":" + event.ThreadTS, Valid: true},
	})
	if err == nil && (parent.WorkspaceID != ap.WorkspaceID || parent.AutopilotID != ap.ID || parent.Event != slackAutopilotEvent) {
		return db.WebhookDelivery{}, errors.New("Slack thread parent ownership mismatch")
	}
	return parent, err
}

func nativeSlackAutopilotEvent(delivery db.WebhookDelivery) (slack.AutopilotEvent, error) {
	var selected map[string]json.RawMessage
	var provenance slackAutopilotProvenance
	var envelope slackAutopilotEnvelope
	if json.Unmarshal(delivery.SelectedHeaders, &selected) != nil ||
		json.Unmarshal(selected[slackAutopilotProvenanceKey], &provenance) != nil ||
		json.Unmarshal(delivery.RawBody, &envelope) != nil {
		return slack.AutopilotEvent{}, errors.New("native Slack provenance required")
	}
	event := envelope.EventPayload
	if provenance.InstallationID == "" || event.AppID == "" || event.UserID == "" || event.MessageTS == "" ||
		provenance.AppID != event.AppID || provenance.TeamID != event.TeamID ||
		provenance.ChannelID != event.ChannelID || provenance.MessageTS != event.MessageTS ||
		envelope.Event != delivery.Event || (delivery.Event != slackAutopilotEvent && delivery.Event != slackAutopilotReplyEvent) {
		return slack.AutopilotEvent{}, errors.New("native Slack delivery provenance mismatch")
	}
	return event, nil
}

func (s *SlackAutopilotIntake) deferReply(ctx context.Context, delivery db.WebhookDelivery) error {
	_, err := s.h.Queries.DeferClaimedWebhookDelivery(ctx, db.DeferClaimedWebhookDeliveryParams{
		ID: delivery.ID, LeaseToken: delivery.LeaseToken,
		AvailableAt: pgtype.Timestamptz{Time: time.Now().Add(5 * time.Second), Valid: true},
	})
	_, err = handleWebhookLeaseMutation("defer Slack reply", delivery, err)
	return err
}

// Replies consume the durable delivery queue, never a second Autopilot run.
// Their comments, media and task obligation share the delivery's lease fence.
func (s *SlackAutopilotIntake) processReply(ctx context.Context, delivery db.WebhookDelivery) error {
	w := s.h.WebhookDeliveryWorker
	event, err := nativeSlackAutopilotEvent(delivery)
	if err != nil || !isSlackAutopilotReply(event) {
		return w.complete(ctx, delivery, deliveryStatusFailed, pgtype.UUID{}, "invalid native Slack reply")
	}
	var selected map[string]json.RawMessage
	var provenance slackAutopilotProvenance
	_ = json.Unmarshal(delivery.SelectedHeaders, &selected)
	_ = json.Unmarshal(selected[slackAutopilotProvenanceKey], &provenance)
	installationID, err := util.ParseUUID(provenance.InstallationID)
	if err != nil {
		return w.complete(ctx, delivery, deliveryStatusFailed, pgtype.UUID{}, "invalid Slack installation")
	}
	installation, err := s.h.Queries.GetChannelInstallationInWorkspace(ctx, db.GetChannelInstallationInWorkspaceParams{
		ID: installationID, WorkspaceID: delivery.WorkspaceID, ChannelType: string(slack.TypeSlack),
	})
	if err != nil {
		return w.retryOrFail(ctx, delivery, err)
	}
	route, matched := s.routeFor(event, installation)
	public := slack.DecodePublicConfig(installation.Config)
	if !matched || installation.Status != "active" || public.AppID != event.AppID || public.TeamID != event.TeamID || public.BotUserID == event.UserID {
		return w.complete(ctx, delivery, deliveryStatusIgnored, pgtype.UUID{}, "Slack thread route is no longer authorized")
	}
	ap, trigger, err := s.loadRoute(ctx, route, installation)
	if err != nil || ap.ID != delivery.AutopilotID || trigger.ID != delivery.TriggerID {
		return w.complete(ctx, delivery, deliveryStatusFailed, pgtype.UUID{}, "Slack thread route ownership mismatch")
	}
	if !trigger.Enabled || ap.Status != "active" {
		return w.complete(ctx, delivery, deliveryStatusIgnored, pgtype.UUID{}, "Slack thread autopilot is inactive")
	}
	principal := service.ResolveAutopilotTriggerPrincipal(ctx, s.h.Queries, trigger.ID, ap.ID, ap.WorkspaceID)
	agent, err := s.h.Queries.GetAgentInWorkspace(ctx, db.GetAgentInWorkspaceParams{ID: installation.AgentID, WorkspaceID: ap.WorkspaceID})
	if err != nil || !principal.Valid || !s.h.canInvokeAgent(ctx, agent, "member", uuidToString(principal), uuidToString(principal), uuidToString(ap.WorkspaceID)) {
		return w.complete(ctx, delivery, deliveryStatusFailed, pgtype.UUID{}, "Slack thread trigger principal is no longer authorized")
	}
	parent, err := s.parentDelivery(ctx, ap, trigger, event)
	if errors.Is(err, pgx.ErrNoRows) {
		if recentSlackAutopilotThread(event.ThreadTS, time.Now()) {
			return s.deferReply(ctx, delivery)
		}
		return w.complete(ctx, delivery, deliveryStatusIgnored, pgtype.UUID{}, "Slack report parent was not received")
	}
	if err != nil {
		return w.retryOrFail(ctx, delivery, err)
	}
	rootEvent, err := nativeSlackAutopilotEvent(parent)
	if err != nil || isSlackAutopilotReply(rootEvent) || rootEvent.AppID != event.AppID || rootEvent.TeamID != event.TeamID || rootEvent.ChannelID != event.ChannelID || rootEvent.MessageTS != event.ThreadTS {
		return w.complete(ctx, delivery, deliveryStatusFailed, pgtype.UUID{}, "Slack thread parent provenance mismatch")
	}
	if parent.Status == deliveryStatusQueued {
		return s.deferReply(ctx, delivery)
	}
	if parent.Status != deliveryStatusDispatched {
		return w.complete(ctx, delivery, deliveryStatusIgnored, pgtype.UUID{}, "Slack report parent did not create an issue")
	}
	run, err := s.h.Queries.GetAutopilotRunByWebhookDelivery(ctx, parent.ID)
	if err != nil {
		return w.retryOrFail(ctx, delivery, err)
	}
	if !run.IssueID.Valid {
		return w.complete(ctx, delivery, deliveryStatusIgnored, run.ID, "Slack report parent did not create an issue")
	}
	issue, err := s.h.Queries.GetIssue(ctx, run.IssueID)
	if err != nil || issue.WorkspaceID != ap.WorkspaceID || issue.OriginType.String != "autopilot" || issue.OriginID != ap.ID {
		return w.complete(ctx, delivery, deliveryStatusFailed, run.ID, "Slack report issue is unavailable or has mismatched ownership")
	}
	// Media is prepared only once; a deferred active task already has its
	// durable comment and attachments on the next pass.
	var prepared service.AutopilotIssueMedia
	_, commentErr := s.h.Queries.GetCommentInWorkspace(ctx, db.GetCommentInWorkspaceParams{ID: delivery.ID, WorkspaceID: ap.WorkspaceID})
	if errors.Is(commentErr, pgx.ErrNoRows) {
		prepared, err = s.PrepareIssueMedia(ctx, ap, db.AutopilotRun{TriggerID: trigger.ID, WebhookDeliveryID: delivery.ID})
		if err != nil {
			return w.retryOrFail(ctx, delivery, err)
		}
	} else if commentErr != nil {
		return w.retryOrFail(ctx, delivery, commentErr)
	}
	deferred, task, err := s.commitReply(ctx, delivery, parent, run, issue, agent, principal, rootEvent, event, prepared)
	if errors.Is(err, service.ErrWebhookDeliveryLeaseLost) {
		return nil
	}
	if err != nil {
		return w.retryOrFail(ctx, delivery, err)
	}
	if task.ID.Valid {
		if task.Status == "queued" {
			s.h.TaskService.NotifyTaskEnqueued(ctx, task)
		}
	}
	if deferred {
		return s.deferReply(ctx, delivery)
	}
	return nil
}

func (s *SlackAutopilotIntake) commitReply(ctx context.Context, delivery, parent db.WebhookDelivery, run db.AutopilotRun, issue db.Issue, agent db.Agent, principal pgtype.UUID, rootEvent, event slack.AutopilotEvent, prepared service.AutopilotIssueMedia) (bool, db.AgentTaskQueue, error) {
	tx, err := s.h.TxStarter.Begin(ctx)
	if err != nil {
		return false, db.AgentTaskQueue{}, err
	}
	defer tx.Rollback(ctx)
	qtx := s.h.Queries.WithTx(tx)
	if _, err := qtx.LockWebhookDeliveryForDispatch(ctx, db.LockWebhookDeliveryForDispatchParams{ID: delivery.ID, WorkspaceID: issue.WorkspaceID, LeaseToken: delivery.LeaseToken}); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			err = service.ErrWebhookDeliveryLeaseLost
		}
		return false, db.AgentTaskQueue{}, err
	}
	// Revalidate after slow downloads; a revoked route cannot commit comments
	// or agent input using the authorization observed before media I/O.
	local := *s.h
	local.Queries = qtx
	authorized := *s
	authorized.h = &local
	installation, err := qtx.GetChannelInstallationByAppID(ctx, db.GetChannelInstallationByAppIDParams{ChannelType: string(slack.TypeSlack), AppID: event.AppID})
	public := slack.DecodePublicConfig(installation.Config)
	if err != nil || installation.WorkspaceID != issue.WorkspaceID || installation.AgentID != agent.ID || installation.Status != "active" || public.AppID != event.AppID || public.TeamID != event.TeamID || public.BotUserID == event.UserID {
		return false, db.AgentTaskQueue{}, errors.New("Slack installation authorization changed")
	}
	route, matched := authorized.routeFor(event, installation)
	ap, trigger, routeErr := authorized.loadRoute(ctx, route, installation)
	agent, err = qtx.GetAgentInWorkspace(ctx, db.GetAgentInWorkspaceParams{ID: installation.AgentID, WorkspaceID: issue.WorkspaceID})
	if !matched || routeErr != nil || err != nil || agent.ArchivedAt.Valid || !agent.RuntimeID.Valid || ap.Status != "active" || !trigger.Enabled || ap.ID != delivery.AutopilotID || trigger.ID != delivery.TriggerID ||
		service.ResolveAutopilotTriggerPrincipal(ctx, qtx, trigger.ID, ap.ID, ap.WorkspaceID) != principal ||
		!local.canInvokeAgent(ctx, agent, "member", uuidToString(principal), uuidToString(principal), uuidToString(issue.WorkspaceID)) {
		return false, db.AgentTaskQueue{}, errors.New("Slack thread authorization changed")
	}
	instructions := ap.Description.String
	// CreateComment locks the issue before child/task rows, matching teardown.
	createdComment := false
	var issueRevision int64
	createdRoot := false
	var rootIssueRevision int64
	root, err := qtx.GetCommentInWorkspace(ctx, db.GetCommentInWorkspaceParams{ID: parent.ID, WorkspaceID: issue.WorkspaceID})
	if errors.Is(err, pgx.ErrNoRows) {
		created, createErr := qtx.CreateComment(ctx, db.CreateCommentParams{ID: parent.ID, IssueID: issue.ID, WorkspaceID: issue.WorkspaceID, AuthorType: "member", AuthorID: principal, Type: "comment", Content: "Original Slack report from <@" + rootEvent.UserID + ">:\n\n" + rootEvent.Text})
		err = createErr
		root = created.Comment()
		createdRoot, rootIssueRevision = createErr == nil, created.IssueRevision
	}
	if err != nil {
		return false, db.AgentTaskQueue{}, fmt.Errorf("resolve Slack root comment: %w", err)
	}
	if root.IssueID != issue.ID || root.ParentID.Valid {
		return false, db.AgentTaskQueue{}, errors.New("Slack root comment ownership mismatch")
	}
	comment, err := qtx.GetCommentInWorkspace(ctx, db.GetCommentInWorkspaceParams{ID: delivery.ID, WorkspaceID: issue.WorkspaceID})
	if errors.Is(err, pgx.ErrNoRows) {
		content := "Slack thread reply from <@" + event.UserID + ">:\n\n" + event.Text
		if instructions != "" {
			content += "\n\nAutopilot instructions (read the current Runbook before acting):\n\n" + instructions
		}
		if prepared.DescriptionAppendix != "" {
			content += "\n\n" + prepared.DescriptionAppendix
		}
		created, createErr := qtx.CreateComment(ctx, db.CreateCommentParams{ID: delivery.ID, IssueID: issue.ID, WorkspaceID: issue.WorkspaceID, AuthorType: "member", AuthorID: principal, Type: "comment", ParentID: root.ID, Content: content})
		if createErr != nil {
			return false, db.AgentTaskQueue{}, createErr
		}
		comment = created.Comment()
		createdComment, issueRevision = true, created.IssueRevision
		markdown, bindErr := service.BindAutopilotCommentMedia(ctx, qtx, issue, comment.ID, prepared)
		if bindErr != nil {
			return false, db.AgentTaskQueue{}, bindErr
		}
		if markdown != "" {
			comment.Content += "\n\n" + markdown
			if _, err := qtx.UpdateComment(ctx, db.UpdateCommentParams{ID: comment.ID, Content: comment.Content}); err != nil {
				return false, db.AgentTaskQueue{}, err
			}
		}
	} else if err != nil {
		return false, db.AgentTaskQueue{}, fmt.Errorf("resolve Slack reply comment: %w", err)
	}
	if comment.IssueID != issue.ID || comment.ParentID != root.ID {
		return false, db.AgentTaskQueue{}, errors.New("Slack reply comment ownership mismatch")
	}
	if err := qtx.AdoptSlackAutopilotTaskThread(ctx, db.AdoptSlackAutopilotTaskThreadParams{RootCommentID: root.ID, RunID: run.ID, WorkspaceID: issue.WorkspaceID, AgentID: agent.ID}); err != nil {
		return false, db.AgentTaskQueue{}, err
	}
	// Reused enqueue/steering helpers publish synchronously; hold their events
	// and daemon notifications until the delivery transaction commits.
	var committedEvents []events.Event
	bus := events.New()
	bus.SubscribeAll(func(event events.Event) { committedEvents = append(committedEvents, event) })
	local.Bus, local.DaemonTaskSupplement = bus, nil
	tasks := service.NewTaskService(qtx, s.h.TxStarter, s.h.TaskService.Hub, bus)
	tasks.Composio, tasks.FeatureFlags = s.h.TaskService.Composio, s.h.TaskService.FeatureFlags
	tasks.Entitlements = s.h.TaskService.Entitlements
	local.TaskService = tasks
	task, taskErr := qtx.GetActiveSlackAutopilotThreadTask(ctx, db.GetActiveSlackAutopilotThreadTaskParams{IssueID: issue.ID, WorkspaceID: issue.WorkspaceID, AgentID: agent.ID, RootCommentID: root.ID})
	if taskErr != nil && !errors.Is(taskErr, pgx.ErrNoRows) {
		return false, task, taskErr
	}
	covered, err := qtx.HasTaskCoveringDelegatedFailureComment(ctx, db.HasTaskCoveringDelegatedFailureCommentParams{IssueID: issue.ID, AgentID: agent.ID, CommentID: comment.ID})
	if err != nil {
		return false, task, err
	}
	deferred := false
	var supplementTask db.AgentTaskQueue
	delivered, err := qtx.HasDeliveredSlackAutopilotReply(ctx, db.HasDeliveredSlackAutopilotReplyParams{IssueID: issue.ID, WorkspaceID: issue.WorkspaceID, AgentID: agent.ID, CommentID: comment.ID})
	if err != nil {
		return false, task, err
	}
	// A pending supplement belongs to its exact turn, even when a newer
	// queued task exists in the thread. Never also merge it into that task.
	receipts, err := qtx.ListTaskSupplementsByCommentIDs(ctx, db.ListTaskSupplementsByCommentIDsParams{WorkspaceID: issue.WorkspaceID, CommentIds: []pgtype.UUID{comment.ID}})
	if err != nil {
		return false, task, err
	}
	for _, receipt := range receipts {
		if receipt.AgentID == agent.ID && receipt.Status == "delivered" {
			delivered = true
		}
	}
	for _, receipt := range receipts {
		if receipt.AgentID != agent.ID || (receipt.Status != "pending" && receipt.Status != "delivering") || delivered {
			continue
		}
		boundTask, err := qtx.GetAgentTask(ctx, receipt.TaskID)
		if err != nil {
			return false, task, err
		}
		if boundTask.Status == "queued" || boundTask.Status == "dispatched" || boundTask.Status == "running" || boundTask.Status == "waiting_local_directory" {
			deferred, supplementTask = true, boundTask
			break
		}
	}
	if !deferred && !covered && !delivered {
		trigger := commentAgentTrigger{Agent: agent, Source: commentTriggerSourceMentionAgent, AlreadyPending: task.ID.Valid, NonLeaderAgentReply: true}
		supplement, supplementErr := qtx.GetTaskSupplementForRun(ctx, db.GetTaskSupplementForRunParams{CommentID: comment.ID, TaskID: task.ID, WorkspaceID: issue.WorkspaceID})
		if supplementErr != nil && !errors.Is(supplementErr, pgx.ErrNoRows) {
			return false, task, supplementErr
		}
		if supplementErr == nil && (supplement.Status == "pending" || supplement.Status == "delivering") {
			deferred, supplementTask = true, task
		} else if supplementErr == nil && supplement.Status == "delivered" {
			delivered = true
		} else if errors.Is(supplementErr, pgx.ErrNoRows) {
			_, steered := local.steerCommentAgentTriggers(ctx, issue, comment, "member", []commentAgentTrigger{trigger}, []pgtype.UUID{task.ID})
			if len(steered) > 0 {
				deferred, supplementTask = true, task
			}
		}
		if !deferred && !delivered {
			status, reason := local.resolveCommentTriggerEnqueue(ctx, issue, trigger, comment.ID)
			if status == DispatchBlocked {
				return false, task, fmt.Errorf("route Slack reply: %s", reason)
			}
			deferred = status == DispatchDeferred
		}
	} else if !delivered && task.ID.Valid && task.Status != "queued" {
		// Keep the durable queue obligation until a running turn
		// actually receives it or ends, including failure/cancellation.
		deferred = true
	}
	if !deferred {
		if _, err := qtx.CompleteClaimedWebhookDelivery(ctx, db.CompleteClaimedWebhookDeliveryParams{ID: delivery.ID, LeaseToken: delivery.LeaseToken, Status: deliveryStatusDispatched, AutopilotRunID: run.ID}); err != nil {
			return false, task, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return false, task, err
	}
	publishCreated := func(created db.Comment, revision int64) {
		response := commentToResponse(created, nil, nil)
		response.IssueRevision = revision
		s.h.publish(protocol.EventCommentCreated, uuidToString(issue.WorkspaceID), "member", uuidToString(principal), map[string]any{
			"comment": response, "issue_title": issue.Title,
			"issue_assignee_type": textToPtr(issue.AssigneeType), "issue_assignee_id": uuidToPtr(issue.AssigneeID),
			"issue_status": issue.Status, "issue_revision": revision,
		})
	}
	if createdRoot {
		publishCreated(root, rootIssueRevision)
	}
	if createdComment {
		publishCreated(comment, issueRevision)
		s.h.TaskService.AutoUnresolveThreadOnReply(ctx, &root, uuidToString(issue.WorkspaceID), "member", uuidToString(principal), pgtype.UUID{})
	}
	for _, event := range committedEvents {
		s.h.Bus.Publish(event)
	}
	if supplementTask.ID.Valid {
		s.h.notifyTaskSupplementAvailable(supplementTask)
	}
	if !task.ID.Valid {
		task, _ = s.h.Queries.GetActiveSlackAutopilotThreadTask(ctx, db.GetActiveSlackAutopilotThreadTaskParams{IssueID: issue.ID, WorkspaceID: issue.WorkspaceID, AgentID: agent.ID, RootCommentID: root.ID})
	}
	return deferred, task, nil
}
