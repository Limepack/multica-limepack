package handler

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/integrations/slack"
	"github.com/multica-ai/multica/server/internal/testutil"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	slackapi "github.com/slack-go/slack"
)

func slackAutopilotReactionFixture(t *testing.T) (*slackAutopilotFixture, db.AutopilotRun, db.AgentTaskQueue, slack.AutopilotReactionEvent) {
	t.Helper()
	f := newSlackAutopilotFixture(t, nil)
	dbfx.Insert(t, "issue_status", testutil.Cols{
		"workspace_id": testWorkspaceID, "key": "ai_done", "name": "AI Done",
		"category": "done", "color": "#123456",
	})
	f.route.CompletionStatusKey = "ai_done"
	f.intake.routes[0] = f.route
	f.intake.reactionTarget = func(_ context.Context, installation db.ChannelInstallation, event slack.AutopilotReactionEvent) (slack.AutopilotReactionTarget, error) {
		if installation.ID != f.install.ID || event.AppID != f.event.AppID {
			t.Fatal("reaction resolution lost its authenticated installation")
		}
		return slack.AutopilotReactionTarget{ThreadTS: f.event.MessageTS, Human: true, Exists: true}, nil
	}
	_, run, task := startSlackAutopilotReport(t, f)
	event := slack.AutopilotReactionEvent{
		AppID: f.event.AppID, TeamID: f.event.TeamID, ChannelID: f.event.ChannelID,
		UserID: "UHUMAN", MessageTS: f.event.MessageTS, Reaction: "white_check_mark", EventTS: "1700000001.000100",
	}
	return f, run, task, event
}

func ingestSlackAutopilotReaction(t *testing.T, f *slackAutopilotFixture, event slack.AutopilotReactionEvent) db.WebhookDelivery {
	t.Helper()
	handled, err := f.intake.HandleAutopilotReaction(context.Background(), event)
	if err != nil || !handled {
		t.Fatalf("native reaction handled=%v err=%v", handled, err)
	}
	key := strings.Join([]string{"slack-reaction", event.TeamID, event.ChannelID, event.MessageTS, event.UserID, event.Reaction, event.EventTS}, ":")
	delivery, err := f.h.Queries.GetWebhookDeliveryByTriggerAndDedupe(context.Background(), db.GetWebhookDeliveryByTriggerAndDedupeParams{
		TriggerID: parseUUID(f.route.TriggerID), DedupeKey: pgtype.Text{String: key, Valid: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	return delivery
}

func TestSlackAutopilotReactionCompletesRootOrSolutionWithoutAnotherRun(t *testing.T) {
	for _, solution := range []bool{false, true} {
		name := "root"
		if solution {
			name = "solution"
		}
		t.Run(name, func(t *testing.T) {
			f, run, initial, event := slackAutopilotReactionFixture(t)
			dbfx.Exec(t, `UPDATE agent_task_queue SET status='completed', completed_at=now() WHERE id=$1`, uuidToString(initial.ID))
			if solution {
				event.MessageTS = "1700000000.000200"
				dbfx.Exec(t, `UPDATE issue SET status='done' WHERE id=$1`, uuidToString(run.IssueID))
			}
			calls := 0
			resolve := f.intake.reactionTarget
			f.intake.reactionTarget = func(ctx context.Context, installation db.ChannelInstallation, got slack.AutopilotReactionEvent) (slack.AutopilotReactionTarget, error) {
				calls++
				if got.MessageTS != event.MessageTS {
					t.Fatal("reaction resolver did not receive the reacted-to message")
				}
				return resolve(ctx, installation, got)
			}
			delivery := ingestSlackAutopilotReaction(t, f, event)
			if calls != 0 {
				t.Fatal("Socket Mode intake performed the remote reaction lookup")
			}
			f.processOne(t)
			if final := f.delivery(t, delivery.ID); final.Status != deliveryStatusDispatched || final.AutopilotRunID != run.ID {
				t.Fatalf("reaction did not complete its original run/issue: %+v", final)
			}
			issue, err := f.h.Queries.GetIssue(context.Background(), run.IssueID)
			if err != nil || issue.Status != "ai_done" {
				t.Fatalf("reaction issue status=%q err=%v", issue.Status, err)
			}
			if calls != 1 {
				t.Fatalf("resolver calls=%d, want 1", calls)
			}
			comment, err := f.h.Queries.GetCommentInWorkspace(context.Background(), db.GetCommentInWorkspaceParams{ID: delivery.ID, WorkspaceID: parseUUID(testWorkspaceID)})
			if err != nil || comment.IssueID != run.IssueID || comment.AuthorType != "system" || !comment.AuthorID.Valid || comment.AuthorID.Bytes != [16]byte{} || !strings.Contains(comment.Content, event.UserID) {
				t.Fatalf("missing human-reaction audit comment: comment=%+v err=%v", comment, err)
			}
			if n := dbfx.Count(t, `SELECT count(*) FROM comment WHERE issue_id=$1`, uuidToString(run.IssueID)); n != 1 {
				t.Fatalf("reaction produced %d audit comments, want 1", n)
			}
			if n := dbfx.Count(t, `SELECT count(*) FROM agent_task_queue WHERE issue_id=$1`, uuidToString(run.IssueID)); n != 1 {
				t.Fatalf("reaction created agent work: %d tasks", n)
			}
			if n := dbfx.Count(t, `SELECT count(*) FROM autopilot_run WHERE autopilot_id=$1`, f.route.AutopilotID); n != 1 {
				t.Fatalf("reaction created %d Autopilot runs", n)
			}
		})
	}
}

func TestSlackAutopilotReactionDuplicateAndSameStatusAreNoops(t *testing.T) {
	f, run, initial, event := slackAutopilotReactionFixture(t)
	dbfx.Exec(t, `UPDATE agent_task_queue SET status='completed', completed_at=now() WHERE id=$1`, uuidToString(initial.ID))
	first := ingestSlackAutopilotReaction(t, f, event)
	duplicate := ingestSlackAutopilotReaction(t, f, event)
	if first.ID != duplicate.ID {
		t.Fatal("duplicate Slack reaction created another delivery")
	}
	f.processOne(t)
	if duplicate = ingestSlackAutopilotReaction(t, f, event); duplicate.ID != first.ID || duplicate.Status != deliveryStatusDispatched {
		t.Fatalf("settled reaction was replayed: %+v", duplicate)
	}
	event.EventTS = "1700000002.000100"
	event.UserID = "UANOTHERHUMAN"
	second := ingestSlackAutopilotReaction(t, f, event)
	f.processOne(t)
	if final := f.delivery(t, second.ID); final.Status != deliveryStatusIgnored {
		t.Fatalf("already-completed issue was not a no-op: %+v", final)
	}
	if n := dbfx.Count(t, `SELECT count(*) FROM comment WHERE issue_id=$1`, uuidToString(run.IssueID)); n != 1 {
		t.Fatalf("duplicate/already-done reactions created %d audit comments", n)
	}
	if n := dbfx.Count(t, `SELECT count(*) FROM agent_task_queue WHERE issue_id=$1`, uuidToString(run.IssueID)); n != 1 {
		t.Fatalf("duplicate reaction created %d tasks", n)
	}
}

func TestSlackAutopilotReactionWaitsForActiveTaskWithoutSpendingAttempts(t *testing.T) {
	for _, status := range []string{"queued", "dispatched", "running", "waiting_local_directory", "deferred"} {
		t.Run(status, func(t *testing.T) {
			f, run, initial, event := slackAutopilotReactionFixture(t)
			dbfx.Exec(t, `UPDATE agent_task_queue SET status=$2 WHERE id=$1`, uuidToString(initial.ID), status)
			delivery := ingestSlackAutopilotReaction(t, f, event)
			for attempt := 0; attempt < 2; attempt++ {
				if attempt > 0 {
					dbfx.Exec(t, `UPDATE webhook_delivery SET available_at=now() WHERE id=$1`, uuidToString(delivery.ID))
				}
				f.processOne(t)
				if deferred := f.delivery(t, delivery.ID); deferred.Status != deliveryStatusQueued || deferred.DispatchAttempts != 0 || deferred.LeaseToken.Valid {
					t.Fatalf("active-task wait consumed attempts: %+v", deferred)
				}
			}
			issue, err := f.h.Queries.GetIssue(context.Background(), run.IssueID)
			if err != nil || issue.Status == "ai_done" {
				t.Fatalf("reaction completed before active task ended: issue=%+v err=%v", issue, err)
			}
			if n := dbfx.Count(t, `SELECT count(*) FROM comment WHERE issue_id=$1`, uuidToString(run.IssueID)); n != 0 {
				t.Fatalf("deferred reaction posted %d premature audit comments", n)
			}
			dbfx.Exec(t, `UPDATE agent_task_queue SET status='failed', completed_at=now() WHERE id=$1`, uuidToString(initial.ID))
			dbfx.Exec(t, `UPDATE webhook_delivery SET available_at=now() WHERE id=$1`, uuidToString(delivery.ID))
			f.processOne(t)
			issue, err = f.h.Queries.GetIssue(context.Background(), run.IssueID)
			if err != nil || issue.Status != "ai_done" || f.delivery(t, delivery.ID).Status != deliveryStatusDispatched {
				t.Fatalf("reaction stranded after task ended: issue=%+v err=%v", issue, err)
			}
		})
	}
}

func TestSlackAutopilotReactionIgnoresMissingUntrackedOrNonhumanTargets(t *testing.T) {
	for _, tc := range []struct {
		name   string
		target slack.AutopilotReactionTarget
	}{
		{"deleted message", slack.AutopilotReactionTarget{Human: true}},
		{"bot reactor", slack.AutopilotReactionTarget{ThreadTS: "1700000000.000100", Exists: true}},
		{"unknown thread", slack.AutopilotReactionTarget{ThreadTS: "1600000000.000100", Human: true, Exists: true}},
		{"missing thread", slack.AutopilotReactionTarget{Human: true, Exists: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, run, initial, event := slackAutopilotReactionFixture(t)
			dbfx.Exec(t, `UPDATE agent_task_queue SET status='completed', completed_at=now() WHERE id=$1`, uuidToString(initial.ID))
			f.intake.reactionTarget = func(context.Context, db.ChannelInstallation, slack.AutopilotReactionEvent) (slack.AutopilotReactionTarget, error) {
				return tc.target, nil
			}
			delivery := ingestSlackAutopilotReaction(t, f, event)
			f.processOne(t)
			if final := f.delivery(t, delivery.ID); final.Status != deliveryStatusIgnored {
				t.Fatalf("irrelevant reaction was not ignored: %+v", final)
			}
			issue, err := f.h.Queries.GetIssue(context.Background(), run.IssueID)
			if err != nil || issue.Status == "ai_done" {
				t.Fatalf("irrelevant reaction completed issue: issue=%+v err=%v", issue, err)
			}
			if n := dbfx.Count(t, `SELECT count(*) FROM comment WHERE issue_id=$1`, uuidToString(run.IssueID)); n != 0 {
				t.Fatalf("irrelevant reaction created %d comments", n)
			}
		})
	}
}

func TestSlackAutopilotReactionRequiresOptInRouteAndAuthenticatedIdentity(t *testing.T) {
	for _, name := range []string{"no opt-in", "wrong channel", "wrong team", "wrong app", "wrong emoji"} {
		t.Run(name, func(t *testing.T) {
			f, _, _, event := slackAutopilotReactionFixture(t)
			switch name {
			case "no opt-in":
				f.intake.routes[0].CompletionStatusKey = ""
			case "wrong channel":
				event.ChannelID = "COTHER"
			case "wrong team":
				event.TeamID = "TOTHER"
			case "wrong app":
				event.AppID = "AUNKNOWN"
			case "wrong emoji":
				event.Reaction = "eyes"
			}
			handled, err := f.intake.HandleAutopilotReaction(context.Background(), event)
			if err != nil || handled {
				t.Fatalf("unmatched reaction handled=%v err=%v", handled, err)
			}
			if n := dbfx.Count(t, `SELECT count(*) FROM webhook_delivery WHERE autopilot_id=$1`, f.route.AutopilotID); n != 1 {
				t.Fatalf("unmatched reaction captured a delivery: %d", n)
			}
		})
	}
}

func TestSlackAutopilotReactionRejectsChangedOwnershipAndProvenance(t *testing.T) {
	for _, name := range []string{"forged provenance", "wrong installation", "route changed", "installation revoked", "reassigned issue", "different issue origin", "disabled trigger", "cancelled issue"} {
		t.Run(name, func(t *testing.T) {
			f, run, initial, event := slackAutopilotReactionFixture(t)
			dbfx.Exec(t, `UPDATE agent_task_queue SET status='completed', completed_at=now() WHERE id=$1`, uuidToString(initial.ID))
			delivery := ingestSlackAutopilotReaction(t, f, event)
			switch name {
			case "forged provenance":
				dbfx.Exec(t, `UPDATE webhook_delivery SET selected_headers='{}' WHERE id=$1`, uuidToString(delivery.ID))
			case "wrong installation":
				dbfx.Exec(t, `UPDATE webhook_delivery SET selected_headers=jsonb_set(selected_headers, '{multica_slack_autopilot,installation_id}', to_jsonb(gen_random_uuid()::text)) WHERE id=$1`, uuidToString(delivery.ID))
			case "route changed":
				f.intake.routes[0].ChannelID = "COTHER"
			case "installation revoked":
				dbfx.Exec(t, `UPDATE channel_installation SET status='revoked' WHERE id=$1`, uuidToString(f.install.ID))
			case "reassigned issue":
				dbfx.Exec(t, `UPDATE issue SET assignee_type='member', assignee_id=$2 WHERE id=$1`, uuidToString(run.IssueID), testUserID)
			case "different issue origin":
				dbfx.Exec(t, `UPDATE issue SET origin_type=NULL, origin_id=NULL WHERE id=$1`, uuidToString(run.IssueID))
			case "disabled trigger":
				dbfx.Exec(t, `UPDATE autopilot_trigger SET enabled=false WHERE id=$1`, f.route.TriggerID)
			case "cancelled issue":
				dbfx.Exec(t, `UPDATE issue SET status='cancelled' WHERE id=$1`, uuidToString(run.IssueID))
			}
			f.processOne(t)
			if final := f.delivery(t, delivery.ID); final.Status == deliveryStatusDispatched {
				t.Fatalf("unauthorized reaction was dispatched: %+v", final)
			}
			issue, err := f.h.Queries.GetIssue(context.Background(), run.IssueID)
			if err != nil || issue.Status == "ai_done" {
				t.Fatalf("unauthorized reaction changed issue: issue=%+v err=%v", issue, err)
			}
			if n := dbfx.Count(t, `SELECT count(*) FROM comment WHERE issue_id=$1`, uuidToString(run.IssueID)); n != 0 {
				t.Fatalf("unauthorized reaction created %d comments", n)
			}
		})
	}
}

func TestSlackAutopilotReactionRetriesResolverFailure(t *testing.T) {
	f, run, initial, event := slackAutopilotReactionFixture(t)
	dbfx.Exec(t, `UPDATE agent_task_queue SET status='completed', completed_at=now() WHERE id=$1`, uuidToString(initial.ID))
	resolve := f.intake.reactionTarget
	calls := 0
	f.intake.reactionTarget = func(ctx context.Context, installation db.ChannelInstallation, event slack.AutopilotReactionEvent) (slack.AutopilotReactionTarget, error) {
		calls++
		if calls == 1 {
			return slack.AutopilotReactionTarget{}, errors.New("Slack temporarily unavailable")
		}
		return resolve(ctx, installation, event)
	}
	delivery := ingestSlackAutopilotReaction(t, f, event)
	f.processOne(t)
	if retry := f.delivery(t, delivery.ID); retry.Status != deliveryStatusQueued || retry.DispatchAttempts != 1 {
		t.Fatalf("resolver failure did not retry: %+v", retry)
	}
	if n := dbfx.Count(t, `SELECT count(*) FROM comment WHERE issue_id=$1`, uuidToString(run.IssueID)); n != 0 {
		t.Fatalf("failed resolver committed %d comments", n)
	}
	dbfx.Exec(t, `UPDATE webhook_delivery SET available_at=now() WHERE id=$1`, uuidToString(delivery.ID))
	f.processOne(t)
	if final := f.delivery(t, delivery.ID); final.Status != deliveryStatusDispatched || calls != 2 {
		t.Fatalf("resolver retry did not complete: delivery=%+v calls=%d", final, calls)
	}
}

func TestSlackAutopilotReactionRejectsUnavailableOrNonDoneStatus(t *testing.T) {
	for _, name := range []string{"non-done category", "archived", "missing"} {
		t.Run(name, func(t *testing.T) {
			f, run, initial, event := slackAutopilotReactionFixture(t)
			dbfx.Exec(t, `UPDATE agent_task_queue SET status='completed', completed_at=now() WHERE id=$1`, uuidToString(initial.ID))
			if name == "non-done category" {
				dbfx.Insert(t, "issue_status", testutil.Cols{
					"workspace_id": testWorkspaceID, "key": "not_done", "name": "Not Done",
					"category": "started", "color": "#123456",
				})
				f.intake.routes[0].CompletionStatusKey = "not_done"
			}
			delivery := ingestSlackAutopilotReaction(t, f, event)
			switch name {
			case "archived":
				dbfx.Exec(t, `UPDATE issue_status SET archived_at=now() WHERE workspace_id=$1 AND key='ai_done'`, testWorkspaceID)
			case "missing":
				dbfx.Exec(t, `DELETE FROM issue_status WHERE workspace_id=$1 AND key='ai_done'`, testWorkspaceID)
			}
			f.processOne(t)
			if final := f.delivery(t, delivery.ID); final.Status == deliveryStatusDispatched {
				t.Fatalf("unavailable completion status accepted: %+v", final)
			}
			issue, err := f.h.Queries.GetIssue(context.Background(), run.IssueID)
			if err != nil || issue.Status == "ai_done" || issue.Status == "not_done" {
				t.Fatalf("unavailable completion status changed issue: issue=%+v err=%v", issue, err)
			}
			if n := dbfx.Count(t, `SELECT count(*) FROM comment WHERE issue_id=$1`, uuidToString(run.IssueID)); n != 0 {
				t.Fatalf("invalid completion status created %d comments", n)
			}
		})
	}
}

func TestSlackAutopilotReactionRevalidatesAfterSlowResolver(t *testing.T) {
	for _, name := range []string{"installation revoked", "lease replaced"} {
		t.Run(name, func(t *testing.T) {
			f, run, initial, event := slackAutopilotReactionFixture(t)
			dbfx.Exec(t, `UPDATE agent_task_queue SET status='completed', completed_at=now() WHERE id=$1`, uuidToString(initial.ID))
			delivery := ingestSlackAutopilotReaction(t, f, event)
			resolve := f.intake.reactionTarget
			f.intake.reactionTarget = func(ctx context.Context, installation db.ChannelInstallation, event slack.AutopilotReactionEvent) (slack.AutopilotReactionTarget, error) {
				if name == "installation revoked" {
					dbfx.Exec(t, `UPDATE channel_installation SET status='revoked' WHERE id=$1`, uuidToString(installation.ID))
				} else {
					dbfx.Exec(t, `UPDATE webhook_delivery SET lease_token=gen_random_uuid() WHERE id=$1`, uuidToString(delivery.ID))
				}
				return resolve(ctx, installation, event)
			}
			f.processOne(t)
			if final := f.delivery(t, delivery.ID); final.Status == deliveryStatusDispatched {
				t.Fatalf("stale authorization/lease committed reaction: %+v", final)
			}
			issue, err := f.h.Queries.GetIssue(context.Background(), run.IssueID)
			if err != nil || issue.Status == "ai_done" {
				t.Fatalf("stale authorization/lease changed issue: issue=%+v err=%v", issue, err)
			}
			if n := dbfx.Count(t, `SELECT count(*) FROM comment WHERE issue_id=$1`, uuidToString(run.IssueID)); n != 0 {
				t.Fatalf("stale authorization/lease committed %d comments", n)
			}
		})
	}
}

func TestSlackAutopilotReactionWaitsForFreshOutOfOrderParent(t *testing.T) {
	f, run, initial, event := slackAutopilotReactionFixture(t)
	dbfx.Exec(t, `UPDATE agent_task_queue SET status='completed', completed_at=now() WHERE id=$1`, uuidToString(initial.ID))
	freshThread := strconv.FormatInt(time.Now().Unix(), 10) + ".000100"
	event.MessageTS = freshThread
	f.intake.reactionTarget = func(context.Context, db.ChannelInstallation, slack.AutopilotReactionEvent) (slack.AutopilotReactionTarget, error) {
		return slack.AutopilotReactionTarget{ThreadTS: freshThread, Human: true, Exists: true}, nil
	}
	delivery := ingestSlackAutopilotReaction(t, f, event)
	for attempt := 0; attempt < 2; attempt++ {
		if attempt > 0 {
			dbfx.Exec(t, `UPDATE webhook_delivery SET available_at=now() WHERE id=$1`, uuidToString(delivery.ID))
		}
		f.processOne(t)
		if deferred := f.delivery(t, delivery.ID); deferred.Status != deliveryStatusQueued || deferred.DispatchAttempts != 0 || deferred.LeaseToken.Valid {
			t.Fatalf("fresh missing parent lost its reaction or consumed attempts: %+v", deferred)
		}
	}
	issue, err := f.h.Queries.GetIssue(context.Background(), run.IssueID)
	if err != nil || issue.Status == "ai_done" {
		t.Fatalf("unmapped fresh reaction changed another issue: issue=%+v err=%v", issue, err)
	}
	if n := dbfx.Count(t, `SELECT count(*) FROM comment WHERE issue_id=$1`, uuidToString(run.IssueID)); n != 0 {
		t.Fatalf("unmapped fresh reaction created %d comments", n)
	}
}

func TestSlackAutopilotReactionFailsImmediatelyForMissingScopes(t *testing.T) {
	f, run, initial, event := slackAutopilotReactionFixture(t)
	dbfx.Exec(t, `UPDATE agent_task_queue SET status='completed', completed_at=now() WHERE id=$1`, uuidToString(initial.ID))
	f.intake.reactionTarget = func(context.Context, db.ChannelInstallation, slack.AutopilotReactionEvent) (slack.AutopilotReactionTarget, error) {
		return slack.AutopilotReactionTarget{}, errors.New("missing_scope")
	}
	delivery := ingestSlackAutopilotReaction(t, f, event)
	f.processOne(t)
	final := f.delivery(t, delivery.ID)
	if final.Status != deliveryStatusFailed || !strings.Contains(final.Error.String, "reactions:read") || !strings.Contains(final.Error.String, "users:read") {
		t.Fatalf("missing scopes did not fail with actionable scope names: %+v", final)
	}
	issue, err := f.h.Queries.GetIssue(context.Background(), run.IssueID)
	if err != nil || issue.Status == "ai_done" {
		t.Fatalf("missing-scope failure changed issue: issue=%+v err=%v", issue, err)
	}
	if n := dbfx.Count(t, `SELECT count(*) FROM comment WHERE issue_id=$1`, uuidToString(run.IssueID)); n != 0 {
		t.Fatalf("missing-scope failure created %d comments", n)
	}
}

func TestSlackAutopilotReactionHonorsRateLimitWithoutSpendingAttempts(t *testing.T) {
	f, run, initial, event := slackAutopilotReactionFixture(t)
	dbfx.Exec(t, `UPDATE agent_task_queue SET status='completed', completed_at=now() WHERE id=$1`, uuidToString(initial.ID))
	f.intake.reactionTarget = func(context.Context, db.ChannelInstallation, slack.AutopilotReactionEvent) (slack.AutopilotReactionTarget, error) {
		return slack.AutopilotReactionTarget{}, &slackapi.RateLimitedError{RetryAfter: time.Minute}
	}
	delivery := ingestSlackAutopilotReaction(t, f, event)
	before := time.Now()
	f.processOne(t)
	after := time.Now()
	deferred := f.delivery(t, delivery.ID)
	if deferred.Status != deliveryStatusQueued || deferred.DispatchAttempts != 0 || deferred.LeaseToken.Valid {
		t.Fatalf("Slack rate limit consumed reaction attempts: %+v", deferred)
	}
	if !deferred.AvailableAt.Valid || deferred.AvailableAt.Time.Before(before.Add(time.Minute)) || deferred.AvailableAt.Time.After(after.Add(time.Minute)) {
		t.Fatalf("Slack Retry-After was not respected: available_at=%v, expected between %v and %v", deferred.AvailableAt, before.Add(time.Minute), after.Add(time.Minute))
	}
	issue, err := f.h.Queries.GetIssue(context.Background(), run.IssueID)
	if err != nil || issue.Status == "ai_done" {
		t.Fatalf("rate-limited reaction changed issue: issue=%+v err=%v", issue, err)
	}
	if n := dbfx.Count(t, `SELECT count(*) FROM comment WHERE issue_id=$1`, uuidToString(run.IssueID)); n != 0 {
		t.Fatalf("rate-limited reaction created %d comments", n)
	}
}
