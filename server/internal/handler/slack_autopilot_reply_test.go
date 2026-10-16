package handler

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/internal/integrations/channel"
	"github.com/multica-ai/multica/server/internal/integrations/channel/engine"
	"github.com/multica-ai/multica/server/internal/integrations/slack"
	"github.com/multica-ai/multica/server/internal/service"
	"github.com/multica-ai/multica/server/internal/testutil"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/dbid"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

func startSlackAutopilotReport(t *testing.T, f *slackAutopilotFixture) (db.WebhookDelivery, db.AutopilotRun, db.AgentTaskQueue) {
	t.Helper()
	parent := f.ingest(t, f.event)
	f.processOne(t)
	run := f.run(t, parent.ID)
	tasks, err := f.h.Queries.ListTasksByIssue(context.Background(), run.IssueID)
	if err != nil || len(tasks) != 1 {
		t.Fatalf("initial report tasks=%d err=%v", len(tasks), err)
	}
	if tasks[0].TriggerCommentID.Valid || tasks[0].CommentThreadID.Valid {
		t.Fatal("initial report must exercise the assignment task's NULL thread")
	}
	return parent, run, tasks[0]
}

func slackAutopilotTestReply(f *slackAutopilotFixture) slack.AutopilotEvent {
	reply := f.event
	reply.ThreadTS = f.event.MessageTS
	reply.MessageTS = "1700000000.000101"
	reply.Text = "The same checkout failure also happens in Firefox."
	return reply
}

func TestRecentSlackAutopilotThread(t *testing.T) {
	now := time.Date(2026, time.October, 16, 12, 0, 0, 0, time.UTC)
	stamp := func(delta time.Duration) string {
		return strconv.FormatInt(now.Add(delta).Unix(), 10) + ".000100"
	}
	for _, tc := range []struct {
		name, timestamp string
		want            bool
	}{
		{"fresh", stamp(-2 * time.Minute), true},
		{"ten-minute boundary", stamp(-10 * time.Minute), true},
		{"historic", stamp(-11 * time.Minute), false},
		{"clock skew", stamp(30 * time.Second), true},
		{"future", stamp(2 * time.Minute), false},
		{"malformed", "not-a-timestamp", false},
		{"empty", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := recentSlackAutopilotThread(tc.timestamp, now); got != tc.want {
				t.Fatalf("recentSlackAutopilotThread(%q)=%v, want %v", tc.timestamp, got, tc.want)
			}
		})
	}
}

func TestSlackAutopilotReplyMergesQueuedAssignmentWithoutAnotherRun(t *testing.T) {
	f := newSlackAutopilotFixture(t, nil)
	parent, run, initial := startSlackAutopilotReport(t, f)
	replyEvent := slackAutopilotTestReply(f)
	reply := f.ingest(t, replyEvent)
	f.processOne(t)
	final := f.delivery(t, reply.ID)
	if final.Status != deliveryStatusDispatched || final.AutopilotRunID != run.ID {
		t.Fatalf("reply delivery status=%s run=%s error=%q", final.Status, uuidToString(final.AutopilotRunID), final.Error.String)
	}
	if _, err := f.h.Queries.GetAutopilotRunByWebhookDelivery(context.Background(), reply.ID); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("reply created an Autopilot run: %v", err)
	}
	if n := dbfx.Count(t, `SELECT count(*) FROM autopilot_run WHERE autopilot_id=$1`, f.route.AutopilotID); n != 1 {
		t.Fatalf("reply produced %d runs, want original run only", n)
	}
	if n := dbfx.Count(t, `SELECT count(*) FROM issue WHERE origin_type='autopilot' AND origin_id=$1`, f.route.AutopilotID); n != 1 {
		t.Fatalf("reply produced %d issues", n)
	}
	tasks, err := f.h.Queries.ListTasksByIssue(context.Background(), run.IssueID)
	if err != nil || len(tasks) != 1 || tasks[0].ID != initial.ID || tasks[0].TriggerCommentID != reply.ID || tasks[0].CommentThreadID != parent.ID {
		t.Fatalf("reply did not merge into the original task: tasks=%+v err=%v", tasks, err)
	}
	root, err := f.h.Queries.GetCommentInWorkspace(context.Background(), db.GetCommentInWorkspaceParams{ID: parent.ID, WorkspaceID: parseUUID(testWorkspaceID)})
	if err != nil || root.IssueID != run.IssueID || root.ParentID.Valid || !strings.Contains(root.Content, f.event.Text) {
		t.Fatalf("original Slack root missing or mis-scoped: root=%+v err=%v", root, err)
	}
	comment, err := f.h.Queries.GetCommentInWorkspace(context.Background(), db.GetCommentInWorkspaceParams{ID: reply.ID, WorkspaceID: parseUUID(testWorkspaceID)})
	if err != nil || comment.IssueID != run.IssueID || comment.ParentID != parent.ID || comment.AuthorID != parseUUID(testUserID) || !strings.Contains(comment.Content, replyEvent.Text) || !strings.Contains(comment.Content, "Runbook") || !strings.Contains(comment.Content, "Reproduce and fix the report.") || !strings.Contains(comment.Content, "/p1700000000000101") {
		t.Fatalf("reply lost content, instructions, principal or source link: comment=%+v err=%v", comment, err)
	}
}

func TestSlackAutopilotReplyDuplicateIsIdempotent(t *testing.T) {
	f := newSlackAutopilotFixture(t, nil)
	_, run, initial := startSlackAutopilotReport(t, f)
	event := slackAutopilotTestReply(f)
	first := f.ingest(t, event)
	if duplicate := f.ingest(t, event); duplicate.ID != first.ID {
		t.Fatal("message/app-mention pair created different reply deliveries")
	}
	f.processOne(t)
	if duplicate := f.ingest(t, event); duplicate.ID != first.ID || duplicate.Status != deliveryStatusDispatched {
		t.Fatalf("completed duplicate changed identity or status: %+v", duplicate)
	}
	if n := dbfx.Count(t, `SELECT count(*) FROM comment WHERE issue_id=$1`, uuidToString(run.IssueID)); n != 2 {
		t.Fatalf("duplicate reply produced %d comments, want root plus reply", n)
	}
	tasks, err := f.h.Queries.ListTasksByIssue(context.Background(), run.IssueID)
	if err != nil || len(tasks) != 1 || tasks[0].ID != initial.ID {
		t.Fatalf("duplicate reply created another task: tasks=%+v err=%v", tasks, err)
	}
	if n := dbfx.Count(t, `SELECT count(*) FROM autopilot_run WHERE autopilot_id=$1`, f.route.AutopilotID); n != 1 {
		t.Fatalf("duplicate reply created %d runs", n)
	}
}

func TestSlackAutopilotReplyAfterFinishedTaskContinuesTheSameIssue(t *testing.T) {
	f := newSlackAutopilotFixture(t, nil)
	parent, run, initial := startSlackAutopilotReport(t, f)
	dbfx.Exec(t, `UPDATE agent_task_queue SET status='completed', completed_at=now() WHERE id=$1`, uuidToString(initial.ID))
	reply := f.ingest(t, slackAutopilotTestReply(f))
	f.processOne(t)
	tasks, err := f.h.Queries.ListTasksByIssue(context.Background(), run.IssueID)
	if err != nil || len(tasks) != 2 {
		t.Fatalf("finished report followup tasks=%d err=%v", len(tasks), err)
	}
	for _, task := range tasks {
		if task.ID == initial.ID {
			if task.Status != "completed" || task.TriggerCommentID.Valid {
				t.Fatal("reply rewrote the finished assignment task")
			}
		} else if task.Status != "queued" || task.TriggerCommentID != reply.ID || task.CommentThreadID != parent.ID {
			t.Fatalf("successor has wrong status or thread: %+v", task)
		}
	}
	if final := f.delivery(t, reply.ID); final.Status != deliveryStatusDispatched || final.AutopilotRunID != run.ID {
		t.Fatalf("reply did not retain original run/issue: %+v", final)
	}
	if n := dbfx.Count(t, `SELECT count(*) FROM autopilot_run WHERE autopilot_id=$1`, f.route.AutopilotID); n != 1 {
		t.Fatalf("finished report followup created %d Autopilot runs", n)
	}
}

func TestSlackAutopilotReplySurvivesUnsupportedRunningTaskEnding(t *testing.T) {
	for _, terminal := range []string{"failed", "cancelled"} {
		t.Run(terminal, func(t *testing.T) {
			f := newSlackAutopilotFixture(t, nil)
			parent, run, initial := startSlackAutopilotReport(t, f)
			dbfx.Exec(t, `UPDATE agent_task_queue SET status='running', dispatched_at=now(), started_at=now() WHERE id=$1`, uuidToString(initial.ID))
			reply := f.ingest(t, slackAutopilotTestReply(f))
			for attempt := 0; attempt < 2; attempt++ {
				if attempt > 0 {
					dbfx.Exec(t, `UPDATE webhook_delivery SET available_at=now() WHERE id=$1`, uuidToString(reply.ID))
				}
				f.processOne(t)
				if deferred := f.delivery(t, reply.ID); deferred.Status != deliveryStatusQueued || deferred.DispatchAttempts != 0 || deferred.LeaseToken.Valid {
					t.Fatalf("waiting for a running task consumed retry attempts: %+v", deferred)
				}
			}
			active, err := f.h.Queries.GetAgentTask(context.Background(), initial.ID)
			if err != nil || active.TriggerCommentID != parent.ID || len(active.CoalescedCommentIds) != 1 || active.CoalescedCommentIds[0] != reply.ID {
				t.Fatalf("reply has no durable planned obligation: task=%+v err=%v", active, err)
			}
			if len(active.DeliveredCommentIds) != 1 || active.DeliveredCommentIds[0] != parent.ID {
				t.Fatalf("reply was incorrectly marked delivered: %+v", active.DeliveredCommentIds)
			}
			if n := dbfx.Count(t, `SELECT count(*) FROM agent_task_queue WHERE issue_id=$1`, uuidToString(run.IssueID)); n != 1 {
				t.Fatalf("unsupported running task got %d concurrent tasks", n)
			}
			// No completion callback reconciles failed/cancelled tasks: the durable
			// reply delivery itself must arrange the successor after either outcome.
			dbfx.Exec(t, `UPDATE agent_task_queue SET status=$2, completed_at=now() WHERE id=$1`, uuidToString(initial.ID), terminal)
			dbfx.Exec(t, `UPDATE webhook_delivery SET available_at=now() WHERE id=$1`, uuidToString(reply.ID))
			f.processOne(t)
			if final := f.delivery(t, reply.ID); final.Status != deliveryStatusDispatched || final.AutopilotRunID != run.ID {
				t.Fatalf("terminal task stranded the reply: %+v", final)
			}
			if n := dbfx.Count(t, `SELECT count(*) FROM agent_task_queue WHERE issue_id=$1 AND id<>$2 AND status='queued' AND trigger_comment_id=$3 AND comment_thread_id=$4`, uuidToString(run.IssueID), uuidToString(initial.ID), uuidToString(reply.ID), uuidToString(parent.ID)); n != 1 {
				t.Fatalf("terminal task produced %d same-issue reply successors", n)
			}
			if n := dbfx.Count(t, `SELECT count(*) FROM comment WHERE issue_id=$1`, uuidToString(run.IssueID)); n != 2 {
				t.Fatalf("durable retry duplicated comments: %d", n)
			}
		})
	}
}

func TestSlackAutopilotReplyWaitsForParentMediaWithoutSpendingAttempts(t *testing.T) {
	resolver := &slackAutopilotMediaResolver{resolve: func(_ context.Context, _ engine.ResolvedInstallation, _ engine.ResolvedIdentity, _, _ pgtype.UUID, msg channel.InboundMessage) channel.InboundMessage {
		return msg
	}}
	f := newSlackAutopilotFixture(t, resolver)
	f.event.Files = []slack.AutopilotFile{slackAutopilotScreenshot()}
	parent := f.ingest(t, f.event)
	f.processOne(t)
	if run := f.run(t, parent.ID); run.IssueID.Valid || f.delivery(t, parent.ID).Status != deliveryStatusQueued {
		t.Fatalf("fixture parent did not remain in media preparation: %+v", run)
	}
	dbfx.Exec(t, `UPDATE webhook_delivery SET available_at=now()+interval '1 hour' WHERE id=$1`, uuidToString(parent.ID))
	replyEvent := slackAutopilotTestReply(f)
	replyEvent.Files = nil
	reply := f.ingest(t, replyEvent)
	for attempt := 0; attempt < 2; attempt++ {
		if attempt > 0 {
			dbfx.Exec(t, `UPDATE webhook_delivery SET available_at=now() WHERE id=$1`, uuidToString(reply.ID))
		}
		f.processOne(t)
		if deferred := f.delivery(t, reply.ID); deferred.Status != deliveryStatusQueued || deferred.DispatchAttempts != 0 || deferred.LeaseToken.Valid {
			t.Fatalf("parent media wait consumed reply attempts: %+v", deferred)
		}
	}
	if resolver.calls != 1 {
		t.Fatalf("waiting reply tried to resolve media %d times, want parent only", resolver.calls)
	}
	if n := dbfx.Count(t, `SELECT count(*) FROM autopilot_run WHERE autopilot_id=$1`, f.route.AutopilotID); n != 1 {
		t.Fatalf("reply waiting for parent created %d runs", n)
	}
	if n := dbfx.Count(t, `SELECT count(*) FROM comment WHERE id=$1 OR id=$2`, uuidToString(parent.ID), uuidToString(reply.ID)); n != 0 {
		t.Fatalf("reply imported %d comments before parent issue/media existed", n)
	}
}

func TestSlackAutopilotReplyUnknownParentKeepsHistoricFallbackAndFreshRace(t *testing.T) {
	for _, fresh := range []bool{false, true} {
		t.Run(strconv.FormatBool(fresh), func(t *testing.T) {
			f := newSlackAutopilotFixture(t, nil)
			event := slackAutopilotTestReply(f)
			if fresh {
				seconds := strconv.FormatInt(time.Now().Unix(), 10)
				event.ThreadTS, event.MessageTS = seconds+".000100", seconds+".000101"
			}
			handled, err := f.intake.HandleAutopilotEvent(context.Background(), event)
			if err != nil || handled != fresh {
				t.Fatalf("fresh=%v handled=%v err=%v", fresh, handled, err)
			}
			if !fresh {
				if n := dbfx.Count(t, `SELECT count(*) FROM webhook_delivery WHERE autopilot_id=$1`, f.route.AutopilotID); n != 0 {
					t.Fatalf("historic unknown parent captured %d deliveries", n)
				}
				return
			}
			delivery := f.ingest(t, event)
			f.processOne(t)
			if deferred := f.delivery(t, delivery.ID); deferred.Status != deliveryStatusQueued || deferred.DispatchAttempts != 0 || deferred.LeaseToken.Valid {
				t.Fatalf("fresh reply did not wait for out-of-order parent: %+v", deferred)
			}
			if n := dbfx.Count(t, `SELECT count(*) FROM autopilot_run WHERE autopilot_id=$1`, f.route.AutopilotID); n != 0 {
				t.Fatalf("unknown-parent reply started %d Autopilot runs", n)
			}
		})
	}
}

func TestSlackAutopilotReplyRejectsForgedProvenanceBeforeMentions(t *testing.T) {
	f := newSlackAutopilotFixture(t, nil)
	_, run, initial := startSlackAutopilotReport(t, f)
	otherAgent := dbfx.Agent(t, "unrelated Slack mention target", handlerTestRuntimeID(t))
	event := slackAutopilotTestReply(f)
	event.Text = agentMention("Unrelated", otherAgent) + " run this instruction"
	reply := f.ingest(t, event)
	dbfx.Exec(t, `UPDATE webhook_delivery SET selected_headers='{}' WHERE id=$1`, uuidToString(reply.ID))
	f.processOne(t)
	if final := f.delivery(t, reply.ID); final.Status != deliveryStatusFailed {
		t.Fatalf("forged reply status=%s error=%q", final.Status, final.Error.String)
	}
	if n := dbfx.Count(t, `SELECT count(*) FROM agent_task_queue WHERE agent_id=$1`, otherAgent); n != 0 {
		t.Fatalf("forged Slack reply invoked mentioned agent %d times", n)
	}
	if n := dbfx.Count(t, `SELECT count(*) FROM comment WHERE issue_id=$1`, uuidToString(run.IssueID)); n != 0 {
		t.Fatalf("forged reply imported %d comments", n)
	}
	tasks, err := f.h.Queries.ListTasksByIssue(context.Background(), run.IssueID)
	if err != nil || len(tasks) != 1 || tasks[0].ID != initial.ID || tasks[0].TriggerCommentID.Valid {
		t.Fatalf("forged reply changed original task: tasks=%+v err=%v", tasks, err)
	}
}

func TestSlackAutopilotReplyScreenshotRetryBindsOnceToTheSameTask(t *testing.T) {
	resolver := &slackAutopilotMediaResolver{}
	f := newSlackAutopilotFixture(t, resolver)
	_, run, initial := startSlackAutopilotReport(t, f)
	event := slackAutopilotTestReply(f)
	event.Files = []slack.AutopilotFile{slackAutopilotScreenshot()}
	reply := f.ingest(t, event)
	key := "test/slack-reply/" + uuidToString(reply.ID) + "/screenshot.png"
	dbfx.Cleanup(t, `DELETE FROM channel_media_pending_object WHERE storage_key=$1`, key)
	resolver.resolve = func(ctx context.Context, inst engine.ResolvedInstallation, _ engine.ResolvedIdentity, _, messageID pgtype.UUID, msg channel.InboundMessage) channel.InboundMessage {
		if messageID != reply.ID || msg.Source.ThreadID != event.ThreadTS {
			t.Fatalf("reply media lost delivery or thread identity: message=%s source=%+v", uuidToString(messageID), msg.Source)
		}
		if resolver.calls == 1 {
			return msg
		}
		url := "https://storage.example.test/" + key
		if _, err := f.h.Queries.RecordChannelMediaPendingObject(ctx, db.RecordChannelMediaPendingObjectParams{StorageKey: key, WorkspaceID: inst.WorkspaceID, InstallationID: inst.ID, ChatMessageID: messageID, StorageUrl: url}); err != nil {
			t.Fatal(err)
		}
		msg.MediaRefs = []channel.MediaRef{{Type: channel.MsgTypeImage, Filename: "reply.png", StorageKey: key, StorageURL: url, MimeType: "image/png", SizeBytes: 123}}
		return msg
	}
	f.processOne(t)
	if retry := f.delivery(t, reply.ID); retry.Status != deliveryStatusQueued || retry.DispatchAttempts != 1 {
		t.Fatalf("reply screenshot failure did not retry: %+v", retry)
	}
	if n := dbfx.Count(t, `SELECT count(*) FROM comment WHERE issue_id=$1`, uuidToString(run.IssueID)); n != 0 {
		t.Fatalf("failed media preparation committed %d comments", n)
	}
	dbfx.Exec(t, `UPDATE webhook_delivery SET available_at=now() WHERE id=$1`, uuidToString(reply.ID))
	f.processOne(t)
	if final := f.delivery(t, reply.ID); final.Status != deliveryStatusDispatched || final.AutopilotRunID != run.ID || resolver.calls != 2 {
		t.Fatalf("reply screenshot retry did not settle once: delivery=%+v calls=%d", final, resolver.calls)
	}
	if n := dbfx.Count(t, `SELECT count(*) FROM attachment WHERE issue_id=$1 AND comment_id=$2`, uuidToString(run.IssueID), uuidToString(reply.ID)); n != 1 {
		t.Fatalf("reply screenshot attached %d times", n)
	}
	if n := dbfx.Count(t, `SELECT count(*) FROM channel_media_pending_object WHERE storage_key=$1`, key); n != 0 {
		t.Fatal("successful reply left a pending media object")
	}
	comment, err := f.h.Queries.GetCommentInWorkspace(context.Background(), db.GetCommentInWorkspaceParams{ID: reply.ID, WorkspaceID: parseUUID(testWorkspaceID)})
	if err != nil || !strings.Contains(comment.Content, "/api/attachments/") || !strings.Contains(comment.Content, event.Text) {
		t.Fatalf("reply lost screenshot markdown or text: comment=%+v err=%v", comment, err)
	}
	_ = f.ingest(t, event)
	if n := dbfx.Count(t, `SELECT count(*) FROM comment WHERE issue_id=$1`, uuidToString(run.IssueID)); n != 2 {
		t.Fatalf("reply media retry produced %d comments", n)
	}
	tasks, err := f.h.Queries.ListTasksByIssue(context.Background(), run.IssueID)
	if err != nil || len(tasks) != 1 || tasks[0].ID != initial.ID || tasks[0].TriggerCommentID != reply.ID {
		t.Fatalf("reply media retry duplicated the initial task: tasks=%+v err=%v", tasks, err)
	}
}

func TestSlackAutopilotReplyRevalidatesInstallationAfterSlowMedia(t *testing.T) {
	resolver := &slackAutopilotMediaResolver{}
	f := newSlackAutopilotFixture(t, resolver)
	_, run, initial := startSlackAutopilotReport(t, f)
	event := slackAutopilotTestReply(f)
	event.Files = []slack.AutopilotFile{slackAutopilotScreenshot()}
	reply := f.ingest(t, event)
	key := "test/slack-revoked-reply/" + uuidToString(reply.ID) + "/screenshot.png"
	dbfx.Cleanup(t, `DELETE FROM channel_media_pending_object WHERE storage_key=$1`, key)
	resolver.resolve = func(ctx context.Context, inst engine.ResolvedInstallation, _ engine.ResolvedIdentity, _, messageID pgtype.UUID, msg channel.InboundMessage) channel.InboundMessage {
		url := "https://storage.example.test/" + key
		if _, err := f.h.Queries.RecordChannelMediaPendingObject(ctx, db.RecordChannelMediaPendingObjectParams{StorageKey: key, WorkspaceID: inst.WorkspaceID, InstallationID: inst.ID, ChatMessageID: messageID, StorageUrl: url}); err != nil {
			t.Fatal(err)
		}
		dbfx.Exec(t, `UPDATE channel_installation SET status='revoked' WHERE id=$1`, uuidToString(inst.ID))
		msg.MediaRefs = []channel.MediaRef{{Type: channel.MsgTypeImage, Filename: "reply.png", StorageKey: key, StorageURL: url, MimeType: "image/png", SizeBytes: 123}}
		return msg
	}
	f.processOne(t)
	if final := f.delivery(t, reply.ID); final.Status == deliveryStatusDispatched || resolver.calls != 1 {
		t.Fatalf("reply committed after installation revocation: delivery=%+v calls=%d", final, resolver.calls)
	}
	if n := dbfx.Count(t, `SELECT count(*) FROM comment WHERE issue_id=$1`, uuidToString(run.IssueID)); n != 0 {
		t.Fatalf("revoked reply committed %d comments", n)
	}
	if n := dbfx.Count(t, `SELECT count(*) FROM attachment WHERE issue_id=$1`, uuidToString(run.IssueID)); n != 0 {
		t.Fatalf("revoked reply bound %d screenshots", n)
	}
	tasks, err := f.h.Queries.ListTasksByIssue(context.Background(), run.IssueID)
	if err != nil || len(tasks) != 1 || tasks[0].ID != initial.ID || tasks[0].TriggerCommentID.Valid || len(tasks[0].CoalescedCommentIds) != 0 {
		t.Fatalf("revoked reply mutated agent input: tasks=%+v err=%v", tasks, err)
	}
}

func TestSlackAutopilotReplyStaleLeaseCannotCommitCommentsOrTasks(t *testing.T) {
	f := newSlackAutopilotFixture(t, nil)
	parent, run, initial := startSlackAutopilotReport(t, f)
	event := slackAutopilotTestReply(f)
	reply := f.ingest(t, event)
	claimed, err := f.h.Queries.ClaimQueuedWebhookDelivery(context.Background())
	if err != nil || claimed.ID != reply.ID || !claimed.LeaseToken.Valid {
		t.Fatalf("reply claim=%+v err=%v", claimed, err)
	}
	newLease := dbid.NewV7()
	dbfx.Exec(t, `UPDATE webhook_delivery SET lease_token=$2 WHERE id=$1`, uuidToString(reply.ID), uuidToString(newLease))
	issue, err := f.h.Queries.GetIssue(context.Background(), run.IssueID)
	if err != nil {
		t.Fatal(err)
	}
	agent, err := f.h.Queries.GetAgent(context.Background(), initial.AgentID)
	if err != nil {
		t.Fatal(err)
	}
	deferred, task, err := f.intake.commitReply(context.Background(), claimed, parent, run, issue, agent, parseUUID(testUserID), f.event, event, service.AutopilotIssueMedia{})
	if !errors.Is(err, service.ErrWebhookDeliveryLeaseLost) || deferred || task.ID.Valid {
		t.Fatalf("stale reply commit: deferred=%v task=%s err=%v", deferred, uuidToString(task.ID), err)
	}
	if final := f.delivery(t, reply.ID); final.Status != deliveryStatusQueued || final.LeaseToken != newLease || final.DispatchAttempts != 0 {
		t.Fatalf("stale reply changed the current lease: %+v", final)
	}
	if n := dbfx.Count(t, `SELECT count(*) FROM comment WHERE issue_id=$1`, uuidToString(run.IssueID)); n != 0 {
		t.Fatalf("stale reply committed %d comments", n)
	}
	tasks, err := f.h.Queries.ListTasksByIssue(context.Background(), run.IssueID)
	if err != nil || len(tasks) != 1 || tasks[0].ID != initial.ID || tasks[0].TriggerCommentID.Valid {
		t.Fatalf("stale reply mutated the initial task: tasks=%+v err=%v", tasks, err)
	}
}

func enableSlackAutopilotReplySupplements(t *testing.T, run db.AutopilotRun, initial db.AgentTaskQueue) {
	t.Helper()
	runtimeID := dbfx.Runtime(t, "Slack reply supplement runtime", testutil.Cols{"provider": "codex"})
	dbfx.Exec(t, `UPDATE agent SET runtime_id=$2 WHERE id=$1`, uuidToString(initial.AgentID), runtimeID)
	dbfx.Exec(t, `UPDATE agent_task_queue SET runtime_id=$2, status='running', dispatched_at=now(), started_at=now() WHERE id=$1`, uuidToString(initial.ID), runtimeID)
	dbfx.Exec(t, `INSERT INTO task_supplement_capability (task_id, workspace_id, issue_id, capability) VALUES ($1,$2,$3,$4)`, uuidToString(initial.ID), testWorkspaceID, uuidToString(run.IssueID), protocol.DaemonCapabilityTaskSupplementV1)
	dbfx.Cleanup(t, `DELETE FROM task_supplement_capability WHERE task_id=$1`, uuidToString(initial.ID))
	dbfx.Cleanup(t, `DELETE FROM task_supplement WHERE task_id=$1`, uuidToString(initial.ID))
}

func TestSlackAutopilotReplySteersNegotiatedRunningTaskWithoutCompletionReplay(t *testing.T) {
	f := newSlackAutopilotFixture(t, nil)
	parent, run, initial := startSlackAutopilotReport(t, f)
	enableSlackAutopilotReplySupplements(t, run, initial)
	reply := f.ingest(t, slackAutopilotTestReply(f))
	f.processOne(t)
	if waiting := f.delivery(t, reply.ID); waiting.Status != deliveryStatusQueued || waiting.DispatchAttempts != 0 {
		t.Fatalf("pending supplement prematurely settled the reply: %+v", waiting)
	}
	if n := dbfx.Count(t, `SELECT count(*) FROM task_supplement WHERE task_id=$1 AND comment_id=$2 AND status='pending'`, uuidToString(initial.ID), uuidToString(reply.ID)); n != 1 {
		t.Fatalf("running task received %d reply supplements", n)
	}
	active, err := f.h.Queries.GetAgentTask(context.Background(), initial.ID)
	if err != nil || active.TriggerCommentID != parent.ID || len(active.DeliveredCommentIds) != 1 || active.DeliveredCommentIds[0] != parent.ID {
		t.Fatalf("adopted running task lost original-report coverage: task=%+v err=%v", active, err)
	}
	// Polling the durable reply must never rebind an existing pending or
	// delivering supplement, and neither state is proof of receipt.
	for _, phase := range []string{"pending", "delivering"} {
		if phase == "delivering" {
			if claimed, err := f.h.Queries.ClaimNextTaskSupplement(context.Background(), initial.ID); err != nil || claimed.CommentID != reply.ID {
				t.Fatalf("claim supplement: comment=%s err=%v", uuidToString(claimed.CommentID), err)
			}
		}
		dbfx.Exec(t, `UPDATE webhook_delivery SET available_at=now() WHERE id=$1`, uuidToString(reply.ID))
		f.processOne(t)
		if waiting := f.delivery(t, reply.ID); waiting.Status != deliveryStatusQueued || waiting.DispatchAttempts != 0 {
			t.Fatalf("%s supplement lost its durable obligation: %+v", phase, waiting)
		}
		if n := dbfx.Count(t, `SELECT count(*) FROM task_supplement WHERE task_id=$1 AND comment_id=$2`, uuidToString(initial.ID), uuidToString(reply.ID)); n != 1 {
			t.Fatalf("%s supplement was bound %d times", phase, n)
		}
	}
	if receipt, err := f.h.Queries.AckTaskSupplementDelivered(context.Background(), db.AckTaskSupplementDeliveredParams{TaskID: initial.ID, CommentID: reply.ID}); err != nil || receipt.Status != "delivered" {
		t.Fatalf("acknowledge real delivery: receipt=%+v err=%v", receipt, err)
	}
	completeTaskViaDaemon(t, uuidToString(initial.ID))
	dbfx.Exec(t, `UPDATE webhook_delivery SET available_at=now() WHERE id=$1`, uuidToString(reply.ID))
	f.processOne(t)
	if final := f.delivery(t, reply.ID); final.Status != deliveryStatusDispatched || final.AutopilotRunID != run.ID {
		t.Fatalf("delivered supplement did not settle the reply after completion: %+v", final)
	}
	if n := dbfx.Count(t, `SELECT count(*) FROM agent_task_queue WHERE issue_id=$1`, uuidToString(run.IssueID)); n != 1 {
		t.Fatalf("completion replayed a steered reply or its synthetic root into %d tasks", n)
	}
}

func TestSlackAutopilotReplyRecoversUndeliveredSupplement(t *testing.T) {
	for _, tc := range []struct {
		phase, terminal string
	}{
		{"pending", "completed"}, {"delivering", "completed"},
		{"pending", "cancelled"}, {"delivering", "failed"},
		{"provider_rejected", "completed"},
	} {
		t.Run(tc.phase+"/"+tc.terminal, func(t *testing.T) {
			f := newSlackAutopilotFixture(t, nil)
			parent, run, initial := startSlackAutopilotReport(t, f)
			enableSlackAutopilotReplySupplements(t, run, initial)
			reply := f.ingest(t, slackAutopilotTestReply(f))
			f.processOne(t)
			if tc.phase != "pending" {
				if _, err := f.h.Queries.ClaimNextTaskSupplement(context.Background(), initial.ID); err != nil {
					t.Fatal(err)
				}
			}
			if tc.phase == "provider_rejected" {
				if _, err := f.h.Queries.AckTaskSupplementFailed(context.Background(), db.AckTaskSupplementFailedParams{TaskID: initial.ID, CommentID: reply.ID, FailureReason: pgtype.Text{String: tc.phase, Valid: true}}); err != nil {
					t.Fatal(err)
				}
				dbfx.Exec(t, `UPDATE webhook_delivery SET available_at=now() WHERE id=$1`, uuidToString(reply.ID))
				f.processOne(t)
				if waiting := f.delivery(t, reply.ID); waiting.Status != deliveryStatusQueued || waiting.DispatchAttempts != 0 {
					t.Fatalf("provider rejection lost the durable followup: %+v", waiting)
				}
			}
			if tc.terminal == "completed" {
				completeTaskViaDaemon(t, uuidToString(initial.ID))
			} else {
				dbfx.Exec(t, `UPDATE agent_task_queue SET status=$2, completed_at=now() WHERE id=$1`, uuidToString(initial.ID), tc.terminal)
				if _, err := f.h.Queries.SettleTerminalTaskSupplements(context.Background(), []pgtype.UUID{initial.ID}); err != nil {
					t.Fatal(err)
				}
			}
			dbfx.Exec(t, `UPDATE webhook_delivery SET available_at=now() WHERE id=$1`, uuidToString(reply.ID))
			f.processOne(t)
			if final := f.delivery(t, reply.ID); final.Status != deliveryStatusDispatched || final.AutopilotRunID != run.ID {
				t.Fatalf("undelivered supplement stranded its reply: %+v", final)
			}
			if n := dbfx.Count(t, `SELECT count(*) FROM agent_task_queue WHERE issue_id=$1 AND id<>$2 AND status='queued' AND trigger_comment_id=$3 AND comment_thread_id=$4`, uuidToString(run.IssueID), uuidToString(initial.ID), uuidToString(reply.ID), uuidToString(parent.ID)); n != 1 {
				t.Fatalf("undelivered supplement created %d same-issue successors", n)
			}
			if n := dbfx.Count(t, `SELECT count(*) FROM task_supplement WHERE task_id=$1 AND comment_id=$2`, uuidToString(initial.ID), uuidToString(reply.ID)); n != 1 {
				t.Fatalf("undelivered supplement created %d bindings", n)
			}
			if n := dbfx.Count(t, `SELECT count(*) FROM comment WHERE issue_id=$1`, uuidToString(run.IssueID)); n != 2 {
				t.Fatalf("undelivered supplement duplicated the reply into %d comments", n)
			}
		})
	}
}

func TestSlackAutopilotReplyKeepsSupplementOnItsBoundTurn(t *testing.T) {
	f := newSlackAutopilotFixture(t, nil)
	parent, run, initial := startSlackAutopilotReport(t, f)
	enableSlackAutopilotReplySupplements(t, run, initial)
	reply := f.ingest(t, slackAutopilotTestReply(f))
	f.processOne(t)
	newerID := dbfx.Task(t, uuidToString(initial.AgentID), testutil.Cols{
		"issue_id": uuidToString(run.IssueID), "runtime_id": uuidToString(initial.RuntimeID),
		"trigger_comment_id": uuidToString(parent.ID),
	})
	dbfx.Exec(t, `UPDATE webhook_delivery SET available_at=now() WHERE id=$1`, uuidToString(reply.ID))
	f.processOne(t)
	if waiting := f.delivery(t, reply.ID); waiting.Status != deliveryStatusQueued || waiting.DispatchAttempts != 0 {
		t.Fatalf("newer task prematurely settled the pending supplement: %+v", waiting)
	}
	newer, err := f.h.Queries.GetAgentTask(context.Background(), parseUUID(newerID))
	if err != nil || newer.TriggerCommentID != parent.ID || len(newer.CoalescedCommentIds) != 0 {
		t.Fatalf("pending supplement also merged into newer queued task: task=%+v err=%v", newer, err)
	}
	if n := dbfx.Count(t, `SELECT count(*) FROM task_supplement WHERE comment_id=$1 AND task_id=$2`, uuidToString(reply.ID), uuidToString(initial.ID)); n != 1 {
		t.Fatalf("reply has %d bindings on its original turn", n)
	}
	if _, err := f.h.Queries.ClaimNextTaskSupplement(context.Background(), initial.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.h.Queries.AckTaskSupplementDelivered(context.Background(), db.AckTaskSupplementDeliveredParams{TaskID: initial.ID, CommentID: reply.ID}); err != nil {
		t.Fatal(err)
	}
	dbfx.Exec(t, `UPDATE webhook_delivery SET available_at=now() WHERE id=$1`, uuidToString(reply.ID))
	f.processOne(t)
	if final := f.delivery(t, reply.ID); final.Status != deliveryStatusDispatched {
		t.Fatalf("original turn receipt did not settle the reply: %+v", final)
	}
	newer, err = f.h.Queries.GetAgentTask(context.Background(), parseUUID(newerID))
	if err != nil || newer.TriggerCommentID != parent.ID || len(newer.CoalescedCommentIds) != 0 {
		t.Fatalf("delivered reply reached the unrelated queued turn: task=%+v err=%v", newer, err)
	}
}

func TestSlackAutopilotReplyPublishesRootBeforeReplies(t *testing.T) {
	f := newSlackAutopilotFixture(t, nil)
	parent, run, _ := startSlackAutopilotReport(t, f)
	var comments []CommentResponse
	f.h.Bus.Subscribe(protocol.EventCommentCreated, func(event events.Event) {
		payload, ok := event.Payload.(map[string]any)
		if !ok {
			return
		}
		comment, ok := payload["comment"].(CommentResponse)
		if ok && comment.IssueID == uuidToString(run.IssueID) {
			comments = append(comments, comment)
		}
	})
	event := slackAutopilotTestReply(f)
	first := f.ingest(t, event)
	f.processOne(t)
	if len(comments) != 2 || comments[0].ID != uuidToString(parent.ID) || comments[1].ID != uuidToString(first.ID) || comments[0].IssueRevision == 0 || comments[1].IssueRevision != comments[0].IssueRevision+1 {
		t.Fatalf("first reply events must introduce root then reply with committed revisions: %+v", comments)
	}
	event.MessageTS = "1700000000.000102"
	second := f.ingest(t, event)
	f.processOne(t)
	if len(comments) != 3 || comments[2].ID != uuidToString(second.ID) {
		t.Fatalf("later reply repeated its synthetic root: %+v", comments)
	}
}

func TestSlackAutopilotReplyAdoptsQueuedRetryOfTheInitialTask(t *testing.T) {
	f := newSlackAutopilotFixture(t, nil)
	parent, run, initial := startSlackAutopilotReport(t, f)
	dbfx.Exec(t, `UPDATE agent_task_queue SET status='failed', completed_at=now() WHERE id=$1`, uuidToString(initial.ID))
	retryID := dbfx.Task(t, uuidToString(initial.AgentID), testutil.Cols{
		"issue_id": uuidToString(run.IssueID), "runtime_id": uuidToString(initial.RuntimeID),
		"parent_task_id": uuidToString(initial.ID), "retry_of_task_id": uuidToString(initial.ID),
		"originator_user_id": testUserID, "accountable_user_id": testUserID,
		"originator_source": "direct_human", "trigger_evidence_kind": "issue_assignment", "trigger_evidence_ref_id": uuidToString(run.IssueID),
	})
	reply := f.ingest(t, slackAutopilotTestReply(f))
	f.processOne(t)
	retry, err := f.h.Queries.GetAgentTask(context.Background(), parseUUID(retryID))
	if err != nil || retry.Status != "queued" || retry.TriggerCommentID != reply.ID || retry.CommentThreadID != parent.ID {
		t.Fatalf("reply did not adopt the initial task's queued retry: retry=%+v err=%v", retry, err)
	}
	if n := dbfx.Count(t, `SELECT count(*) FROM agent_task_queue WHERE issue_id=$1`, uuidToString(run.IssueID)); n != 2 {
		t.Fatalf("retry adoption created %d tasks, want failed original plus retry", n)
	}
	if final := f.delivery(t, reply.ID); final.Status != deliveryStatusDispatched || final.AutopilotRunID != run.ID {
		t.Fatalf("reply did not settle against the existing retry: %+v", final)
	}
}
