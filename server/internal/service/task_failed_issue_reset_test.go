package service

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/internal/testutil"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/dbid"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

func failedIssueResetFixture(t *testing.T) (*testutil.Fixture, string, string) {
	t.Helper()
	fx := testutil.New(sharedTestPool(t), "", "")
	suffix := util.UUIDToString(dbid.NewV7())
	fx.UserID = fx.User(t, "failure reset", "failure-reset-"+suffix+"@example.test")
	fx.WorkspaceID = fx.Workspace(t, "failure reset", "failure-reset-"+suffix)
	fx.Member(t, fx.WorkspaceID, fx.UserID, "owner")
	runtimeID := fx.Runtime(t, "failure reset")
	agentID := fx.Agent(t, "failure reset", runtimeID)
	fx.Insert(t, "issue_status", testutil.Cols{
		"workspace_id": fx.WorkspaceID, "key": "ai_done", "name": "AI Done", "category": "done", "color": "#123456",
	})
	return fx, agentID, runtimeID
}

// Preserve the actual service read/write window: the human update or follow-up
// commits after HandleFailedTasks read the issue, before its status write.
type beforeFailedIssueResetDB struct {
	db.DBTX
	before func()
}

func (q *beforeFailedIssueResetDB) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	if q.before != nil && strings.Contains(sql, "UPDATE issue AS i SET") {
		before := q.before
		q.before = nil
		before()
	}
	return q.DBTX.QueryRow(ctx, sql, args...)
}

func TestHandleFailedTasksResetPreservesHumanCompletionAndFollowup(t *testing.T) {
	for _, race := range []string{"human completion", "queued followup"} {
		t.Run(race, func(t *testing.T) {
			fx, agentID, runtimeID := failedIssueResetFixture(t)
			ctx := context.Background()
			issueID := fx.Issue(t, "failure reset race", testutil.Cols{"status": "in_progress"})
			failedID := fx.Task(t, agentID, testutil.Cols{
				"runtime_id": runtimeID, "issue_id": issueID, "status": "failed", "failure_reason": "agent_error",
			})
			q := db.New(fx.Pool)
			failed, err := q.GetAgentTask(ctx, util.MustParseUUID(failedID))
			if err != nil {
				t.Fatal(err)
			}
			var before db.Issue
			intercept := &beforeFailedIssueResetDB{DBTX: fx.Pool, before: func() {
				if race == "human completion" {
					if _, err := q.UpdateIssueStatus(ctx, db.UpdateIssueStatusParams{
						ID: util.MustParseUUID(issueID), WorkspaceID: util.MustParseUUID(fx.WorkspaceID), Status: "ai_done",
					}); err != nil {
						t.Fatal(err)
					}
				} else {
					fx.Task(t, agentID, testutil.Cols{"runtime_id": runtimeID, "issue_id": issueID, "status": "queued"})
				}
				before, err = q.GetIssue(ctx, util.MustParseUUID(issueID))
				if err != nil {
					t.Fatal(err)
				}
			}}
			bus := events.New()
			issueEvents := 0
			bus.SubscribeAll(func(event events.Event) {
				if event.Type == protocol.EventIssueUpdated {
					issueEvents++
				}
			})
			svc := NewTaskService(db.New(intercept), fx.Pool, nil, bus)
			if retried := svc.HandleFailedTasks(ctx, []db.AgentTaskQueue{failed}); retried != 0 {
				t.Fatalf("unexpected retries: %d", retried)
			}
			if intercept.before != nil {
				t.Fatal("service did not reach the recovery status write")
			}
			after, err := q.GetIssue(ctx, util.MustParseUUID(issueID))
			if err != nil {
				t.Fatal(err)
			}
			if after.Status != before.Status || after.Revision != before.Revision || after.UpdatedAt != before.UpdatedAt {
				t.Fatalf("stale recovery changed issue: before=%s/%d after=%s/%d", before.Status, before.Revision, after.Status, after.Revision)
			}
			if issueEvents != 0 {
				t.Fatalf("rejected reset published %d issue updates", issueEvents)
			}
		})
	}
}

func TestResetStuckIssueAfterFailedTaskGuards(t *testing.T) {
	fx, agentID, runtimeID := failedIssueResetFixture(t)
	ctx := context.Background()
	q := db.New(fx.Pool)
	fx.Issue(t, "existing todo", testutil.Cols{"position": -20})
	for _, state := range []string{"idle", "ai_done", "wrong workspace", "queued", "dispatched", "running", "waiting_local_directory", "deferred retry", "deferred media"} {
		t.Run(state, func(t *testing.T) {
			issueID := fx.Issue(t, state, testutil.Cols{"status": "in_progress", "position": 42})
			failedID := fx.Task(t, agentID, testutil.Cols{"runtime_id": runtimeID, "issue_id": issueID, "status": "failed"})
			if state == "ai_done" {
				fx.Exec(t, "UPDATE issue SET status='ai_done' WHERE id=$1", issueID)
			} else if state != "idle" && state != "wrong workspace" {
				cols := testutil.Cols{"runtime_id": runtimeID, "issue_id": issueID, "status": state}
				if state == "deferred retry" || state == "deferred media" {
					cols["status"] = "deferred"
					cols["fire_at"] = testutil.Raw("now() + interval '1 hour'")
					if state == "deferred retry" {
						cols["retry_of_task_id"] = failedID
					} else {
						cols["context"] = []byte(`{"channel_issue_media_pending":true}`)
					}
				}
				if state == "running" {
					cols["started_at"] = testutil.Raw("now()")
				}
				if state == "waiting_local_directory" {
					cols["wait_reason"] = "directory unavailable"
				}
				fx.Task(t, agentID, cols)
			}
			before, err := q.GetIssue(ctx, util.MustParseUUID(issueID))
			if err != nil {
				t.Fatal(err)
			}
			workspaceID := util.MustParseUUID(fx.WorkspaceID)
			if state == "wrong workspace" {
				workspaceID = dbid.NewV7()
			}
			updated, err := q.ResetStuckIssueAfterFailedTask(ctx, db.ResetStuckIssueAfterFailedTaskParams{
				ID: before.ID, WorkspaceID: workspaceID, SourceTaskID: util.MustParseUUID(failedID),
			})
			if state == "idle" {
				if err != nil || updated.Status != "todo" || updated.Revision != before.Revision+1 || updated.Position >= -20 {
					t.Fatalf("idle recovery status/revision/position = %s/%d/%g, err=%v", updated.Status, updated.Revision, updated.Position, err)
				}
			} else {
				if !errors.Is(err, pgx.ErrNoRows) {
					t.Fatalf("guard returned %v, want no rows", err)
				}
				after, readErr := q.GetIssue(ctx, before.ID)
				if readErr != nil {
					t.Fatal(readErr)
				}
				if after.Status != before.Status || after.Revision != before.Revision || after.Position != before.Position {
					t.Fatal("guarded recovery changed status, revision or position")
				}
			}
		})
	}
}
