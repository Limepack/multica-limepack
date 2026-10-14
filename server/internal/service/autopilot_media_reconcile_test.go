package service

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/multica-ai/multica/server/internal/integrations/channel"
	"github.com/multica-ai/multica/server/internal/testutil"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/dbid"
)

func TestAutopilotIssueMediaReconcilePreservesBoundScreenshot(t *testing.T) {
	ctx := context.Background()
	pool := sharedTestPool(t)
	fx := testutil.New(pool, "", "")
	suffix := time.Now().UnixNano()
	fx.UserID = fx.User(t, "screenshot owner", fmt.Sprintf("media-reconcile-%d@example.test", suffix))
	fx.WorkspaceID = fx.Workspace(t, "screenshot workspace", fmt.Sprintf("media-reconcile-%d", suffix))
	fx.Member(t, fx.WorkspaceID, fx.UserID, "owner")
	otherWorkspaceID := fx.Workspace(t, "other workspace", fmt.Sprintf("media-reconcile-other-%d", suffix))
	issueID := fx.Issue(t, "bug with a bound screenshot")
	workspaceID := util.MustParseUUID(fx.WorkspaceID)
	deliveryID := dbid.NewV7()
	key := fmt.Sprintf("test/autopilot-media-reconcile-%d.png", suffix)
	url := "https://storage.example.test/" + key
	q := db.New(pool)
	fx.Cleanup(t, "DELETE FROM attachment WHERE issue_id=$1", issueID)
	fx.Cleanup(t, "DELETE FROM channel_media_pending_object WHERE storage_key=$1", key)
	if _, err := q.RecordChannelMediaPendingObject(ctx, db.RecordChannelMediaPendingObjectParams{
		StorageKey: key, WorkspaceID: workspaceID, ChatMessageID: deliveryID, StorageUrl: url,
	}); err != nil {
		t.Fatal(err)
	}

	// Complete the real issue binding first: its screenshot has no chat owner,
	// and its upload intent disappears in the same commit as the attachment.
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	qtx := q.WithTx(tx)
	issue, err := qtx.GetIssue(ctx, util.MustParseUUID(issueID))
	if err != nil {
		t.Fatal(err)
	}
	if err := bindAutopilotIssueMedia(ctx, qtx, &issue, AutopilotIssueMedia{
		UploaderID: util.MustParseUUID(fx.UserID),
		Refs: []channel.MediaRef{{Type: channel.MsgTypeImage, StorageKey: key,
			StorageURL: url, Filename: "screenshot.png", MimeType: "image/png", SizeBytes: 123}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if n := fx.Count(t, "SELECT count(*) FROM channel_media_pending_object WHERE storage_key=$1", key); n != 0 {
		t.Fatalf("bound screenshot retained %d upload intents", n)
	}
	if n := fx.Count(t, "SELECT count(*) FROM attachment WHERE issue_id=$1 AND chat_message_id IS NULL AND url=$2", issueID, url); n != 1 {
		t.Fatalf("expected one issue-owned screenshot, got %d", n)
	}

	for _, tc := range []struct {
		name      string
		workspace string
		keep      bool
	}{
		{name: "same workspace retains bound screenshot", workspace: fx.WorkspaceID, keep: true},
		{name: "other workspace cannot borrow issue reference", workspace: otherWorkspaceID},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ledgerWorkspace := util.MustParseUUID(tc.workspace)
			// A stale retry may re-record the deterministic intent after the
			// successful bind. Simulate its later reconciler claim directly so
			// this test does not sweep rows belonging to other test packages.
			if _, err := q.RecordChannelMediaPendingObject(ctx, db.RecordChannelMediaPendingObjectParams{
				StorageKey: key, WorkspaceID: ledgerWorkspace, ChatMessageID: deliveryID, StorageUrl: url,
			}); err != nil {
				t.Fatal(err)
			}
			lease := dbid.NewV7()
			fx.Exec(t, `UPDATE channel_media_pending_object
				SET state='deleting', lease_token=$2, attempt=1 WHERE storage_key=$1`, key, lease)
			deleter := &fakeObjectDeleter{}
			reconciler := &ChannelMediaReconciler{Queries: q, Storage: deleter}
			reconciler.settle(ctx, db.ChannelMediaPendingObject{
				StorageKey: key, WorkspaceID: ledgerWorkspace, ChatMessageID: deliveryID,
				StorageUrl: url, State: "deleting", LeaseToken: lease, Attempt: 1,
			}, lease)

			deleted := deleter.deletedKeys()
			if tc.keep {
				if len(deleted) != 0 {
					t.Fatalf("reconciler deleted a bound issue screenshot: %v", deleted)
				}
				if n := fx.Count(t, "SELECT count(*) FROM channel_media_pending_object WHERE storage_key=$1", key); n != 0 {
					t.Fatalf("referenced screenshot retained %d intents instead of clearing the stale retry", n)
				}
			} else {
				if len(deleted) != 1 || deleted[0] != key {
					t.Fatalf("cross-workspace reference suppressed deletion: %v", deleted)
				}
				var state string
				fx.QueryRow(t, "SELECT state FROM channel_media_pending_object WHERE storage_key=$1", key).Scan(&state)
				if state != "tombstoned" {
					t.Fatalf("unreferenced object state=%q, want tombstoned", state)
				}
			}
		})
	}
}
