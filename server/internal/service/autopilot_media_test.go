package service

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/multica-ai/multica/server/internal/integrations/channel"
	"github.com/multica-ai/multica/server/internal/testutil"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/dbid"
)

func TestWebhookIssueTitlesDistinctAndStable(t *testing.T) {
	svc := &AutopilotService{}
	ap := db.Autopilot{Title: "Slack bug report"}
	first := db.AutopilotRun{Source: "webhook", WebhookDeliveryID: util.MustParseUUID("019a1234-0000-7000-8000-000000000001")}
	second := first
	second.WebhookDeliveryID = util.MustParseUUID("019a1234-0000-7000-8000-000000000002")
	firstTitle := svc.interpolateTemplate(ap, first, "UTC")
	if firstTitle == svc.interpolateTemplate(ap, second, "UTC") {
		t.Fatal("two distinct deliveries with a static title must not collide")
	}
	if firstTitle != svc.interpolateTemplate(ap, first, "UTC") {
		t.Fatal("recovery must preserve the delivery's original issue title")
	}
	if got := svc.interpolateTemplate(ap, db.AutopilotRun{Source: "schedule"}, "UTC"); got != ap.Title {
		t.Fatalf("scheduled title changed: %q", got)
	}
}

func TestAutopilotMediaBindsAtomicallyWithIssue(t *testing.T) {
	ctx := context.Background()
	pool := sharedTestPool(t)
	fx := testutil.New(pool, "", "")
	suffix := time.Now().UnixNano()
	fx.UserID = fx.User(t, "media owner", fmt.Sprintf("autopilot-media-%d@example.test", suffix))
	fx.WorkspaceID = fx.Workspace(t, "media owner", fmt.Sprintf("autopilot-media-%d", suffix))
	fx.Member(t, fx.WorkspaceID, fx.UserID, "owner")
	issueID := fx.Issue(t, "bug with screenshot", testutil.Cols{"description": "Original report"})
	q := db.New(pool)
	workspaceID := util.MustParseUUID(fx.WorkspaceID)
	key := fmt.Sprintf("test/autopilot-media-%d.png", suffix)
	if _, err := q.RecordChannelMediaPendingObject(ctx, db.RecordChannelMediaPendingObjectParams{
		StorageKey: key, WorkspaceID: workspaceID, ChatMessageID: dbid.NewV7(),
		StorageUrl: "https://storage.example.test/" + key,
	}); err != nil {
		t.Fatal(err)
	}
	fx.Cleanup(t, "DELETE FROM attachment WHERE issue_id=$1", issueID)
	fx.Cleanup(t, "DELETE FROM channel_media_pending_object WHERE storage_key=$1", key)
	prepared := AutopilotIssueMedia{
		UploaderID: util.MustParseUUID(fx.UserID),
		Refs: []channel.MediaRef{{Type: channel.MsgTypeImage, StorageKey: key,
			StorageURL: "https://storage.example.test/" + key, Filename: "screenshot.png", MimeType: "image/png", SizeBytes: 123}},
	}
	for _, commit := range []bool{false, true} {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		qtx := q.WithTx(tx)
		issue, err := qtx.GetIssue(ctx, util.MustParseUUID(issueID))
		if err != nil {
			_ = tx.Rollback(ctx)
			t.Fatal(err)
		}
		if err := bindAutopilotIssueMedia(ctx, qtx, &issue, prepared); err != nil {
			_ = tx.Rollback(ctx)
			t.Fatal(err)
		}
		if !strings.Contains(issue.Description.String, "/api/attachments/") || !strings.Contains(issue.Description.String, "Original report") {
			_ = tx.Rollback(ctx)
			t.Fatalf("issue lacks original report or image attachment: %q", issue.Description.String)
		}
		if commit {
			if err := tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
		} else {
			_ = tx.Rollback(ctx)
		}
		var attachments, intents int
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM attachment WHERE issue_id=$1", issueID).Scan(&attachments); err != nil {
			t.Fatal(err)
		}
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM channel_media_pending_object WHERE storage_key=$1", key).Scan(&intents); err != nil {
			t.Fatal(err)
		}
		if commit && (attachments != 1 || intents != 0) || !commit && (attachments != 0 || intents != 1) {
			t.Fatalf("commit=%v: attachments=%d intents=%d", commit, attachments, intents)
		}
	}
}
