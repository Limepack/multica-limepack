package service

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/channelmedia"
	"github.com/multica-ai/multica/server/internal/integrations/channel"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/dbid"
)

// ErrAutopilotIssueMediaPreparation marks an upstream media failure before
// issue creation. Durable webhook workers can retry the same admitted run.
var ErrAutopilotIssueMediaPreparation = errors.New("autopilot issue media preparation failed")

// ErrWebhookDeliveryLeaseLost is a benign handoff to the delivery's current
// owner. It must not settle the admitted run or its quota reservation.
var ErrWebhookDeliveryLeaseLost = errors.New("webhook delivery lease lost")

type webhookDeliveryLeaseKey struct{}
type webhookDeliveryLease struct{ deliveryID, token pgtype.UUID }

// WithWebhookDeliveryLease carries the worker's current claim into the issue
// transaction, without putting a lease token in an agent-visible payload.
func WithWebhookDeliveryLease(ctx context.Context, deliveryID, token pgtype.UUID) context.Context {
	return context.WithValue(ctx, webhookDeliveryLeaseKey{}, webhookDeliveryLease{deliveryID, token})
}

// AutopilotIssueMedia contains uploaded objects backed by pending media
// intents. Only a trusted server integration may produce these references.
type AutopilotIssueMedia struct {
	Refs                []channel.MediaRef
	UploaderID          pgtype.UUID
	DescriptionAppendix string
}

// FailIssueMediaPreparation settles an admitted run after media retries are
// exhausted. A worker calls this through a transaction-scoped service only
// after fencing the terminal delivery transition with its current lease.
func (s *AutopilotService) FailIssueMediaPreparation(ctx context.Context, runID, workspaceID pgtype.UUID, reason string) error {
	current, err := s.Queries.LockAutopilotRunForDispatch(ctx, db.LockAutopilotRunForDispatchParams{ID: runID, WorkspaceID: workspaceID})
	if err != nil {
		return err
	}
	if isAutopilotRunComplete(current) || current.IssueID.Valid || current.TaskID.Valid {
		return nil
	}
	_, err = s.failAutopilotRun(ctx, db.UpdateAutopilotRunFailedParams{
		ID: runID, FailureReason: pgtype.Text{String: reason, Valid: true},
	})
	return err
}

// bindAutopilotIssueMedia participates in the issue/run transaction. Claimed
// intents and attachment rows commit atomically, so recovery never dispatches
// an issue before its screenshots are attached.
func bindAutopilotIssueMedia(ctx context.Context, qtx *db.Queries, issue *db.Issue, prepared AutopilotIssueMedia) error {
	markdown, err := bindAutopilotMedia(ctx, qtx, *issue, pgtype.UUID{}, prepared)
	if err != nil || markdown == "" {
		return err
	}
	updated, err := qtx.MaterializeIssueChannelMediaMarkdown(ctx, db.MaterializeIssueChannelMediaMarkdownParams{
		ID: issue.ID, WorkspaceID: issue.WorkspaceID, Markdown: pgtype.Text{String: markdown, Valid: true},
	})
	if err == nil {
		*issue = updated
	}
	return err
}

// BindAutopilotCommentMedia binds native Slack reply uploads in the comment's
// lease-fenced transaction and returns the attachment markdown for its text.
func BindAutopilotCommentMedia(ctx context.Context, qtx *db.Queries, issue db.Issue, commentID pgtype.UUID, prepared AutopilotIssueMedia) (string, error) {
	if !commentID.Valid {
		return "", errors.New("media comment is required")
	}
	return bindAutopilotMedia(ctx, qtx, issue, commentID, prepared)
}

func bindAutopilotMedia(ctx context.Context, qtx *db.Queries, issue db.Issue, commentID pgtype.UUID, prepared AutopilotIssueMedia) (string, error) {
	if len(prepared.Refs) == 0 {
		return "", nil
	}
	if !issue.ID.Valid || !issue.WorkspaceID.Valid || !prepared.UploaderID.Valid {
		return "", errors.New("issue, workspace and media uploader are required")
	}
	keys := make([]string, 0, len(prepared.Refs))
	for _, ref := range prepared.Refs {
		if ref.StorageKey == "" || ref.StorageURL == "" {
			return "", errors.New("media storage key and URL are required")
		}
		keys = append(keys, ref.StorageKey)
	}
	claimedKeys, err := qtx.ClaimChannelMediaPendingObjectsForBind(ctx, db.ClaimChannelMediaPendingObjectsForBindParams{
		StorageKeys: keys, WorkspaceID: issue.WorkspaceID,
	})
	if err != nil {
		return "", fmt.Errorf("claim media intents: %w", err)
	}
	claimed := make(map[string]bool, len(claimedKeys))
	for _, key := range claimedKeys {
		claimed[key] = true
	}
	blocks := make([]string, 0, len(prepared.Refs))
	for _, ref := range prepared.Refs {
		if !claimed[ref.StorageKey] {
			// The reconciler owns or already removed the object. Roll back
			// the new issue rather than hand the agent a missing screenshot.
			return "", fmt.Errorf("media intent unavailable for %q", ref.StorageKey)
		}
		delete(claimed, ref.StorageKey)
		id := dbid.NewV7()
		filename := strings.TrimSpace(ref.Filename)
		if filename == "" {
			filename = "attachment-" + util.UUIDToString(id)
		}
		contentType := ref.MimeType
		if contentType == "" {
			contentType = "application/octet-stream"
		}
		att, err := qtx.CreateAttachment(ctx, db.CreateAttachmentParams{
			ID: id, WorkspaceID: issue.WorkspaceID, IssueID: issue.ID, CommentID: commentID,
			UploaderType: "member", UploaderID: prepared.UploaderID,
			Filename: filename, Url: ref.StorageURL, ContentType: contentType,
			SizeBytes: ref.SizeBytes,
		})
		if err != nil {
			return "", fmt.Errorf("create attachment: %w", err)
		}
		blocks = append(blocks, channelmedia.Block(util.UUIDToString(att.ID), filename, ref.Type == channel.MsgTypeImage))
	}
	return strings.Join(blocks, "\n\n"), nil
}
