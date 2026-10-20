-- name: ResetStuckIssueAfterFailedTask :one
-- Recheck the locked target row so recovery cannot overwrite a human's status
-- change after the caller read in_progress.
WITH wakeup_source AS MATERIALIZED (
    SELECT set_config('multica.source_task_id', COALESCE(sqlc.narg('source_task_id')::uuid::text, ''), true)
)
UPDATE issue AS i SET
    status = 'todo',
    duplicate_of_issue_id = NULL,
    position = (
        SELECT COALESCE(MIN(target.position), 0) - 1
        FROM issue AS target
        WHERE target.workspace_id = i.workspace_id AND target.status = 'todo'
    ),
    revision = i.revision + 1,
    last_activity_at = GREATEST(COALESCE(i.last_activity_at, i.updated_at), now()),
    updated_at = now()
FROM wakeup_source
WHERE i.id = @id AND i.workspace_id = @workspace_id
  AND i.status = 'in_progress'
  AND NOT EXISTS (
      SELECT 1 FROM agent_task_queue task
      WHERE task.issue_id = i.id
        AND (
            task.status IN ('queued', 'dispatched', 'running', 'waiting_local_directory')
            OR (task.status = 'deferred' AND (
                task.retry_of_task_id IS NOT NULL
                OR task.context->>'channel_issue_media_pending' = 'true'
            ))
        )
  )
RETURNING i.*;
