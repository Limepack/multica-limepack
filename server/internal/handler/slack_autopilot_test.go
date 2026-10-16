package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/integrations/channel"
	"github.com/multica-ai/multica/server/internal/integrations/channel/engine"
	"github.com/multica-ai/multica/server/internal/integrations/slack"
	"github.com/multica-ai/multica/server/internal/service"
	"github.com/multica-ai/multica/server/internal/testutil"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/dbid"
)

type slackAutopilotFixture struct {
	h       *Handler
	intake  *SlackAutopilotIntake
	route   slack.AutopilotRoute
	install db.ChannelInstallation
	event   slack.AutopilotEvent
}

func newSlackAutopilotFixture(t *testing.T, media engine.MediaResolver) *slackAutopilotFixture {
	t.Helper()
	if testHandler == nil || testPool == nil {
		t.Skip("local test database not available")
	}
	ctx := context.Background()
	agentID := dbfx.Agent(t, "Slack bug investigator "+uuidToString(dbid.NewV7()), handlerTestRuntimeID(t))
	apID := dbfx.Insert(t, "autopilot", testutil.Cols{
		"workspace_id": testWorkspaceID, "title": "Slack bug report", "description": "Reproduce and fix the report.",
		"assignee_type": "agent", "assignee_id": agentID, "status": "active", "execution_mode": "create_issue",
		"created_by_type": "member", "created_by_id": testUserID,
	})
	trigger := createWebhookTriggerViaHandler(t, apID)
	appID := "A" + strings.ReplaceAll(uuidToString(dbid.NewV7()), "-", "")
	config, err := json.Marshal(map[string]string{"app_id": appID, "team_id": "T1", "bot_user_id": "UBOT"})
	if err != nil {
		t.Fatal(err)
	}
	installationID := dbfx.Insert(t, "channel_installation", testutil.Cols{
		"workspace_id": testWorkspaceID, "agent_id": agentID, "channel_type": "slack", "config": config,
		"status": "active", "installer_user_id": testUserID,
	})
	installation, err := testHandler.Queries.GetChannelInstallationInWorkspace(ctx, db.GetChannelInstallationInWorkspaceParams{
		ID: parseUUID(installationID), WorkspaceID: parseUUID(testWorkspaceID), ChannelType: "slack",
	})
	if err != nil {
		t.Fatal(err)
	}
	// A local service/worker pair keeps the media callback scoped to this test.
	h := *testHandler
	svc := *testHandler.AutopilotService
	h.AutopilotService = &svc
	h.WebhookDeliveryWorker = NewWebhookDeliveryWorker(&h)
	route := slack.AutopilotRoute{WorkspaceID: testWorkspaceID, AgentID: agentID, TeamID: "T1", ChannelID: "C123", AutopilotID: apID, TriggerID: trigger.ID}
	intake := NewSlackAutopilotIntake(&h, []slack.AutopilotRoute{route}, media, nil)
	intake.permalink = func(_ context.Context, _ db.ChannelInstallation, event slack.AutopilotEvent) (string, error) {
		return "https://limepack.slack.com/archives/" + event.ChannelID + "/p" + strings.ReplaceAll(event.MessageTS, ".", ""), nil
	}
	h.AutopilotService.PrepareIssueMedia = intake.PrepareIssueMedia
	t.Cleanup(func() {
		for _, query := range []string{
			`DELETE FROM task_supplement WHERE issue_id IN (SELECT issue_id FROM autopilot_run WHERE autopilot_id=$1)`,
			`DELETE FROM task_supplement_capability WHERE task_id IN (SELECT id FROM agent_task_queue WHERE issue_id IN (SELECT issue_id FROM autopilot_run WHERE autopilot_id=$1))`,
			`DELETE FROM agent_task_queue WHERE issue_id IN (SELECT issue_id FROM autopilot_run WHERE autopilot_id=$1)`,
			`DELETE FROM attachment WHERE issue_id IN (SELECT issue_id FROM autopilot_run WHERE autopilot_id=$1)`,
			`DELETE FROM comment WHERE issue_id IN (SELECT issue_id FROM autopilot_run WHERE autopilot_id=$1)`,
			`DELETE FROM issue WHERE id IN (SELECT issue_id FROM autopilot_run WHERE autopilot_id=$1)`,
			`DELETE FROM autopilot_run WHERE autopilot_id=$1`,
			`DELETE FROM webhook_delivery WHERE autopilot_id=$1`,
			`DELETE FROM autopilot_rule_version WHERE autopilot_id=$1`,
			`DELETE FROM autopilot_trigger WHERE autopilot_id=$1`,
		} {
			_, _ = testPool.Exec(context.Background(), query, apID)
		}
	})
	return &slackAutopilotFixture{h: &h, intake: intake, route: route, install: installation,
		event: slack.AutopilotEvent{AppID: appID, TeamID: "T1", ChannelID: "C123", UserID: "UNBOUNDREPORTER", MessageTS: "1700000000.000100", Text: "Checkout shows an error after choosing a cup."},
	}
}

func (f *slackAutopilotFixture) ingest(t *testing.T, event slack.AutopilotEvent) db.WebhookDelivery {
	t.Helper()
	handled, err := f.intake.HandleAutopilotEvent(context.Background(), event)
	if err != nil || !handled {
		t.Fatalf("native report handled=%v, err=%v", handled, err)
	}
	delivery, err := f.h.Queries.GetWebhookDeliveryByTriggerAndDedupe(context.Background(), db.GetWebhookDeliveryByTriggerAndDedupeParams{
		TriggerID: parseUUID(f.route.TriggerID), DedupeKey: pgtype.Text{String: "slack:" + event.TeamID + ":" + event.ChannelID + ":" + event.MessageTS, Valid: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	return delivery
}

func (f *slackAutopilotFixture) processOne(t *testing.T) {
	t.Helper()
	worked, err := f.h.WebhookDeliveryWorker.ProcessNext(context.Background())
	if err != nil || !worked {
		t.Fatalf("worker processed=%v, err=%v", worked, err)
	}
}

func (f *slackAutopilotFixture) delivery(t *testing.T, id pgtype.UUID) db.WebhookDelivery {
	t.Helper()
	delivery, err := f.h.Queries.GetWebhookDelivery(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return delivery
}

func (f *slackAutopilotFixture) run(t *testing.T, deliveryID pgtype.UUID) db.AutopilotRun {
	t.Helper()
	run, err := f.h.Queries.GetAutopilotRunByWebhookDelivery(context.Background(), deliveryID)
	if err != nil {
		t.Fatal(err)
	}
	return run
}

func TestSlackAutopilotNativeDedupeAndDistinctReports(t *testing.T) {
	f := newSlackAutopilotFixture(t, nil)
	first := f.ingest(t, f.event)
	duplicate := f.ingest(t, f.event)
	if first.ID != duplicate.ID || first.Status != deliveryStatusQueued {
		t.Fatalf("duplicate delivery IDs/status: first=%s/%s duplicate=%s", uuidToString(first.ID), first.Status, uuidToString(duplicate.ID))
	}
	if n := dbfx.Count(t, `SELECT count(*) FROM webhook_delivery WHERE autopilot_id=$1`, f.route.AutopilotID); n != 1 {
		t.Fatalf("duplicate report produced %d delivery rows", n)
	}
	if n := dbfx.Count(t, `SELECT count(*) FROM channel_user_binding WHERE installation_id=$1 AND channel_user_id=$2`, uuidToString(f.install.ID), f.event.UserID); n != 0 {
		t.Fatal("reporter unexpectedly has a Multica account binding")
	}
	secondEvent := f.event
	secondEvent.MessageTS = "1700000000.000101"
	secondEvent.Text = "A second bug report arrives immediately."
	second := f.ingest(t, secondEvent)
	f.processOne(t)
	f.processOne(t)
	firstRun, secondRun := f.run(t, first.ID), f.run(t, second.ID)
	if !firstRun.IssueID.Valid || !secondRun.IssueID.Valid || firstRun.IssueID == secondRun.IssueID || firstRun.ID == secondRun.ID {
		t.Fatalf("distinct reports must create distinct runs/issues: first=%+v second=%+v", firstRun, secondRun)
	}
	for _, id := range []pgtype.UUID{first.ID, second.ID} {
		if delivery := f.delivery(t, id); delivery.Status != deliveryStatusDispatched {
			t.Fatalf("delivery %s status=%s error=%s", uuidToString(id), delivery.Status, delivery.Error.String)
		}
	}
	issue, err := f.h.Queries.GetIssue(context.Background(), firstRun.IssueID)
	if err != nil || !strings.Contains(issue.Description.String, f.event.Text) {
		t.Fatalf("issue lacks original report: err=%v description=%q", err, issue.Description.String)
	}
}

func TestSlackAutopilotRouteScopeAndInstallationIdentity(t *testing.T) {
	for _, tc := range []struct {
		name        string
		edit        func(*testing.T, *slackAutopilotFixture)
		wantHandled bool
		wantError   bool
	}{
		{"wrong app", func(_ *testing.T, f *slackAutopilotFixture) { f.event.AppID = "ANOTINSTALLED" }, false, false},
		{"wrong team", func(_ *testing.T, f *slackAutopilotFixture) { f.event.TeamID = "TOTHER" }, false, false},
		{"wrong channel", func(_ *testing.T, f *slackAutopilotFixture) { f.event.ChannelID = "COTHER" }, false, false},
		{"wrong workspace", func(_ *testing.T, f *slackAutopilotFixture) {
			f.intake.routes[0].WorkspaceID = uuidToString(dbid.NewV7())
		}, false, false},
		{"wrong agent", func(_ *testing.T, f *slackAutopilotFixture) { f.intake.routes[0].AgentID = uuidToString(dbid.NewV7()) }, false, false},
		{"own bot", func(_ *testing.T, f *slackAutopilotFixture) { f.event.UserID = "UBOT" }, true, true},
		{"revoked installation", func(t *testing.T, f *slackAutopilotFixture) {
			dbfx.Exec(t, `UPDATE channel_installation SET status='revoked' WHERE id=$1`, uuidToString(f.install.ID))
		}, true, false},
		{"stored team mismatch", func(t *testing.T, f *slackAutopilotFixture) {
			dbfx.Exec(t, `UPDATE channel_installation SET config=jsonb_set(config,'{team_id}','"TOTHER"') WHERE id=$1`, uuidToString(f.install.ID))
		}, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newSlackAutopilotFixture(t, nil)
			tc.edit(t, f)
			handled, err := f.intake.HandleAutopilotEvent(context.Background(), f.event)
			if handled != tc.wantHandled || (err != nil) != tc.wantError {
				t.Fatalf("handled=%v err=%v; want handled=%v error=%v", handled, err, tc.wantHandled, tc.wantError)
			}
			if n := dbfx.Count(t, `SELECT count(*) FROM webhook_delivery WHERE autopilot_id=$1`, f.route.AutopilotID); n != 0 {
				t.Fatalf("out-of-scope report queued %d deliveries", n)
			}
		})
	}
}

func TestSlackAutopilotRejectsTriggerOwnedByAnotherAutopilot(t *testing.T) {
	f := newSlackAutopilotFixture(t, nil)
	other := newSlackAutopilotFixture(t, nil)
	f.intake.routes[0].TriggerID = other.route.TriggerID
	handled, err := f.intake.HandleAutopilotEvent(context.Background(), f.event)
	if !handled || err == nil {
		t.Fatalf("wrong trigger owner handled=%v err=%v", handled, err)
	}
	if n := dbfx.Count(t, `SELECT count(*) FROM webhook_delivery WHERE autopilot_id=$1`, f.route.AutopilotID); n != 0 {
		t.Fatalf("ownership mismatch queued %d deliveries", n)
	}
}

type slackAutopilotMediaResolver struct {
	resolve func(context.Context, engine.ResolvedInstallation, engine.ResolvedIdentity, pgtype.UUID, pgtype.UUID, channel.InboundMessage) channel.InboundMessage
	calls   int
}

func (r *slackAutopilotMediaResolver) HasMedia(channel.InboundMessage) bool { return true }
func (r *slackAutopilotMediaResolver) ResolveMedia(ctx context.Context, inst engine.ResolvedInstallation, sender engine.ResolvedIdentity, sessionID, messageID pgtype.UUID, msg channel.InboundMessage) channel.InboundMessage {
	r.calls++
	return r.resolve(ctx, inst, sender, sessionID, messageID, msg)
}

func slackAutopilotScreenshot() slack.AutopilotFile {
	return slack.AutopilotFile{ID: "F1", Name: "checkout.png", Mimetype: "image/png", Size: 123, DownloadURL: "https://files.slack.com/checkout.png"}
}

func TestSlackAutopilotMediaRequiresNativeProvenanceAndInstalledContext(t *testing.T) {
	resolver := &slackAutopilotMediaResolver{}
	f := newSlackAutopilotFixture(t, resolver)
	f.event.Files = []slack.AutopilotFile{slackAutopilotScreenshot()}
	delivery := f.ingest(t, f.event)
	wantRefs := []channel.MediaRef{{Type: channel.MsgTypeImage, Filename: "checkout.png", StorageKey: "test/checkout.png", StorageURL: "https://storage.example.test/checkout.png", MimeType: "image/png", SizeBytes: 123}}
	resolver.resolve = func(_ context.Context, inst engine.ResolvedInstallation, sender engine.ResolvedIdentity, sessionID, messageID pgtype.UUID, msg channel.InboundMessage) channel.InboundMessage {
		if inst.ID != f.install.ID || inst.WorkspaceID != f.install.WorkspaceID || inst.AgentID != f.install.AgentID || !inst.Active || sender.UserID != parseUUID(testUserID) || sessionID.Valid || messageID != delivery.ID {
			t.Fatalf("incorrect native media context: installation=%+v sender=%+v session=%+v message=%+v", inst, sender, sessionID, messageID)
		}
		platform, ok := inst.Platform.(db.ChannelInstallation)
		if !ok || platform.ID != f.install.ID || msg.Source.ChatID != f.event.ChannelID || msg.MessageID != f.event.MessageTS || msg.Text != f.event.Text {
			t.Fatalf("incorrect media source/platform: %+v / %+v", msg, inst.Platform)
		}
		msg.MediaRefs = wantRefs
		return msg
	}
	if resolver.calls != 0 || dbfx.Count(t, `SELECT count(*) FROM autopilot_run WHERE autopilot_id=$1`, f.route.AutopilotID) != 0 {
		t.Fatal("intake performed media resolution or dispatch before the worker")
	}
	ap, err := f.h.Queries.GetAutopilot(context.Background(), parseUUID(f.route.AutopilotID))
	if err != nil {
		t.Fatal(err)
	}
	run := db.AutopilotRun{TriggerID: parseUUID(f.route.TriggerID), WebhookDeliveryID: delivery.ID}
	prepared, err := f.intake.PrepareIssueMedia(context.Background(), ap, run)
	if err != nil || prepared.UploaderID != parseUUID(testUserID) || !reflect.DeepEqual(prepared.Refs, wantRefs) || resolver.calls != 1 {
		t.Fatalf("prepared=%+v calls=%d err=%v", prepared, resolver.calls, err)
	}
	if !strings.Contains(prepared.DescriptionAppendix, "https://limepack.slack.com/archives/C123/p1700000000000100") {
		t.Fatalf("missing canonical Slack source link: %q", prepared.DescriptionAppendix)
	}
	// HTTP callers cannot persist the marker through the selected-header allowlist.
	headers := http.Header{}
	headers.Set(slackAutopilotProvenanceKey, string(delivery.SelectedHeaders))
	if strings.Contains(string(selectedHeadersJSON(headers)), slackAutopilotProvenanceKey) {
		t.Fatal("HTTP headers can forge native Slack provenance")
	}
	forged, err := json.Marshal(map[string]any{slackAutopilotProvenanceKey: slackAutopilotProvenance{
		InstallationID: uuidToString(f.install.ID), AppID: f.event.AppID, TeamID: f.event.TeamID, ChannelID: "COTHER", MessageTS: f.event.MessageTS,
	}})
	if err != nil {
		t.Fatal(err)
	}
	dbfx.Exec(t, `UPDATE webhook_delivery SET selected_headers=$2 WHERE id=$1`, uuidToString(delivery.ID), forged)
	if _, err := f.intake.PrepareIssueMedia(context.Background(), ap, run); err == nil || resolver.calls != 1 {
		t.Fatalf("mismatched provenance used Slack credentials: calls=%d err=%v", resolver.calls, err)
	}
	dbfx.Exec(t, `UPDATE webhook_delivery SET selected_headers='{}' WHERE id=$1`, uuidToString(delivery.ID))
	prepared, err = f.intake.PrepareIssueMedia(context.Background(), ap, run)
	if err != nil || len(prepared.Refs) != 0 || resolver.calls != 1 {
		t.Fatalf("unmarked webhook used Slack media credentials: prepared=%+v calls=%d err=%v", prepared, resolver.calls, err)
	}
}

func TestSlackAutopilotPermanentAttachmentWarningCreatesIssue(t *testing.T) {
	resolver := &slackAutopilotMediaResolver{resolve: func(_ context.Context, _ engine.ResolvedInstallation, _ engine.ResolvedIdentity, _, _ pgtype.UUID, msg channel.InboundMessage) channel.InboundMessage {
		t.Fatal("permanently oversized attachment reached media download")
		return msg
	}}
	f := newSlackAutopilotFixture(t, resolver)
	file := slackAutopilotScreenshot()
	file.Size = 21 << 20
	f.event.Files = []slack.AutopilotFile{file}
	f.event.AttachmentWarnings = []string{"An external attachment was not imported."}
	delivery := f.ingest(t, f.event)
	f.processOne(t)
	run := f.run(t, delivery.ID)
	if !run.IssueID.Valid || f.delivery(t, delivery.ID).Status != deliveryStatusDispatched || resolver.calls != 0 {
		t.Fatalf("permanent omission failed report: run=%+v calls=%d", run, resolver.calls)
	}
	issue, err := f.h.Queries.GetIssue(context.Background(), run.IssueID)
	if err != nil || !strings.Contains(issue.Description.String, "20 MiB limit") || !strings.Contains(issue.Description.String, "external attachment") || !strings.Contains(issue.Description.String, "https://limepack.slack.com/archives/C123/p1700000000000100") {
		t.Fatalf("issue lacks attachment warning or source link: err=%v description=%q", err, issue.Description.String)
	}
}

func TestSlackAutopilotMediaRetryPreservesRunAndAttachesBeforeDispatch(t *testing.T) {
	resolver := &slackAutopilotMediaResolver{}
	f := newSlackAutopilotFixture(t, resolver)
	f.event.Files = []slack.AutopilotFile{slackAutopilotScreenshot()}
	delivery := f.ingest(t, f.event)
	key := "test/slack-autopilot/" + uuidToString(delivery.ID) + "/F1.png"
	dbfx.Cleanup(t, `DELETE FROM channel_media_pending_object WHERE storage_key=$1`, key)
	resolver.resolve = func(ctx context.Context, inst engine.ResolvedInstallation, _ engine.ResolvedIdentity, _, messageID pgtype.UUID, msg channel.InboundMessage) channel.InboundMessage {
		if resolver.calls == 1 {
			return msg
		}
		url := "https://storage.example.test/" + key
		if _, err := f.h.Queries.RecordChannelMediaPendingObject(ctx, db.RecordChannelMediaPendingObjectParams{StorageKey: key, WorkspaceID: inst.WorkspaceID, InstallationID: inst.ID, ChatMessageID: messageID, StorageUrl: url}); err != nil {
			t.Fatal(err)
		}
		msg.MediaRefs = []channel.MediaRef{{Type: channel.MsgTypeImage, Filename: "checkout.png", StorageKey: key, StorageURL: url, MimeType: "image/png", SizeBytes: 123}}
		return msg
	}
	f.processOne(t)
	firstRun := f.run(t, delivery.ID)
	firstDelivery := f.delivery(t, delivery.ID)
	if firstDelivery.Status != deliveryStatusQueued || firstRun.Status == "failed" || firstRun.IssueID.Valid {
		t.Fatalf("media failure must retain admitted run without issue: delivery=%+v run=%+v", firstDelivery, firstRun)
	}
	dbfx.Exec(t, `UPDATE webhook_delivery SET available_at=now() WHERE id=$1`, uuidToString(delivery.ID))
	f.processOne(t)
	finalRun := f.run(t, delivery.ID)
	if finalRun.ID != firstRun.ID || finalRun.QuotaReservationID != firstRun.QuotaReservationID || !finalRun.IssueID.Valid || f.delivery(t, delivery.ID).Status != deliveryStatusDispatched || resolver.calls != 2 {
		t.Fatalf("retry did not recover original run: first=%+v final=%+v calls=%d", firstRun, finalRun, resolver.calls)
	}
	if n := dbfx.Count(t, `SELECT count(*) FROM attachment WHERE issue_id=$1`, uuidToString(finalRun.IssueID)); n != 1 {
		t.Fatalf("resolved screenshot count=%d", n)
	}
	if n := dbfx.Count(t, `SELECT count(*) FROM channel_media_pending_object WHERE storage_key=$1`, key); n != 0 {
		t.Fatal("success left a pending screenshot intent")
	}
	stale := firstDelivery
	stale.DispatchAttempts = webhookWorkerMaxAttempts - 1
	stale.LeaseToken = dbid.NewV7()
	if err := f.h.WebhookDeliveryWorker.retryIssueMedia(context.Background(), stale, firstRun, errors.New("stale screenshot failure")); err != nil {
		t.Fatal(err)
	}
	if got := f.run(t, delivery.ID); got.IssueID != finalRun.IssueID || got.Status != finalRun.Status || f.delivery(t, delivery.ID).Status != deliveryStatusDispatched {
		t.Fatal("stale worker changed a successful report into a failure")
	}
}

func TestSlackAutopilotMediaExhaustionSettlesRunAndDelivery(t *testing.T) {
	resolver := &slackAutopilotMediaResolver{resolve: func(_ context.Context, _ engine.ResolvedInstallation, _ engine.ResolvedIdentity, _, _ pgtype.UUID, msg channel.InboundMessage) channel.InboundMessage {
		return msg
	}}
	f := newSlackAutopilotFixture(t, resolver)
	f.event.Files = []slack.AutopilotFile{slackAutopilotScreenshot()}
	delivery := f.ingest(t, f.event)
	// Authorization failures must still fail closed on the final attempt.
	dbfx.Exec(t, `UPDATE channel_installation SET status='revoked' WHERE id=$1`, uuidToString(f.install.ID))
	dbfx.Exec(t, `UPDATE webhook_delivery SET dispatch_attempts=$2 WHERE id=$1`, uuidToString(delivery.ID), webhookWorkerMaxAttempts-1)
	f.processOne(t)
	run := f.run(t, delivery.ID)
	final := f.delivery(t, delivery.ID)
	if final.Status != deliveryStatusFailed || final.ReasonCode.String != "attachment_preparation_failed" || run.Status != "failed" || run.IssueID.Valid || final.AutopilotRunID != run.ID || resolver.calls != 0 {
		t.Fatalf("exhausted media retry not settled: delivery=%+v run=%+v", final, run)
	}
	if !strings.Contains(run.FailureReason.String, service.ErrAutopilotIssueMediaPreparation.Error()) {
		t.Fatalf("run lacks classified media failure: %q", run.FailureReason.String)
	}
	// A stale lease must not fail the same run a second time or change a completed delivery.
	stale := delivery
	stale.DispatchAttempts = webhookWorkerMaxAttempts - 1
	stale.LeaseToken = dbid.NewV7()
	if err := f.h.WebhookDeliveryWorker.retryIssueMedia(context.Background(), stale, run, errors.New("stale worker")); err != nil {
		t.Fatal(err)
	}
	if got := f.run(t, delivery.ID); got.FailureReason != run.FailureReason {
		t.Fatal("stale worker replaced the settled run failure")
	}
}

func TestSlackAutopilotFinalAttachmentAttemptPreservesReport(t *testing.T) {
	resolver := &slackAutopilotMediaResolver{resolve: func(_ context.Context, _ engine.ResolvedInstallation, _ engine.ResolvedIdentity, _, _ pgtype.UUID, msg channel.InboundMessage) channel.InboundMessage {
		return msg
	}}
	f := newSlackAutopilotFixture(t, resolver)
	f.event.Files = []slack.AutopilotFile{slackAutopilotScreenshot()}
	delivery := f.ingest(t, f.event)
	dbfx.Exec(t, `UPDATE webhook_delivery SET dispatch_attempts=$2 WHERE id=$1`, uuidToString(delivery.ID), webhookWorkerMaxAttempts-1)
	f.processOne(t)
	run := f.run(t, delivery.ID)
	if !run.IssueID.Valid || run.Status == "failed" || f.delivery(t, delivery.ID).Status != deliveryStatusDispatched || resolver.calls != 1 {
		t.Fatalf("final screenshot failure lost the report: run=%+v calls=%d", run, resolver.calls)
	}
	issue, err := f.h.Queries.GetIssue(context.Background(), run.IssueID)
	if err != nil || !strings.Contains(issue.Description.String, f.event.Text) || !strings.Contains(issue.Description.String, "https://limepack.slack.com/archives/C123/p1700000000000100") || !strings.Contains(issue.Description.String, "0 of 1 eligible files were available after five attempts") {
		t.Fatalf("preserved report lacks text/source/explicit screenshot omission: err=%v description=%q", err, issue.Description.String)
	}
	if n := dbfx.Count(t, `SELECT count(*) FROM attachment WHERE issue_id=$1`, uuidToString(run.IssueID)); n != 0 {
		t.Fatalf("unavailable screenshot created %d attachments", n)
	}
}
