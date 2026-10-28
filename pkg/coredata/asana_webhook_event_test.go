// Copyright (c) 2026 Probo Inc <hello@probo.com>.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package coredata_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.gearno.de/kit/pg"
	"go.probo.inc/probo/internal/test"
	"go.probo.inc/probo/pkg/coredata"
	"go.probo.inc/probo/pkg/gid"
)

var asanaWebhookClaimMu sync.Mutex

func lockAsanaWebhookClaim(t *testing.T) {
	t.Helper()

	asanaWebhookClaimMu.Lock()
	t.Cleanup(asanaWebhookClaimMu.Unlock)
}

func insertAsanaWebhookEventFixture(
	t *testing.T,
	pgClient *pg.Client,
	now time.Time,
	ownerToken *string,
	startedAt *time.Time,
	attemptCount int,
) *coredata.AsanaWebhookEvent {
	t.Helper()

	event := coredata.NewAsanaWebhookEvent(
		"asana-"+gid.New(gid.NewTenantID(), coredata.TaskSyncJobEntityType).String(),
		[]byte(`{}`),
	)
	event.CreatedAt = now
	event.UpdatedAt = now
	event.ProcessingOwnerToken = ownerToken
	event.ProcessingStartedAt = startedAt
	event.AttemptCount = attemptCount

	insertAsanaWebhookEvent(t, pgClient, event)

	return event
}

func insertAsanaWebhookEvent(
	t *testing.T,
	pgClient *pg.Client,
	event *coredata.AsanaWebhookEvent,
) {
	t.Helper()

	require.NoError(
		t,
		pgClient.WithConn(
			t.Context(),
			func(ctx context.Context, conn pg.Querier) error {
				_, err := event.Insert(ctx, conn)

				return err
			},
		),
	)
	t.Cleanup(
		func() {
			_ = pgClient.WithConn(
				context.Background(),
				func(ctx context.Context, conn pg.Querier) error {
					_, err := conn.Exec(
						ctx,
						`DELETE FROM asana_webhook_events WHERE delivery_id = @delivery_id`,
						pgx.StrictNamedArgs{"delivery_id": event.DeliveryID},
					)

					return err
				},
			)
		},
	)
}

func loadAsanaWebhookEvent(
	t *testing.T,
	pgClient *pg.Client,
	deliveryID string,
) *coredata.AsanaWebhookEvent {
	t.Helper()

	var event coredata.AsanaWebhookEvent

	require.NoError(
		t,
		pgClient.WithConn(
			t.Context(),
			func(ctx context.Context, conn pg.Querier) error {
				return event.LoadByDeliveryID(ctx, conn, deliveryID)
			},
		),
	)

	return &event
}

func TestAsanaWebhookEvent_ClaimNextForUpdateSkipLocked_OrdersByCreatedAt(t *testing.T) {
	t.Parallel()
	lockAsanaWebhookClaim(t)

	pgClient := test.PGClient(t)
	now := time.Now().UTC()
	base := time.Date(1990, 1, 1, 0, 0, 0, 0, time.UTC)
	newer := insertAsanaWebhookEventFixture(t, pgClient, base.Add(time.Second), nil, nil, 0)
	older := insertAsanaWebhookEventFixture(t, pgClient, base, nil, nil, 0)

	var first coredata.AsanaWebhookEvent

	require.NoError(
		t,
		pgClient.WithTx(
			t.Context(),
			func(ctx context.Context, tx pg.Tx) error {
				return first.ClaimNextForUpdateSkipLocked(ctx, tx, now)
			},
		),
	)
	assert.Equal(t, older.DeliveryID, first.DeliveryID)
	assert.Equal(t, 1, first.AttemptCount)
	require.NotNil(t, first.ProcessingOwnerToken)
	require.NotNil(t, first.ProcessingStartedAt)

	var second coredata.AsanaWebhookEvent

	require.NoError(
		t,
		pgClient.WithTx(
			t.Context(),
			func(ctx context.Context, tx pg.Tx) error {
				return second.ClaimNextForUpdateSkipLocked(ctx, tx, now)
			},
		),
	)
	assert.Equal(t, newer.DeliveryID, second.DeliveryID)
	assert.Equal(t, 1, second.AttemptCount)
}

func TestAsanaWebhookEvent_ClaimNextForUpdateSkipLocked_SkipsLockedRow(t *testing.T) {
	t.Parallel()
	lockAsanaWebhookClaim(t)

	pgClient := test.PGClient(t)
	now := time.Now().UTC()
	base := time.Date(1990, 1, 2, 0, 0, 0, 0, time.UTC)
	older := insertAsanaWebhookEventFixture(t, pgClient, base, nil, nil, 0)
	newer := insertAsanaWebhookEventFixture(t, pgClient, base.Add(time.Second), nil, nil, 0)

	release := make(chan struct{})
	locked := make(chan struct{})
	done := make(chan error, 1)

	var releaseOnce sync.Once

	releaseLock := func() {
		releaseOnce.Do(
			func() {
				close(release)
			},
		)
	}
	defer releaseLock()

	go func() {
		done <- pgClient.WithTx(
			context.Background(),
			func(ctx context.Context, tx pg.Tx) error {
				_, err := tx.Exec(
					ctx,
					`SELECT delivery_id
					FROM asana_webhook_events
					WHERE delivery_id = @delivery_id
					FOR UPDATE`,
					pgx.StrictNamedArgs{"delivery_id": older.DeliveryID},
				)
				if err != nil {
					return err
				}

				close(locked)
				<-release

				return nil
			},
		)
	}()

	select {
	case <-locked:
	case err := <-done:
		require.NoError(t, err)
		require.FailNow(t, "row lock transaction ended early")
	case <-time.After(5 * time.Second):
		require.FailNow(t, "timed out waiting for row lock")
	}

	claimCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	var claimed coredata.AsanaWebhookEvent

	require.NoError(
		t,
		pgClient.WithTx(
			claimCtx,
			func(ctx context.Context, tx pg.Tx) error {
				return claimed.ClaimNextForUpdateSkipLocked(ctx, tx, now)
			},
		),
	)
	assert.Equal(t, newer.DeliveryID, claimed.DeliveryID)

	releaseLock()
	require.NoError(t, <-done)

	var next coredata.AsanaWebhookEvent

	require.NoError(
		t,
		pgClient.WithTx(
			t.Context(),
			func(ctx context.Context, tx pg.Tx) error {
				return next.ClaimNextForUpdateSkipLocked(ctx, tx, now)
			},
		),
	)
	assert.Equal(t, older.DeliveryID, next.DeliveryID)
}

func TestAsanaWebhookEvent_ClaimNextForUpdateSkipLocked_SkipsIneligibleRows(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	tests := []struct {
		name  string
		base  time.Time
		setup func(*coredata.AsanaWebhookEvent)
	}{
		{
			name: "dead-lettered row",
			base: time.Date(1990, 1, 4, 0, 0, 0, 0, time.UTC),
			setup: func(event *coredata.AsanaWebhookEvent) {
				deadAt := event.CreatedAt
				event.DeadLetteredAt = &deadAt
			},
		},
		{
			name: "row at max attempts",
			base: time.Date(1990, 1, 5, 0, 0, 0, 0, time.UTC),
			setup: func(event *coredata.AsanaWebhookEvent) {
				event.AttemptCount = coredata.AsanaWebhookEventDefaultMaxAttempts
			},
		},
		{
			name: "row with a future next attempt",
			base: time.Date(1990, 1, 6, 0, 0, 0, 0, time.UTC),
			setup: func(event *coredata.AsanaWebhookEvent) {
				nextAttemptAt := now.Add(time.Hour)
				event.NextAttemptAt = &nextAttemptAt
			},
		},
	}

	for _, tt := range tests {
		t.Run(
			tt.name,
			func(t *testing.T) {
				t.Parallel()
				lockAsanaWebhookClaim(t)

				pgClient := test.PGClient(t)
				skipped := coredata.NewAsanaWebhookEvent(
					"asana-"+gid.New(gid.NewTenantID(), coredata.TaskSyncJobEntityType).String(),
					[]byte(`{}`),
				)
				skipped.CreatedAt = tt.base
				skipped.UpdatedAt = tt.base
				tt.setup(skipped)
				insertAsanaWebhookEvent(t, pgClient, skipped)

				eligible := insertAsanaWebhookEventFixture(
					t,
					pgClient,
					tt.base.Add(time.Second),
					nil,
					nil,
					0,
				)

				var claimed coredata.AsanaWebhookEvent

				require.NoError(
					t,
					pgClient.WithTx(
						t.Context(),
						func(ctx context.Context, tx pg.Tx) error {
							return claimed.ClaimNextForUpdateSkipLocked(ctx, tx, now)
						},
					),
				)
				assert.Equal(t, eligible.DeliveryID, claimed.DeliveryID)
				assert.Equal(t, 1, claimed.AttemptCount)

				persisted := loadAsanaWebhookEvent(t, pgClient, skipped.DeliveryID)
				assert.Nil(t, persisted.ProcessingOwnerToken)
				assert.Nil(t, persisted.ProcessingStartedAt)
				assert.Equal(t, skipped.AttemptCount, persisted.AttemptCount)
			},
		)
	}
}

func TestAsanaWebhookEvent_UpdateProcessingState_WrongToken(t *testing.T) {
	t.Parallel()

	pgClient := test.PGClient(t)
	now := time.Now().UTC()
	startedAt := now.Add(-time.Minute)
	event := insertAsanaWebhookEventFixture(
		t,
		pgClient,
		now,
		new("owner-a"),
		&startedAt,
		1,
	)

	event.ProcessingOwnerToken = new("owner-b")
	processedAt := now
	event.ProcessedAt = &processedAt
	event.ProcessingStartedAt = nil
	event.UpdatedAt = now

	err := pgClient.WithConn(
		t.Context(),
		func(ctx context.Context, conn pg.Querier) error {
			return event.UpdateProcessingState(ctx, conn)
		},
	)
	require.ErrorIs(t, err, coredata.ErrProcessingLeaseLost)

	persisted := loadAsanaWebhookEvent(t, pgClient, event.DeliveryID)
	require.NotNil(t, persisted.ProcessingOwnerToken)
	assert.Equal(t, "owner-a", *persisted.ProcessingOwnerToken)
	assert.Nil(t, persisted.ProcessedAt)
	require.NotNil(t, persisted.ProcessingStartedAt)
	assert.WithinDuration(t, startedAt, *persisted.ProcessingStartedAt, time.Microsecond)
}

func TestAsanaWebhookEvent_UpdateProcessingState_ClearsOwnerToken(t *testing.T) {
	t.Parallel()

	pgClient := test.PGClient(t)
	now := time.Now().UTC()
	startedAt := now.Add(-time.Minute)
	event := insertAsanaWebhookEventFixture(
		t,
		pgClient,
		now,
		new("owner"),
		&startedAt,
		1,
	)

	processedAt := now
	event.ProcessingStartedAt = nil
	event.ProcessedAt = &processedAt
	event.UpdatedAt = now

	require.NoError(
		t,
		pgClient.WithConn(
			t.Context(),
			func(ctx context.Context, conn pg.Querier) error {
				return event.UpdateProcessingState(ctx, conn)
			},
		),
	)

	persisted := loadAsanaWebhookEvent(t, pgClient, event.DeliveryID)
	assert.Nil(t, persisted.ProcessingOwnerToken)
	assert.Nil(t, persisted.ProcessingStartedAt)
	require.NotNil(t, persisted.ProcessedAt)
	assert.WithinDuration(t, processedAt, *persisted.ProcessedAt, time.Microsecond)
	assert.Equal(t, 1, persisted.AttemptCount)
}

func TestAsanaWebhookEvent_TouchLease_RenewsStartedAt(t *testing.T) {
	t.Parallel()

	pgClient := test.PGClient(t)
	now := time.Now().UTC()
	startedAt := now.Add(-time.Minute)
	event := insertAsanaWebhookEventFixture(
		t,
		pgClient,
		now,
		new("owner"),
		&startedAt,
		1,
	)

	touchedAt := now.Add(time.Minute)

	require.NoError(
		t,
		pgClient.WithConn(
			t.Context(),
			func(ctx context.Context, conn pg.Querier) error {
				return event.TouchLease(ctx, conn, touchedAt)
			},
		),
	)

	persisted := loadAsanaWebhookEvent(t, pgClient, event.DeliveryID)
	require.NotNil(t, persisted.ProcessingOwnerToken)
	assert.Equal(t, "owner", *persisted.ProcessingOwnerToken)
	require.NotNil(t, persisted.ProcessingStartedAt)
	assert.WithinDuration(t, touchedAt, *persisted.ProcessingStartedAt, time.Second)
}

func TestAsanaWebhookEvent_TouchLease_WrongToken(t *testing.T) {
	t.Parallel()

	pgClient := test.PGClient(t)
	now := time.Now().UTC()
	startedAt := now.Add(-time.Minute)
	event := insertAsanaWebhookEventFixture(
		t,
		pgClient,
		now,
		new("owner-a"),
		&startedAt,
		1,
	)

	event.ProcessingOwnerToken = new("owner-b")

	err := pgClient.WithConn(
		t.Context(),
		func(ctx context.Context, conn pg.Querier) error {
			return event.TouchLease(ctx, conn, now)
		},
	)
	require.ErrorIs(t, err, coredata.ErrProcessingLeaseLost)

	persisted := loadAsanaWebhookEvent(t, pgClient, event.DeliveryID)
	require.NotNil(t, persisted.ProcessingOwnerToken)
	assert.Equal(t, "owner-a", *persisted.ProcessingOwnerToken)
	require.NotNil(t, persisted.ProcessingStartedAt)
	assert.WithinDuration(t, startedAt, *persisted.ProcessingStartedAt, time.Microsecond)
}

func TestResetStaleProcessingAsanaWebhookEvents_RequeuesOrDeadLetters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		attemptCount int
		wantDead     bool
	}{
		{
			name:         "requeues while attempts remain",
			attemptCount: 1,
		},
		{
			name:         "dead letters after attempts are exhausted",
			attemptCount: coredata.AsanaWebhookEventDefaultMaxAttempts,
			wantDead:     true,
		},
	}

	for _, tt := range tests {
		t.Run(
			tt.name,
			func(t *testing.T) {
				t.Parallel()

				pgClient := test.PGClient(t)
				now := time.Now().UTC()
				startedAt := now.Add(-time.Hour)
				event := insertAsanaWebhookEventFixture(
					t,
					pgClient,
					now,
					new("owner"),
					&startedAt,
					tt.attemptCount,
				)

				require.NoError(
					t,
					pgClient.WithConn(
						t.Context(),
						func(ctx context.Context, conn pg.Querier) error {
							return coredata.ResetStaleProcessingAsanaWebhookEvents(
								ctx,
								conn,
								now,
								10*time.Minute,
							)
						},
					),
				)

				persisted := loadAsanaWebhookEvent(t, pgClient, event.DeliveryID)
				assert.Nil(t, persisted.ProcessingOwnerToken)
				assert.Nil(t, persisted.ProcessingStartedAt)
				assert.Equal(t, tt.wantDead, persisted.DeadLetteredAt != nil)
				assert.Equal(t, !tt.wantDead, persisted.NextAttemptAt != nil)
				assert.Equal(t, tt.attemptCount, persisted.AttemptCount)
				require.NotNil(t, persisted.LastError)
				assert.Equal(t, "Asana webhook event processing lease expired", *persisted.LastError)
			},
		)
	}
}

func TestAsanaWebhookEvent_DeleteExpiredBeforeBatch(t *testing.T) {
	t.Parallel()

	pgClient := test.PGClient(t)
	now := time.Now().UTC().Round(time.Microsecond)
	suffix := gid.New(gid.NewTenantID(), coredata.TaskSyncJobEntityType).String()
	expiredProcessedAt := now.Add(-2 * time.Hour)
	keptProcessedAt := now.Add(-30 * time.Minute)
	expiredDeadLetteredAt := now.Add(-4 * time.Hour)

	expiredProcessed := coredata.NewAsanaWebhookEvent("expired-processed-"+suffix, []byte(`{}`))
	expiredProcessed.ProcessedAt = &expiredProcessedAt
	expiredProcessed.UpdatedAt = expiredProcessedAt

	keptProcessed := coredata.NewAsanaWebhookEvent("kept-processed-"+suffix, []byte(`{}`))
	keptProcessed.ProcessedAt = &keptProcessedAt
	keptProcessed.UpdatedAt = keptProcessedAt

	expiredDeadLetter := coredata.NewAsanaWebhookEvent("expired-dead-letter-"+suffix, []byte(`{}`))
	expiredDeadLetter.DeadLetteredAt = &expiredDeadLetteredAt
	expiredDeadLetter.UpdatedAt = expiredDeadLetteredAt

	pending := coredata.NewAsanaWebhookEvent("pending-"+suffix, []byte(`{}`))

	require.NoError(
		t,
		pgClient.WithTx(
			t.Context(),
			func(ctx context.Context, tx pg.Tx) error {
				for _, event := range []*coredata.AsanaWebhookEvent{
					expiredProcessed,
					keptProcessed,
					expiredDeadLetter,
					pending,
				} {
					if _, err := event.Insert(ctx, tx); err != nil {
						return err
					}
				}

				return nil
			},
		),
	)
	t.Cleanup(
		func() {
			_ = pgClient.WithConn(
				context.Background(),
				func(ctx context.Context, conn pg.Querier) error {
					_, err := conn.Exec(
						ctx,
						`DELETE FROM asana_webhook_events
						WHERE delivery_id IN (
							@expired_processed,
							@kept_processed,
							@expired_dead_letter,
							@pending
						)`,
						pgx.StrictNamedArgs{
							"expired_processed":   expiredProcessed.DeliveryID,
							"kept_processed":      keptProcessed.DeliveryID,
							"expired_dead_letter": expiredDeadLetter.DeliveryID,
							"pending":             pending.DeliveryID,
						},
					)

					return err
				},
			)
		},
	)

	require.NoError(
		t,
		pgClient.WithConn(
			t.Context(),
			func(ctx context.Context, conn pg.Querier) error {
				if _, err := coredata.DeleteProcessedAsanaWebhookEventsBeforeBatch(
					ctx,
					conn,
					now.Add(-time.Hour),
					1000,
				); err != nil {
					return err
				}

				_, err := coredata.DeleteDeadLetteredAsanaWebhookEventsBeforeBatch(
					ctx,
					conn,
					now.Add(-3*time.Hour),
					1000,
				)

				return err
			},
		),
	)

	var remaining []string

	require.NoError(
		t,
		pgClient.WithConn(
			t.Context(),
			func(ctx context.Context, conn pg.Querier) error {
				rows, err := conn.Query(
					ctx,
					`SELECT delivery_id
					FROM asana_webhook_events
					WHERE delivery_id IN (
						@expired_processed,
						@kept_processed,
						@expired_dead_letter,
						@pending
					)
					ORDER BY delivery_id`,
					pgx.StrictNamedArgs{
						"expired_processed":   expiredProcessed.DeliveryID,
						"kept_processed":      keptProcessed.DeliveryID,
						"expired_dead_letter": expiredDeadLetter.DeliveryID,
						"pending":             pending.DeliveryID,
					},
				)
				if err != nil {
					return err
				}

				remaining, err = pgx.CollectRows(rows, pgx.RowTo[string])

				return err
			},
		),
	)
	require.Equal(
		t,
		[]string{keptProcessed.DeliveryID, pending.DeliveryID},
		remaining,
	)
}
