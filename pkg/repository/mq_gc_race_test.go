//go:build !e2e && !load && !rampup && !integration

package repository

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
)

const gcRaceQueueName = "mq-gc-race-test-queue"

func TestCleanupReapsStaleAutoDeletedQueue(t *testing.T) {
	pool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	ctx := context.Background()
	q := sqlcv1.New()

	_, err := q.UpsertMessageQueue(ctx, pool, sqlcv1.UpsertMessageQueueParams{
		Name:        gcRaceQueueName,
		Durable:     false,
		Autodeleted: true,
		Exclusive:   false,
	})
	require.NoError(t, err)

	_, err = pool.Exec(ctx,
		`UPDATE "MessageQueue" SET "lastActive" = NOW() - INTERVAL '2 hours' WHERE "name" = $1`,
		gcRaceQueueName,
	)
	require.NoError(t, err)

	require.NoError(t, q.CleanupMessageQueue(ctx, pool))

	var count int
	err = pool.QueryRow(ctx, `SELECT count(*) FROM "MessageQueue" WHERE "name" = $1`, gcRaceQueueName).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count, "a stale auto-deleted queue must be reaped by CleanupMessageQueue")
}

func TestBindRefreshesLastActiveAndSurvivesCleanup(t *testing.T) {
	pool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	ctx := context.Background()
	q := sqlcv1.New()

	bind := func() {
		_, err := q.UpsertMessageQueue(ctx, pool, sqlcv1.UpsertMessageQueueParams{
			Name:        gcRaceQueueName,
			Durable:     false,
			Autodeleted: true,
			Exclusive:   false,
		})
		require.NoError(t, err)
	}

	bind()

	_, err := pool.Exec(ctx,
		`UPDATE "MessageQueue" SET "lastActive" = NOW() - INTERVAL '2 hours' WHERE "name" = $1`,
		gcRaceQueueName,
	)
	require.NoError(t, err)

	bind()

	var lastActive time.Time
	err = pool.QueryRow(ctx, `SELECT "lastActive" FROM "MessageQueue" WHERE "name" = $1`, gcRaceQueueName).Scan(&lastActive)
	require.NoError(t, err)
	assert.WithinDuration(t, time.Now(), lastActive, time.Minute,
		"binding a queue must refresh lastActive so an actively-produced queue is not reaped")

	require.NoError(t, q.CleanupMessageQueue(ctx, pool))

	var count int
	err = pool.QueryRow(ctx, `SELECT count(*) FROM "MessageQueue" WHERE "name" = $1`, gcRaceQueueName).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count, "a re-bound queue must survive the 1h-inactivity sweep")

	err = q.AddMessage(ctx, pool, sqlcv1.AddMessageParams{
		Payload: []byte(`{"hello":"world"}`),
		Queueid: gcRaceQueueName,
	})
	require.NoError(t, err, "AddMessage after re-bind must not raise MessageQueueItem_queueId_fkey (23503)")
}
