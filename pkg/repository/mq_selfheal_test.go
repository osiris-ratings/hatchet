//go:build !e2e && !load && !rampup && !integration

package repository

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
)

func TestAddMessageCreatesMissingQueue(t *testing.T) {
	pool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	ctx := context.Background()
	q := sqlcv1.New()
	const name = "mq-selfheal-missing-queue"

	err := q.AddMessage(ctx, pool, sqlcv1.AddMessageParams{
		Queueid:     name,
		Payload:     []byte(`{"hello":"world"}`),
		Durable:     false,
		Autodeleted: true,
		Exclusive:   false,
	})
	require.NoError(t, err, "AddMessage must create the parent queue, not raise MessageQueueItem_queueId_fkey (23503)")

	var autoDeleted bool
	err = pool.QueryRow(ctx, `SELECT "autoDeleted" FROM "MessageQueue" WHERE "name" = $1`, name).Scan(&autoDeleted)
	require.NoError(t, err)
	assert.True(t, autoDeleted, "the parent queue must be created with the supplied attributes")

	var items int
	err = pool.QueryRow(ctx, `SELECT count(*) FROM "MessageQueueItem" WHERE "queueId" = $1`, name).Scan(&items)
	require.NoError(t, err)
	assert.Equal(t, 1, items)
}

func TestAddMessageSurvivesQueueGC(t *testing.T) {
	pool, cleanup := setupPostgresWithMigration(t)
	defer cleanup()

	ctx := context.Background()
	q := sqlcv1.New()
	const name = "mq-selfheal-gc-queue"

	_, err := q.UpsertMessageQueue(ctx, pool, sqlcv1.UpsertMessageQueueParams{
		Name:        name,
		Durable:     false,
		Autodeleted: true,
		Exclusive:   false,
	})
	require.NoError(t, err)

	_, err = pool.Exec(ctx,
		`UPDATE "MessageQueue" SET "lastActive" = NOW() - INTERVAL '2 hours' WHERE "name" = $1`,
		name,
	)
	require.NoError(t, err)

	require.NoError(t, q.CleanupMessageQueue(ctx, pool))

	var reaped int
	err = pool.QueryRow(ctx, `SELECT count(*) FROM "MessageQueue" WHERE "name" = $1`, name).Scan(&reaped)
	require.NoError(t, err)
	require.Equal(t, 0, reaped, "queue should have been reaped, setting up the race")

	err = q.AddMessage(ctx, pool, sqlcv1.AddMessageParams{
		Queueid:     name,
		Payload:     []byte(`{"hello":"world"}`),
		Durable:     false,
		Autodeleted: true,
		Exclusive:   false,
	})
	require.NoError(t, err, "AddMessage must recreate a GC'd parent, not raise 23503")

	var exists int
	err = pool.QueryRow(ctx, `SELECT count(*) FROM "MessageQueue" WHERE "name" = $1`, name).Scan(&exists)
	require.NoError(t, err)
	assert.Equal(t, 1, exists, "the parent queue must be recreated by AddMessage")
}
