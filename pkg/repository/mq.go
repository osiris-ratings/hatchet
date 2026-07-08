package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/hatchet-dev/hatchet/pkg/repository/cache"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
	"github.com/hatchet-dev/hatchet/pkg/telemetry"
)

// lastActiveTouchInterval is how often a publishing process refreshes a queue's
// MessageQueue.lastActive. It must stay well under CleanupMessageQueue's 1h
// idle threshold so an actively-published queue is never reap-eligible, while
// keeping the parent-row upsert (an exclusive row lock) off the per-message
// hot path.
const lastActiveTouchInterval = 5 * time.Minute

type PubSubMessage struct {
	QueueName string          `json:"queue_name"`
	Payload   json.RawMessage `json:"payload"`
}

type MessageQueueRepository interface {
	// PubSub
	Listen(ctx context.Context, name string, f func(ctx context.Context, notification *PubSubMessage) error) error
	Notify(ctx context.Context, name string, payload string, durable, autoDeleted, exclusive bool) error

	// Queues
	BindQueue(ctx context.Context, queue string, durable, autoDeleted, exclusive bool, exclusiveConsumer *string) error
	UpdateQueueLastActive(ctx context.Context, queue string) error
	CleanupQueues(ctx context.Context) error

	// Messages
	AddMessage(ctx context.Context, queue string, payload []byte) error
	AddMessageEnsuringQueue(ctx context.Context, queue string, payload []byte, durable, autoDeleted, exclusive bool) error
	ReadMessages(ctx context.Context, queue string, qos int) ([]*sqlcv1.ReadMessagesRow, error)
	AckMessage(ctx context.Context, id int64) error
	CleanupMessageQueueItems(ctx context.Context) error
}

type messageQueueRepository struct {
	*sharedRepository

	m *multiplexedListener

	// lastActiveTouch records queues whose lastActive this process refreshed
	// within the touch interval, so publishes to them can skip the parent-row
	// upsert.
	lastActiveTouch *cache.Cache
}

func newMessageQueueRepository(shared *sharedRepository) (*messageQueueRepository, func() error) {
	m := newMultiplexedListener(shared.l, shared.pool)
	lastActiveTouch := cache.New(lastActiveTouchInterval)

	return &messageQueueRepository{
			sharedRepository: shared,
			m:                m,
			lastActiveTouch:  lastActiveTouch,
		}, func() error {
			m.cancel()
			lastActiveTouch.Stop()
			return nil
		}
}

func (m *messageQueueRepository) Listen(ctx context.Context, name string, f func(ctx context.Context, notification *PubSubMessage) error) error {
	return m.m.listen(ctx, name, f)
}

func (m *messageQueueRepository) Notify(ctx context.Context, name string, payload string, durable, autoDeleted, exclusive bool) error {
	wrappedPayload, err := m.m.wrapMessage(name, payload)
	if err != nil {
		m.l.Error().Ctx(ctx).Err(err).Msg("error wrapping message")
		return err
	}

	// PostgreSQL's pg_notify has an 8000 byte limit
	// If the wrapped message exceeds this, fall back to database storage
	if len(wrappedPayload) > 8000 {
		// An auto-deleted queue can be reaped by CleanupMessageQueue in the window
		// between the producer's 15s existence-cache hit and this insert, so route
		// through the touch-throttled self-healing path. This covers EXCLUSIVE
		// auto-deleted queues too — the dispatcher queue (expirable ⇒ autoDeleted,
		// exclusive) and controller consumer queues — which the reaper deletes
		// regardless of exclusivity.
		if autoDeleted {
			return m.AddMessageEnsuringQueue(ctx, name, []byte(payload), durable, autoDeleted, exclusive)
		}

		return m.AddMessage(ctx, name, []byte(payload))
	}

	return m.m.notify(ctx, wrappedPayload)
}

func (m *messageQueueRepository) AddMessage(ctx context.Context, queue string, payload []byte) error {
	return m.queries.AddMessage(ctx, m.pool, sqlcv1.AddMessageParams{
		Queueid: queue,
		Payload: payload,
	})
}

func (m *messageQueueRepository) AddMessageEnsuringQueue(ctx context.Context, queue string, payload []byte, durable, autoDeleted, exclusive bool) error {
	if !autoDeleted {
		return m.AddMessage(ctx, queue, payload)
	}

	// The ensure-queue upsert takes an exclusive lock on the parent MessageQueue
	// row, so running it on every publish serializes all publishers to the same
	// queue. One touch per interval per process is enough to keep an
	// actively-published queue outside CleanupMessageQueue's 1h idle threshold —
	// it is never reaped, so its pending items are never orphaned.
	if _, touched := m.lastActiveTouch.Get(queue); !touched {
		return m.addMessageWithQueueTouch(ctx, queue, payload, durable, autoDeleted, exclusive)
	}

	// Recently-touched queue: the plain insert's FK check takes only a KEY SHARE
	// lock on the parent row, so concurrent publishers do not serialize. It
	// fails with a foreign-key violation exactly when the parent is missing
	// anyway (a reap more than an hour after this process's last publish, with
	// the touch entry still cached, cannot happen — the cache TTL is minutes —
	// so this covers races with concurrent reaps and out-of-band deletes).
	err := m.AddMessage(ctx, queue, payload)

	if err == nil || !isForeignKeyViolation(err) {
		return err
	}

	return m.addMessageWithQueueTouch(ctx, queue, payload, durable, autoDeleted, exclusive)
}

// addMessageWithQueueTouch upserts the parent queue row (refreshing
// lastActive) and inserts the item in a single atomic statement, then records
// the touch so subsequent publishes within the interval take the plain-insert
// path.
func (m *messageQueueRepository) addMessageWithQueueTouch(ctx context.Context, queue string, payload []byte, durable, autoDeleted, exclusive bool) error {
	err := m.queries.AddMessageEnsuringQueue(ctx, m.pool, sqlcv1.AddMessageEnsuringQueueParams{
		Queueid:     queue,
		Payload:     payload,
		Durable:     durable,
		Autodeleted: autoDeleted,
		Exclusive:   exclusive,
	})

	if err != nil {
		return err
	}

	m.lastActiveTouch.Set(queue, true)

	return nil
}

func isForeignKeyViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgerrcode.ForeignKeyViolation
}

func (m *messageQueueRepository) BindQueue(ctx context.Context, queue string, durable, autoDeleted, exclusive bool, exclusiveConsumer *string) error {
	// if exclusive, but no consumer, return error
	if exclusive && exclusiveConsumer == nil {
		return errors.New("exclusive queue must have exclusive consumer")
	}

	params := sqlcv1.UpsertMessageQueueParams{
		Name:        queue,
		Durable:     durable,
		Autodeleted: autoDeleted,
		Exclusive:   exclusive,
	}

	if exclusiveConsumer != nil {
		parsedUuid := uuid.MustParse(*exclusiveConsumer)
		params.ExclusiveConsumerId = &parsedUuid
	}

	_, err := m.queries.UpsertMessageQueue(ctx, m.pool, params)

	return err
}

func (m *messageQueueRepository) UpdateQueueLastActive(ctx context.Context, queue string) error {
	return m.queries.UpdateMessageQueueActive(ctx, m.pool, queue)
}

func (m *messageQueueRepository) CleanupQueues(ctx context.Context) error {
	return m.queries.CleanupMessageQueue(ctx, m.pool)
}

func (m *messageQueueRepository) ReadMessages(ctx context.Context, queue string, qos int) ([]*sqlcv1.ReadMessagesRow, error) {
	ctx, span := telemetry.NewSpan(ctx, "pgmq-read-messages")
	defer span.End()

	return m.queries.ReadMessages(ctx, m.pool, sqlcv1.ReadMessagesParams{
		Queueid: queue,
		Limit:   pgtype.Int4{Int32: int32(qos), Valid: true}, // nolint: gosec
	})
}

func (m *messageQueueRepository) AckMessage(ctx context.Context, id int64) error {
	return m.queries.BulkAckMessages(ctx, m.pool, []int64{id})
}

func (m *messageQueueRepository) CleanupMessageQueueItems(ctx context.Context) error {
	// setup telemetry
	ctx, span := telemetry.NewSpan(ctx, "cleanup-message-queues-database")
	defer span.End()

	// get the min and max queue items
	minMax, err := m.queries.GetMinMaxExpiredMessageQueueItems(ctx, m.pool)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}

		return fmt.Errorf("could not get min max processed queue items: %w", err)
	}

	if minMax == nil {
		return nil
	}

	minId := minMax.MinId
	maxId := minMax.MaxId

	if maxId == 0 {
		return nil
	}

	// iterate until we have no more queue items to process
	var batchSize int64 = 10000
	var currBatch int64

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		currBatch++

		currMax := minId + batchSize*currBatch

		if currMax > maxId {
			currMax = maxId
		}

		// get the next batch of queue items
		err := m.queries.CleanupMessageQueueItems(ctx, m.pool, sqlcv1.CleanupMessageQueueItemsParams{
			Minid: minId,
			Maxid: minId + batchSize*currBatch,
		})

		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil
			}

			return fmt.Errorf("could not cleanup queue items: %w", err)
		}

		if currMax == maxId {
			break
		}
	}

	return nil
}
