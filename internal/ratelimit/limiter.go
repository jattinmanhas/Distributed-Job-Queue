package ratelimit

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// slidingWindowScript implements a sliding-window rate-limit check atomically on
// the Redis server. It evicts entries older than the window, counts what remains,
// and admits the request (recording it) only if the count is below the limit.
var slidingWindowScript = redis.NewScript(`
local key = KEYS[1]
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local id = ARGV[4]
redis.call('ZREMRANGEBYSCORE', key, '-inf', now - window)
local count = redis.call('ZCARD', key)
if count < limit then
  redis.call('ZADD', key, now, id)
  redis.call('PEXPIRE', key, window)
  return 1
end
return 0
`)

// Limiter enforces a per-service sliding-window rate limit backed by a Redis
// sorted set.
type Limiter struct {
	client        *redis.Client
	window        time.Duration
	maxRequests   int64
	retryInterval time.Duration
}

// NewLimiter connects to Redis, verifies the connection with a ping, and returns
// a ready Limiter.
func NewLimiter(addr string, password string, db int, window time.Duration, maxRequests int64, retryInterval time.Duration) (*Limiter, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	if err := client.Ping(context.Background()).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("ping redis at %s: %w", addr, err)
	}

	return &Limiter{
		client:        client,
		window:        window,
		maxRequests:   maxRequests,
		retryInterval: retryInterval,
	}, nil
}

// Allow blocks until the given service is permitted to proceed under the sliding
// window, then returns nil. It never drops the request: when the limit is
// exceeded it sleeps retryInterval and retries. It returns ctx.Err() if the
// context is cancelled while waiting.
func (l *Limiter) Allow(ctx context.Context, service string) error {
	key := "rate_limit:" + service
	windowMs := l.window.Milliseconds()
	logged := false

	for {
		now := time.Now().UnixMilli()
		id := uuid.NewString()

		result, err := slidingWindowScript.Run(
			ctx, l.client, []string{key},
			now, windowMs, l.maxRequests, id,
		).Int64()
		if err != nil {
			return fmt.Errorf("run sliding window script for service %s: %w", service, err)
		}

		if result == 1 {
			return nil
		}

		// Log only on the first denial to avoid spamming the log on every retry.
		if !logged {
			log.Printf("rate limit exceeded for service=%s, retrying in %s", service, l.retryInterval)
			logged = true
		}

		select {
		case <-time.After(l.retryInterval):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Close releases the underlying Redis client.
func (l *Limiter) Close() {
	if err := l.client.Close(); err != nil {
		log.Printf("error closing redis client: %v", err)
	}
}
