package config

import (
	"os"
	"strconv"
	"time"
)

// Config holds all runtime configuration loaded from environment variables.
type Config struct {
	DBHost     string
	DBPort     string
	DBUser     string
	DBPassword string
	DBName     string
	ServerPort string

	KafkaBrokers   string
	KafkaTopic     string
	KafkaDLQTopic  string
	KafkaGroupID   string
	WorkerPoolSize int

	RedisAddr            string
	RedisPassword        string
	RedisDB              int
	RateLimitWindow      time.Duration
	RateLimitMaxRequests int64
	RateLimitRetryMs     time.Duration
}

// Load reads configuration from the environment, applying sensible defaults.
func Load() Config {
	return Config{
		DBHost:     getEnv("DB_HOST", "localhost"),
		DBPort:     getEnv("DB_PORT", "5432"),
		DBUser:     getEnv("DB_USER", "postgres"),
		DBPassword: getEnv("DB_PASSWORD", "postgres"),
		DBName:     getEnv("DB_NAME", "jobqueue"),
		ServerPort: getEnv("SERVER_PORT", "8080"),

		KafkaBrokers:   getEnv("KAFKA_BROKERS", "localhost:9092"),
		KafkaTopic:     getEnv("KAFKA_TOPIC", "jobs"),
		KafkaDLQTopic:  getEnv("KAFKA_DLQ_TOPIC", "jobs.dlq"),
		KafkaGroupID:   getEnv("KAFKA_GROUP_ID", "job-workers"),
		WorkerPoolSize: getEnvInt("WORKER_POOL_SIZE", 10),

		RedisAddr:            getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword:        getEnv("REDIS_PASSWORD", ""),
		RedisDB:              getEnvInt("REDIS_DB", 0),
		RateLimitWindow:      time.Duration(getEnvInt("RATE_LIMIT_WINDOW_SECONDS", 60)) * time.Second,
		RateLimitMaxRequests: int64(getEnvInt("RATE_LIMIT_MAX_REQUESTS", 100)),
		RateLimitRetryMs:     time.Duration(getEnvInt("RATE_LIMIT_RETRY_MS", 100)) * time.Millisecond,
	}
}

func getEnvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
