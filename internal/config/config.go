package config

import (
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	Port          string
	PostgresDSN   string
	RedisAddr     string
	RedisPassword string
	RedisDB       int

	KafkaBrokers  []string 
	KafkaTopic    string
	KafkaGroupID  string

	FCMCredFile   string

	OutboxPollInterval time.Duration
}

func Load() *Config {
	_ = godotenv.Load()

	return &Config{
		Port:          getEnv("PORT", "8088"),
		PostgresDSN:   getEnv("POSTGRES_DSN", "host=localhost user=root password=secretpassword dbname=chat_db port=5432 sslmode=disable"),
		RedisAddr:     getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword: getEnv("REDIS_PASSWORD", ""),
		RedisDB:       getEnvAsInt("REDIS_DB", 0),

		KafkaBrokers:  []string{getEnv("KAFKA_BROKER", "localhost:9092")}, 
		KafkaTopic:    getEnv("KAFKA_TOPIC", "message.sent"),
		KafkaGroupID:  getEnv("KAFKA_GROUP_ID", "notification-group"),

		FCMCredFile:       getEnv("FCM_CRED_FILE", "firebase-key.json"),
		OutboxPollInterval: getEnvAsDuration("OUTBOX_POLL_INTERVAL", 5*time.Second),
	}
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}


func getEnvAsInt(key string, fallback int) int {
	if value, exists := os.LookupEnv(key); exists {
		if intVal, err := strconv.Atoi(value); err == nil {
			return intVal
		}
	}
	return fallback
}


func getEnvAsDuration(key string, fallback time.Duration) time.Duration {
	if value, exists := os.LookupEnv(key); exists {
		if dur, err := time.ParseDuration(value); err == nil {
			return dur
		}
	}
	return fallback
}