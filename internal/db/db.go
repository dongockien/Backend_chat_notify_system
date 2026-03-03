package db

import (
	"chat-notify-system/internal/models"
	"context"
	"fmt"
	"log"
	"time"

	"github.com/go-redis/redis/v8"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type Database struct {
	PG    *gorm.DB
	Redis *redis.Client
}

func NewDatabase(dsn, redisAddr, redisPassword string, redisDB int) (*Database, error) {
	pg, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("postgres connection failed: %w", err)
	}
	
	if err := pg.AutoMigrate(
		&models.Message{},
		&models.Device{},
		&models.Outbox{}, 
		&models.UserNotificationSetting{},
		&models.User{},
		&models.Conversation{},
	); err != nil {
		return nil, fmt.Errorf("auto migrate failed: %w", err)
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: redisPassword,
		DB:       redisDB,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping failed: %w", err)
	}

	log.Println("Kết nối PostgreSQL và redis thành công")
	return &Database{PG: pg, Redis: rdb}, nil
}

func (db *Database) Close() {
	if db.Redis != nil {
		_ = db.Redis.Close()
	}
	sqlDB, err := db.PG.DB()
	if err == nil && sqlDB != nil {
		_ = sqlDB.Close()
	}
}