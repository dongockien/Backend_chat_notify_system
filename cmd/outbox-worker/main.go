package main

import (
	"chat-notify-system/internal/config"
	"chat-notify-system/internal/db"
	"chat-notify-system/internal/kafka"
	"chat-notify-system/internal/models"
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gorm.io/gorm"
)

func main() {
	cfg := config.Load()

	database, err := db.NewDatabase(cfg.PostgresDSN, cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB)
	if err != nil {
		log.Fatalf("Failed to init db: %v", err)
	}
	defer database.Close()

	kafka.InitProducer(cfg.KafkaBrokers, cfg.KafkaTopic)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	
	ticker := time.NewTicker(cfg.OutboxPollInterval)
	defer ticker.Stop()

	log.Println("Outbox worker đang chạy và quét database...")

	for {
		select {
		case <-ctx.Done():
			log.Println("Outbox worker stopping...")
			return
		case <-ticker.C:
			
			if err := processOutbox(ctx, database.PG); err != nil {
				log.Printf(" Lỗi processOutbox: %v", err)
			}
		}
	}
}
func processOutbox(ctx context.Context, db *gorm.DB) error {
	var events []models.Outbox

	tx := db.Begin()
	if tx.Error != nil {
		return tx.Error
	}

	
	tx.Raw("SELECT * FROM outboxes WHERE status = 'pending' ORDER BY created_at ASC LIMIT 50 FOR UPDATE SKIP LOCKED").Scan(&events)

	if len(events) == 0 {
		tx.Rollback()
		return nil
	}

	for _, event := range events {
	
		var msgEvent models.MessageSentEvent
		if err := json.Unmarshal([]byte(event.Payload), &msgEvent); err != nil {
			log.Printf(" Lỗi parse JSON Outbox ID %d: %v", event.ID, err)
			tx.Exec("UPDATE outboxes SET status = 'failed' WHERE id = ?", event.ID)
			continue
		}

	
		err := kafka.PublishEvent(ctx, msgEvent)

		if err == nil {
			tx.Exec("UPDATE outboxes SET status = 'processed' WHERE id = ?", event.ID)
		} else {
			tx.Exec("UPDATE outboxes SET retry_count = retry_count + 1 WHERE id = ?", event.ID)
		}
	}
	
	return tx.Commit().Error
}