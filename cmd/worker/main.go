package main

import (
	"chat-notify-system/internal/config"
	"chat-notify-system/internal/db"
	"chat-notify-system/internal/fcm"
	"chat-notify-system/internal/kafka"
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	cfg := config.Load()

	database, err := db.NewDatabase(cfg.PostgresDSN, cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB)
	if err != nil {
		log.Fatalf("Failed to init db: %v", err)
	}
	defer database.Close()

	fcmClient, err := fcm.NewClient(cfg.FCMCredFile)
	if err != nil {
		log.Fatalf("Failed to init FCM: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	
	kafka.InitProducer(cfg.KafkaBrokers, "notification.dlq")
	
	
	if err := kafka.StartConsumer(ctx, database, fcmClient, cfg); err != nil {
		log.Printf("Consumer stopped with error: %v", err)
	}
}