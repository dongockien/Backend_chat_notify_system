package main

import (
	"chat-notify-system/internal/config"
	"chat-notify-system/internal/db"
	"chat-notify-system/internal/models"
	"log"
)

func main() {
	cfg := config.Load()
	database, err := db.NewDatabase(cfg.PostgresDSN, cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB)
	if err != nil {
		log.Fatalf("Failed to connect db: %v", err)
	}
	defer database.Close()

	// 1. Tự động tạo bảng
	if err := database.PG.AutoMigrate(
		&models.User{},       
		&models.Conversation{}, 
		&models.Message{},
		&models.Device{},
		&models.Outbox{},
		&models.UserNotificationSetting{},
		&models.NotificationLog{},
	); err != nil {
		log.Fatalf("Migration failed: %v", err)
	}

	// 2. Bơm dữ liệu mẫu (Seed Data) dùng ON CONFLICT để chạy nhiều lần không bị lỗi trùng
	database.PG.Exec(`INSERT INTO users (id, name) VALUES 
		(1, 'Kiên (Admin)'), 
		(2, 'Bình (Dev)'), 
		(3, 'An (Tester)') 
		ON CONFLICT (id) DO NOTHING;`)

	database.PG.Exec(`INSERT INTO conversations (id, name) VALUES 
		(99, 'Nhóm Chat Kỹ Thuật Số') 
		ON CONFLICT (id) DO NOTHING;`)
database.PG.Exec(`DROP TABLE IF EXISTS conversation_members;`)
	
	database.PG.Exec(`CREATE TABLE IF NOT EXISTS conversation_members (
		conversation_id BIGINT, 
		user_id BIGINT,
		UNIQUE(conversation_id, user_id)
	);`)
	database.PG.Exec(`INSERT INTO conversation_members (conversation_id, user_id) VALUES 
		(99, 1), (99, 2), (99, 3) 
		ON CONFLICT (conversation_id, user_id) DO NOTHING;`)

	log.Println(" Migration và Bơm dữ liệu mẫu thành công!")
}