package kafka

import (
	"chat-notify-system/internal/config"
	"chat-notify-system/internal/db"
	"chat-notify-system/internal/fcm"
	"chat-notify-system/internal/models"
	"chat-notify-system/internal/repository"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func StartConsumer(ctx context.Context, database *db.Database, fcmClient *fcm.Client, cfg *config.Config) error {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: cfg.KafkaBrokers,
		GroupID: cfg.KafkaGroupID,
		Topic: cfg.KafkaTopic,
		MinBytes: 10e3,
		MaxBytes: 10e6,
		CommitInterval: 0,
	})

	defer reader.Close()

	log.Println("Consumer đang lắng nghe topic 'message.sent'...")

	for {
		select {
		case <- ctx.Done():
			log.Println("Consumer nhận tín hiệu dừng")
			return nil 

		default:
			msg, err := reader.FetchMessage(ctx)
			if err != nil {
				log.Printf("Lỗi đọc kafka message: %v", err)
				time.Sleep(1 * time.Second)
				continue
			}
		if err := processMessage(ctx, database, fcmClient, msg); err != nil {
			log.Printf("Lỗi xử lý message partition=%d offset=%d: %v", msg.Partition, msg.Offset, err)

			continue
		}

		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("Lỗi commit offset: %v", err)
		}
	}
}
}
func processMessage(ctx context.Context, database *db.Database, fcmClient *fcm.Client, msg kafka.Message) error {
	var event models.MessageSentEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		return fmt.Errorf("parse JSON: %w", err)
	}
	log.Printf("📥 [KAFKA] Bắt đầu xử lý MessageID: %d | Gửi từ UserID: %d | Tới Group: %d", event.MessageID, event.SenderID, event.ConversationID)
	redisKey := fmt.Sprintf("processed_msg:%d", event.MessageID)
	
	ok, err := database.Redis.SetNX(ctx, redisKey, "1", 24*time.Hour).Result()
	if err != nil {
		return fmt.Errorf("redis SetNX: %w", err)
	}
	if !ok {
		log.Printf("MessageID %d đã được xử lý trước đó. Bỏ qua.", event.MessageID)
		return nil
	}
	userTokens, err := repository.GetActiveTokensBatch(ctx, database, event.ReceiverIDs)
	if err != nil {
		return fmt.Errorf("GetActiveTokensBatch: %w", err)
	}
	var allTokens []string
	for _, tokens := range userTokens {
		allTokens = append(allTokens, tokens...)
	}
if len(allTokens) == 0 {
		// log.Printf("MessageID %d không có device token nào hợp lệ.", event.MessageID)
		log.Printf(" MessageID %d: Dừng quy trình gửi Push (Do người nhận đang Online, đã Mute, hoặc chưa đăng nhập App).", event.MessageID)
		var skippedLogs []models.NotificationLog
		for _, receiverID := range event.ReceiverIDs {
			skippedLogs = append(skippedLogs, models.NotificationLog {
				MessageID: event.MessageID,
				UserID: receiverID,
				Status: "Skipped",
				Reason: "Bị bộ lọc chặn (Đang online Socket hoặc chưa có Token)",
			})
		}
		if err := database.PG.Create(&skippedLogs).Error; err != nil {
			log.Printf("Lỗi ghi sổ Skipped: %v", err)
		}
		return nil
	}

	
	var failureTokens []string
	var fcmErr error
	maxRetries := 3

	for attempt := 1; attempt <= maxRetries; attempt++ {
		
		sendCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		
		failureTokens, fcmErr = fcmClient.SendPushNotification(sendCtx, allTokens, event.Content)
		cancel() 
		if fcmErr == nil {
			log.Printf("[FCM SUCCESS] Đã bắn Push Notification thành công tới %d thiết bị!", len(allTokens))
			break 
		}

	log.Printf("Lỗi gọi FCM (Lần %d/%d): %v. Đang thử lại...", attempt, maxRetries, fcmErr)
		if attempt < maxRetries {
			time.Sleep(time.Duration(attempt*2) * time.Second) 
		}
	}

	
	if fcmErr != nil {
		log.Printf(" Thất bại hoàn toàn sau %d lần. Chuyển vào DLQ...", maxRetries)
		
	
		dlqErr := PublishToDLQ(ctx, msg.Key, msg.Value)
		if dlqErr != nil {
			log.Printf("CRITICAL: Lỗi ghi DLQ: %v", dlqErr)
		}
		
		
		return nil 
	}


	if len(failureTokens) > 0 {
		log.Printf("Phát hiện %d token không hợp lệ. Đang cập nhật DB...", len(failureTokens))
		if err := repository.InvalidateInvalidTokens(ctx, database, failureTokens); err != nil {
			log.Printf("Lỗi cập nhật token không hợp lệ: %v", err)
		}
	}
	var finalLogs []models.NotificationLog
	status := "Success"
	reason := fmt.Sprintf("Đã gửi lệnh Push tới %d thiết bị của Google FCM", len(allTokens))
	
	if fcmErr != nil {
		status = "failed"
		reason = fmt.Sprintf("Lỗi mạng/FCM Client: %v", fcmErr)
	}

	for _, receiverID := range event.ReceiverIDs {
		finalLogs = append(finalLogs, models.NotificationLog{
			MessageID: event.MessageID,
			UserID: receiverID,
			Status: status,
			Reason: reason,
		})
	}
	if err := database.PG.Create(&finalLogs).Error; err != nil {
		log.Printf("Lỗi ghi sổ SUCCESS/FAILED: %v", err)
	} else {
		log.Printf("Đã ghi thành công cho MessageID %d", event.MessageID)
	}
	return nil
}