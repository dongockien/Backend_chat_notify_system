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

// StartConsumer khởi tạo một tiến trình lắng nghe tin nhắn từ Kafka.
// Hàm nhận vào context (để quản lý vòng đời), kết nối database, client FCM và cấu hình.
func StartConsumer(ctx context.Context, database *db.Database, fcmClient *fcm.Client, cfg *config.Config) error {

	// Khởi tạo một Kafka Reader (Consumer) với các thông số cấu hình
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.KafkaBrokers, // Danh sách địa chỉ máy chủ Kafka
		GroupID:        cfg.KafkaGroupID, // Định danh nhóm Consumer. Các consumer chung nhóm sẽ chia sẻ công việc (không đọc trùng tin).
		Topic:          cfg.KafkaTopic,   // Tên topic cần đọc (message.sent)
		MinBytes:       10e3,             // Kích thước tối thiểu mỗi lần fetch (10KB)
		MaxBytes:       10e6,             // Kích thước tối đa mỗi lần fetch (10MB)
		CommitInterval: 0,                // Tắt tính năng tự động commit (xác nhận) để tự kiểm soát luồng dữ liệu bằng tay
	})

	// Đảm bảo đóng kết nối với Kafka khi hàm kết thúc để giải phóng tài nguyên
	defer reader.Close()

	log.Println("Consumer đang lắng nghe topic 'message.sent'...")

	// Vòng lặp vô hạn để liên tục kéo tin nhắn mới từ Kafka về xử lý
	for {
		select {
		case <-ctx.Done(): // Nếu nhận được tín hiệu yêu cầu dừng từ hệ thống (khi tắt server)
			log.Println("Consumer nhận tín hiệu dừng")
			return nil // Thoát khỏi hàm một cách an toàn

		default: // Nếu không có tín hiệu dừng, tiến hành đọc tin nhắn

			// FetchMessage lấy một tin nhắn từ Kafka. Hàm này sẽ block (chờ) nếu chưa có tin nhắn nào.
			msg, err := reader.FetchMessage(ctx)
			if err != nil {
				log.Printf("Lỗi đọc kafka message: %v", err)
				time.Sleep(1 * time.Second) // Tạm nghỉ 1 giây trước khi thử lại để tránh làm treo CPU nếu Kafka đang lỗi
				continue                    // Bỏ qua phần dưới, quay lại đầu vòng lặp
			}

			// Gọi hàm processMessage để xử lý logic nghiệp vụ cho tin nhắn vừa lấy được
			if err := processMessage(ctx, database, fcmClient, msg); err != nil {
				log.Printf("Lỗi xử lý message partition=%d offset=%d: %v", msg.Partition, msg.Offset, err)
				continue // Nếu xử lý lỗi, bỏ qua lệnh commit bên dưới để Kafka có thể phân phối lại tin nhắn này
			}

			// Nếu xử lý thành công, gọi CommitMessages để báo cho Kafka biết tin nhắn này đã xong.
			// Offset (vị trí đọc) sẽ được lưu lại, lần sau Kafka sẽ không gửi lại tin nhắn này nữa.
			if err := reader.CommitMessages(ctx, msg); err != nil {
				log.Printf("Lỗi commit offset: %v", err)
			}
		}
	}
}

// processMessage chứa toàn bộ logic xử lý: chống trùng lặp, lọc token, gửi FCM và ghi log.
func processMessage(ctx context.Context, database *db.Database, fcmClient *fcm.Client, msg kafka.Message) error {

	var event models.MessageSentEvent

	// msg.Value là dữ liệu thô (mảng byte) chứa JSON. Hàm Unmarshal giải mã nó vào biến struct 'event'.
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		return fmt.Errorf("parse JSON: %w", err)
	}

	log.Printf("📥 [KAFKA] Bắt đầu xử lý MessageID: %d | Gửi từ UserID: %d | Tới Group: %d", event.MessageID, event.SenderID, event.ConversationID)

	// Bước 1: Kỹ thuật Idempotency (Chống xử lý lặp) sử dụng Redis.
	// Tạo một key duy nhất dựa trên ID của tin nhắn.
	redisKey := fmt.Sprintf("processed_msg:%d", event.MessageID)

	// SetNX (Set if Not eXists): Lệnh Redis chỉ lưu giá trị nếu key chưa tồn tại.
	// Nếu key đã có, nó trả về ok = false. Thời gian sống của key là 24 giờ.
	ok, err := database.Redis.SetNX(ctx, redisKey, "1", 24*time.Hour).Result()
	if err != nil {
		return fmt.Errorf("redis SetNX: %w", err)
	}
	if !ok {
		// Nếu ok == false, nghĩa là tin nhắn này đã được xử lý trước đó rồi (có thể do Kafka gửi đúp).
		log.Printf("MessageID %d đã được xử lý trước đó. Bỏ qua.", event.MessageID)
		return nil // Trả về nil để hàm ngoài commit tin nhắn này, kết thúc luồng.
	}

	// Bước 2: Gọi Repository để lấy danh sách FCM Token hợp lệ của những người nhận.
	// Quá trình này đã tự động lọc bỏ những người đang Online App hoặc đang bật chế độ Mute (im lặng).
	userTokens, err := repository.GetActiveTokensBatch(ctx, database, event.ReceiverIDs)
	if err != nil {
		return fmt.Errorf("GetActiveTokensBatch: %w", err)
	}

	var allTokens []string // Mảng chứa toàn bộ token gộp lại

	// userTokens là một map (từ UserID -> mảng tokens). Vòng lặp này gộp tất cả mảng con thành một mảng lớn.
	for _, tokens := range userTokens {
		allTokens = append(allTokens, tokens...)
	}

	// Bước 3: Kiểm tra nếu không có ai cần gửi thông báo
	if len(allTokens) == 0 {
		log.Printf(" MessageID %d: Dừng quy trình gửi Push (Do người nhận đang Online, đã Mute, hoặc chưa đăng nhập App).", event.MessageID)

		var skippedLogs []models.NotificationLog
		// Tạo log cho từng người nhận để lưu lại bằng chứng là hệ thống đã chủ động bỏ qua (Skipped)
		for _, receiverID := range event.ReceiverIDs {
			skippedLogs = append(skippedLogs, models.NotificationLog{
				MessageID: event.MessageID,
				UserID:    receiverID,
				Status:    "Skipped",
				Reason:    "Bị bộ lọc chặn (Đang online Socket hoặc chưa có Token)",
			})
		}

		// Lưu mảng log vào database
		if err := database.PG.Create(&skippedLogs).Error; err != nil {
			log.Printf("Lỗi ghi sổ Skipped: %v", err)
		}
		return nil // Hoàn tất quá trình
	}

	// Bước 4: Gọi Firebase để gửi thông báo có cơ chế thử lại (Retry)
	var failureTokens []string
	var fcmErr error
	maxRetries := 3 // Cấu hình thử lại tối đa 3 lần nếu mạng lỗi

	for attempt := 1; attempt <= maxRetries; attempt++ {

		// Tạo một context có thời gian đếm ngược 10 giây. Tránh việc gọi API Firebase bị treo vĩnh viễn.
		sendCtx, cancel := context.WithTimeout(ctx, 10*time.Second)

		// Gọi hàm gửi của fcmClient. Hàm trả về danh sách token hỏng và lỗi cấu hình (nếu có).
		failureTokens, fcmErr = fcmClient.SendPushNotification(sendCtx, allTokens, event.Content)
		cancel() // Hủy context đếm ngược sau khi gọi xong để giải phóng bộ nhớ

		if fcmErr == nil {
			// Nếu không có lỗi, in log thành công và thoát khỏi vòng lặp thử lại (break)
			log.Printf("[FCM SUCCESS] Đã bắn Push Notification thành công tới %d thiết bị!", len(allTokens))
			break
		}

		log.Printf("Lỗi gọi FCM (Lần %d/%d): %v. Đang thử lại...", attempt, maxRetries, fcmErr)

		// Nếu vẫn còn cơ hội thử lại, cho hệ thống ngủ một lúc trước khi gọi lại.
		// Thời gian ngủ tăng dần: Lần 1 ngủ 2s, lần 2 ngủ 4s (kỹ thuật Exponential Backoff).
		if attempt < maxRetries {
			time.Sleep(time.Duration(attempt*2) * time.Second)
		}
	}

	// Bước 5: Xử lý khi mọi nỗ lực đều thất bại
	if fcmErr != nil {
		log.Printf(" Thất bại hoàn toàn sau %d lần. Chuyển vào DLQ...", maxRetries)

		// Nếu không gửi được, đẩy tin nhắn sang topic Dead Letter Queue (DLQ) để xử lý thủ công sau.
		dlqErr := PublishToDLQ(ctx, msg.Key, msg.Value)
		if dlqErr != nil {
			log.Printf("CRITICAL: Lỗi ghi DLQ: %v", dlqErr) // Lỗi nghiêm trọng nếu DLQ cũng hỏng
		}

		// Trả về nil để luồng chính tiến hành commit tin nhắn hiện tại (xóa nó khỏi luồng chính để tránh tắc nghẽn).
		return nil
	}

	// Bước 6: Vô hiệu hóa (Invalidate) các token rác
	if len(failureTokens) > 0 {
		log.Printf("Phát hiện %d token không hợp lệ. Đang cập nhật DB...", len(failureTokens))
		// Gọi hàm cập nhật is_active = false trong DB cho những token bị Firebase báo là đã lỗi/gỡ app
		if err := repository.InvalidateInvalidTokens(ctx, database, failureTokens); err != nil {
			log.Printf("Lỗi cập nhật token không hợp lệ: %v", err)
		}
	}

	// Bước 7: Ghi sổ cái (Audit Log) lưu trạng thái cuối cùng vào Database
	var finalLogs []models.NotificationLog
	status := "Success"
	reason := fmt.Sprintf("Đã gửi lệnh Push tới %d thiết bị của Google FCM", len(allTokens))

	// Đoạn check này là dư thừa an toàn, vì phía trên nếu fcmErr != nil ta đã return sớm rồi.
	if fcmErr != nil {
		status = "failed"
		reason = fmt.Sprintf("Lỗi mạng/FCM Client: %v", fcmErr)
	}

	// Tạo log cho từng user nhận thông báo
	for _, receiverID := range event.ReceiverIDs {
		finalLogs = append(finalLogs, models.NotificationLog{
			MessageID: event.MessageID,
			UserID:    receiverID,
			Status:    status,
			Reason:    reason,
		})
	}

	// Ghi toàn bộ dữ liệu log vào database bằng một thao tác duy nhất (Batch Insert)
	if err := database.PG.Create(&finalLogs).Error; err != nil {
		log.Printf("Lỗi ghi sổ SUCCESS/FAILED: %v", err)
	} else {
		log.Printf("Đã ghi thành công cho MessageID %d", event.MessageID)
	}

	return nil // Báo cáo quá trình xử lý hoàn tất không có lỗi hệ thống
}
