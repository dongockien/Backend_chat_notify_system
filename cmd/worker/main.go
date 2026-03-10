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

// hàm chạy chính của tiến trình Consumer Worker.
// tiến trình này hoàn toàn độc lập, chạy song song với API Server để giảm tải cho API.
func main() {
	// đọc toàn bộ các cấu hình từ biến môi trường file .env vào biến cfg
	cfg := config.Load()

	// khởi tạo kết nối tới cơ sở dữ liệu (PostgreSQL) và bộ nhớ đệm (Redis)
	database, err := db.NewDatabase(cfg.PostgresDSN, cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB)
	if err != nil {
		// nếu không kết nối được database, tiến trình này không thể tra cứu ai đang online/offline,
		// nên dùng Fatalf để ghi log và ép chương trình dừng lại ngay lập tức
		log.Fatalf("Failed to init db: %v", err)
	}
	// lệnh defer đảm bảo khi tiến trình worker này bị tắt, nó sẽ gọi hàm Close() để ngắt kết nối DB gọn gàng
	defer database.Close()

	// khởi tạo một client (bộ công cụ) để giao tiếp với dịch vụ Firebase Cloud Messaging (FCM) của Google.
	// truyền vào đường dẫn tới file JSON chứa khóa bí mật (credentials) của dự án Firebase.
	fcmClient, err := fcm.NewClient(cfg.FCMCredFile)
	if err != nil {
		log.Fatalf("Failed to init FCM: %v", err) // không có bộ công cụ của google thì cũng không bắn thông báo được -> sập luôn
	}

	// tạo ra một context có khả năng lắng nghe tín hiệu từ hệ điều hành.
	// khi bạn bấm Ctrl+C hoặc khi Docker gửi lệnh tắt (SIGTERM), biến ctx này sẽ nhận được thông báo để dừng Worker an toàn.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop() // hủy bỏ bộ lắng nghe tín hiệu này khi hàm main chuẩn bị kết thúc

	// Khởi tạo một Kafka Producer (Bút ghi).
	// Khi Consumer đọc một tin nhắn và cố gắng gửi FCM thất bại quá nhiều lần (quá số lần retry),
	// nó cần một cái bút ghi để vứt tin nhắn vô phương cứu chữa đó sang một Topic đặc biệt tên là "notification.dlq" (Dead Letter Queue - Thùng rác chứa thư chết).
	kafka.InitProducer(cfg.KafkaBrokers, "notification.dlq")

	// Bắt đầu kích hoạt Consumer. Hàm này chứa một vòng lặp vô hạn để liên tục lấy tin nhắn mới từ Kafka xuống.
	// Chú ý: Hàm này là một hàm chặn (blocking). Nó sẽ bắt luồng main() đứng im ở đây và chạy liên tục.
	// Ta truyền vào nó biến ctx (để biết khi nào cần tắt), database (để tra cứu), fcmClient (để gửi Push) và cfg (chứa tên Topic cần đọc).
	if err := kafka.StartConsumer(ctx, database, fcmClient, cfg); err != nil {
		// nếu vòng lặp bên trong StartConsumer bị vỡ và văng ra lỗi, in log để cảnh báo
		log.Printf("Consumer stopped with error: %v", err)
	}
}
