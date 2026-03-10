package main

import (
	"chat-notify-system/internal/config"
	"chat-notify-system/internal/db"
	"chat-notify-system/internal/models"
	"log"
)

// Hàm main của file migrate. Khi chạy lệnh `go run cmd/migrate/main.go`, nó sẽ thực thi từ trên xuống dưới rồi tự động thoát.
func main() {
	// Tải các biến môi trường từ file .env (chứa thông tin kết nối DB, Redis...)
	cfg := config.Load()

	// Khởi tạo kết nối tới PostgreSQL và Redis sử dụng cấu hình vừa tải
	database, err := db.NewDatabase(cfg.PostgresDSN, cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB)
	if err != nil {
		log.Fatalf("Failed to connect db: %v", err) // Báo lỗi và dừng ngay nếu không nối được DB
	}
	defer database.Close() // Đảm bảo đóng kết nối DB khi chạy xong script này

	// 1. chạy migration tự động
	// Tính năng cực mạnh của GORM: Nhận vào danh sách các struct model và tự động dịch chúng thành các câu lệnh CREATE TABLE / ALTER TABLE.
	// Nếu bảng chưa có, sẽ tạo mới. Nếu bảng thiếu cột, nó tự động thêm cột.
	if err := database.PG.AutoMigrate(
		&models.User{},
		&models.Conversation{},
		&models.Message{},
		&models.Device{},
		&models.Outbox{},
		&models.UserNotificationSetting{},
		&models.NotificationLog{},
	); err != nil {
		log.Fatalf("Migration failed: %v", err) // Lỗi thì dừng luôn quá trình cài đặt DB
	}

	// 2. bơm dữ liệu (seed data)
	// Đoạn này dùng các câu lệnh SQL thô (Raw SQL) thông qua hàm Exec để chèn sẵn một vài user và phòng chat dùng cho việc test ứng dụng.

	// Tạo sẵn 3 user.
	// Mệnh đề 'ON CONFLICT (id) DO NOTHING' giúp đoạn script này có thể chạy đi chạy lại nhiều lần mà không bị lỗi trùng lặp dữ liệu (Duplicate Key).
	database.PG.Exec(`INSERT INTO users (id, name) VALUES 
        (1, 'Kiên (Admin)'), 
        (2, 'Bình (Dev)'), 
        (3, 'An (Tester)') 
        ON CONFLICT (id) DO NOTHING;`)

	// Tạo sẵn 1 phòng chat chung (Group Chat) có ID là 99.
	database.PG.Exec(`INSERT INTO conversations (id, name) VALUES 
        (99, 'Nhóm Chat Kỹ Thuật Số') 
        ON CONFLICT (id) DO NOTHING;`)

	// 3. quản lý bằng trung gian (N-N)
	// Bảng conversation_members là bảng nối giữa users và conversations (Nhiều user ở trong 1 phòng).
	// Dòng này rà soát nếu cấu trúc cũ bị lỗi thì xóa đi làm lại cho sạch sẽ.
	database.PG.Exec(`DROP TABLE IF EXISTS conversation_members;`)

	// Tạo lại bảng conversation_members.
	// Lệnh 'UNIQUE(conversation_id, user_id)' đảm bảo 1 user không thể bị add vào cùng 1 phòng 2 lần.
	database.PG.Exec(`CREATE TABLE IF NOT EXISTS conversation_members (
        conversation_id BIGINT, 
        user_id BIGINT,
        UNIQUE(conversation_id, user_id)
    );`)

	// Nhét 3 user mẫu ở trên vào chung phòng chat 99.
	database.PG.Exec(`INSERT INTO conversation_members (conversation_id, user_id) VALUES 
        (99, 1), (99, 2), (99, 3) 
        ON CONFLICT (conversation_id, user_id) DO NOTHING;`)

	log.Println(" Migration và Bơm dữ liệu mẫu thành công!") // Thông báo hoàn tất
}
