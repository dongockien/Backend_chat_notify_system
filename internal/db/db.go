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

// định nghĩa một struct gom cả 2 kết nối (postgres và redis) vào một chỗ
// việc gom lại thế này giúp mình dễ dàng truyền cả 2 kết nối này đi khắp nơi trong dự án chỉ bằng 1 biến duy nhất
type Database struct {
	PG    *gorm.DB      // con trỏ chứa phiên làm việc của postgres
	Redis *redis.Client // con trỏ chứa phiên làm việc của postgres
}

// hàm khởi tạo (constructor) dùng để tạo ra một đối tượng Database hoàn chỉnh
// hàm nhận vào các tham số cấu hình và trả về con trỏ của struct Database cùng với lỗi (nếu có)
func NewDatabase(dsn, redisAddr, redisPassword string, redisDB int) (*Database, error) {
	// dùng gorm.Open để kết nối vào postgres dựa trên chuỗi dsn (chứa host, port, user, pass)
	pg, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		// fmt.Errorf giúp định dạng chuỗi lỗi. %w (wrap) dùng để bọc cái lỗi gốc (err) vào bên trong câu báo lỗi của mình để dễ debug
		return nil, fmt.Errorf("postgres connection failed: %w", err)
	}

	// AutoMigrate tự động đồng bộ cấu trúc struct trong code go thành các bảng (table) trong database
	// nếu bảng chưa có, nó tạo mới. nếu bảng thiếu cột, nó tự thêm cột (không xóa dữ liệu cũ)
	if err := pg.AutoMigrate(
		&models.Message{},
		&models.Device{},
		&models.Outbox{},
		&models.UserNotificationSetting{},
		&models.User{},
		&models.Conversation{},
	); err != nil {
		return nil, fmt.Errorf("auto migrate failed: %w", err) // trả về lỗi nếu quá trình tạo bảng thất bại
	}

	// khởi tạo một client mới để nói chuyện với redis server dựa trên thông số cấu hình
	rdb := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: redisPassword,
		DB:       redisDB, // số thứ tự của kho lưu trữ bên trong redis (mặc định là 0)
	})

	// tạo một (context) đính kèm thời gian hết hạn là 5 giây
	// nếu redis đang bị sập, mình không muốn app bị treo chờ mãi mãi, đúng 5s không phản hồi là báo lỗi
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// gọi lệnh Ping (gửi một tín hiệu nhỏ) đến redis để chắc chắn là nó đang sống và có thể giao tiếp được
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping failed: %w", err)
	}

	log.Println("Kết nối PostgreSQL và redis thành công")
	// trả về đối tượng Database chứa 2 kết nối đã khởi tạo thành công để các file khác lấy ra dùng
	return &Database{PG: pg, Redis: rdb}, nil
}

// phương thức (method) được gắn vào struct Database thông qua biến receiver (db *Database)
// hàm này có nhiệm vụ ngắt kết nối an toàn (dọn dẹp tài nguyên) khi hệ thống tắt
func (db *Database) Close() {
	if db.Redis != nil {
		_ = db.Redis.Close()
	}
	sqlDB, err := db.PG.DB()
	if err == nil && sqlDB != nil {
		_ = sqlDB.Close()
	}
}
