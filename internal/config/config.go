package config

import (
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

// định nghĩa (struct) cho cả api và các worker chạy ngầm
type Config struct {
	Port          string // cổng chạy server api (vd: 8088)
	PostgresDSN   string // chuỗi kết nối tới db postgres (chứa user, pass, dbname)
	RedisAddr     string // địa chỉ máy chủ redis (vd: localhost:6379)
	RedisPassword string // mật khẩu redis
	RedisDB       int    // số thứ tự của database trong redis (mặc định là 0)

	KafkaBrokers []string // danh sách các máy chủ (broker) của kafka. kiểu mảng vì thực tế kafka thường chạy thành cụm (cluster)
	KafkaTopic   string   // tên cái chủ đề (topic) để push tin nhắn lên kafka
	KafkaGroupID string   // tên nhóm của consumer. các consumer cùng nhóm sẽ chia nhau đọc tin nhắn chứ không đọc trùng của nhau

	FCMCredFile string // đường dẫn tới file json chứa khóa bí mật của firebase để bắn push thông báo

	OutboxPollInterval time.Duration // khoảng thời gian (chu kỳ) để outbox worker quét database 1 lần
}

// hàm này làm nhiệm vụ thu thập tất cả thông số môi trường
func Load() *Config {
	// gọi thư viện godotenv để nạp các biến từ file .env lên bộ nhớ của hệ điều hành
	// dùng dấu '_' để bỏ qua lỗi nếu không tìm thấy file .env (ví dụ khi đưa lên docker/server thật thì mình set thẳng biến môi trường chứ k dùng file .env nữa)
	_ = godotenv.Load()

	// khởi tạo và trả về con trỏ trỏ tới struct Config đã được lấp đầy dữ liệu
	return &Config{
		Port:          getEnv("PORT", "8088"), // thử lấy cổng từ biến PORT, nếu không có thì xài mặc định "8088"
		PostgresDSN:   getEnv("POSTGRES_DSN", "host=localhost user=root password=secretpassword dbname=chat_db port=5432 sslmode=disable"),
		RedisAddr:     getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword: getEnv("REDIS_PASSWORD", ""),
		RedisDB:       getEnvAsInt("REDIS_DB", 0), // ép kiểu thẳng thành số nguyên cho redis db

		KafkaBrokers: []string{getEnv("KAFKA_BROKER", "localhost:9092")}, // đưa broker vào 1 mảng
		KafkaTopic:   getEnv("KAFKA_TOPIC", "message.sent"),
		KafkaGroupID: getEnv("KAFKA_GROUP_ID", "notification-group"),

		FCMCredFile:        getEnv("FCM_CRED_FILE", "firebase-key.json"),
		OutboxPollInterval: getEnvAsDuration("OUTBOX_POLL_INTERVAL", 5*time.Second), // ép thành kiểu thời gian (duration)
	}
}

// ham lôi một biến môi trường ra dạng chuỗi
func getEnv(key, fallback string) string {
	// os.LookupEnv đi tìm biến môi trường. nếu tồn tại (exists = true) thì trả về giá trị đó
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback // k tìm thấy thì xài hàng (fallback)
}

// hàm lôi biến môi trường ra và ráng ép nó thành kiểu số nguyên (int)
func getEnvAsInt(key string, fallback int) int {
	if value, exists := os.LookupEnv(key); exists {
		// strconv.Atoi (ASCII to Integer): hàm chuyển chuỗi thành số. nếu chuyển thành công (err == nil) thì trả về con số
		if intVal, err := strconv.Atoi(value); err == nil {
			return intVal
		}
	}
	return fallback // vỡ kế hoạch (không tồn tại hoặc chứa chữ cái không ép ra số được) thì lấy mặc định
}

// hàm lôi biến môi trường và ép thành kiểu thời lượng (time.Duration)
func getEnvAsDuration(key string, fallback time.Duration) time.Duration {
	if value, exists := os.LookupEnv(key); exists {
		// time.ParseDuration đọc các chuỗi như "5s", "10m", "2h" và biến nó thành đối tượng thời gian chuẩn
		if dur, err := time.ParseDuration(value); err == nil {
			return dur
		}
	}
	return fallback // k dịch được thì lấy backup
}
