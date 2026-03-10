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
	// đọc các thông số cấu hình (link db, redis, port kafka...) từ biến môi trường hoặc file .env
	cfg := config.Load()

	// khởi tạo kết nối vào cơ sở dữ liệu postgres và cache redis
	// hàm trả về con trỏ database và lỗi err nếu có
	database, err := db.NewDatabase(cfg.PostgresDSN, cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB)
	if err != nil {
		log.Fatalf("Failed to init db: %v", err) // nếu lỗi thì in log và buộc dừng chương trình luôn (fatal)
	}
	// dặn go tự động đóng kết nối db khi hàm main() kết thúc
	defer database.Close()

	// chuẩn bị sẵn bộ công cụ gửi tin của kafka (producer), trỏ tới địa chỉ broker và topic cấu hình sẵn
	kafka.InitProducer(cfg.KafkaBrokers, cfg.KafkaTopic)

	// tạo một context để hứng tín hiệu từ hệ điều hành (ví dụ ấn ctrl+c hoặc bị docker kill)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop() // tự động dọn dẹp bộ lắng nghe tín hiệu này khi dừng

	// tạo bộ đếm thời gian (ticker), tự động nhịp mỗi chu kỳ (ví dụ 5 giây một lần)
	ticker := time.NewTicker(cfg.OutboxPollInterval)
	defer ticker.Stop() // dừng đếm nhịp khi worker bị tắt

	log.Println("Outbox worker đang chạy và quét database...")

	for { // vòng lặp vô hạn để giữ cho worker luôn sống và chạy nền
		select { // dùng select để lắng nghe cùng lúc nhiều sự kiện channel
		case <-ctx.Done(): // nếu nhận được tín hiệu yêu cầu tắt máy từ biến ctx
			log.Println("Outbox worker stopping...")
			return // thoát khỏi hàm main, kết thúc tiến trình an toàn
		case <-ticker.C: // nếu bộ đếm thời gian báo đến lúc chạy (hết chu kỳ 5 giây)

			// gọi hàm xử lý lấy dữ liệu và đẩy đi. truyền context và đối tượng db vào
			if err := processOutbox(ctx, database.PG); err != nil {
				log.Printf(" Lỗi processOutbox: %v", err) // nếu quá trình xử lý có lỗi thì in ra log, không làm chết worker
			}
		}
	}
}

// hàm lõi thực hiện quét bảng outbox và đưa tin nhắn lên kafka
func processOutbox(ctx context.Context, db *gorm.DB) error {
	var events []models.Outbox // tạo mảng rỗng để hứng danh sách các bản ghi outbox từ database

	tx := db.Begin() // khởi tạo một giao dịch (transaction) mới để đảm bảo tính toàn vẹn dữ liệu
	if tx.Error != nil {
		return tx.Error // nếu không thể tạo transaction thì trả về lỗi luôn
	}

	// câu truy vấn quan trọng nhất: lấy tối đa 50 bản ghi cũ nhất (cần gửi trước) đang ở trạng thái pending
	// FOR UPDATE SKIP LOCKED giúp khóa các dòng đang đọc lại, nếu chạy nhiều worker cùng lúc thì worker khác sẽ tự động bỏ qua các dòng đang bị khóa, tránh gửi trùng tin nhắn
	tx.Raw("SELECT * FROM outboxes WHERE status = 'pending' ORDER BY created_at ASC LIMIT 50 FOR UPDATE SKIP LOCKED").Scan(&events)

	if len(events) == 0 { // kiểm tra nếu mảng rỗng (không có tin nhắn nào cần gửi)
		tx.Rollback() // hủy giao dịch hiện tại cho nhẹ database
		return nil    // thoát hàm sớm mà không có lỗi
	}

	for _, event := range events { // lặp qua từng bản ghi outbox vừa truy vấn được

		var msgEvent models.MessageSentEvent // tạo biến struct rỗng để chứa dữ liệu sự kiện gửi tin

		// chuỗi payload trong db đang lưu dưới dạng json, hàm unmarshal sẽ giải mã chuỗi này và đắp dữ liệu vào biến msgEvent
		if err := json.Unmarshal([]byte(event.Payload), &msgEvent); err != nil {
			log.Printf(" Lỗi parse JSON Outbox ID %d: %v", event.ID, err)
			// nếu json bị lỗi không giải mã được, đánh dấu bản ghi này là failed để các chu kỳ sau không lôi ra gửi nữa
			tx.Exec("UPDATE outboxes SET status = 'failed' WHERE id = ?", event.ID)
			continue // bỏ qua bản ghi lỗi này và xử lý tiếp bản ghi tiếp theo trong mảng
		}

		// gọi hàm PublishEvent bên package kafka để đẩy cục sự kiện msgEvent lên topic của kafka
		err := kafka.PublishEvent(ctx, msgEvent)

		if err == nil {
			// nếu đẩy lên kafka thành công (không có lỗi), đánh dấu trạng thái bản ghi trong db là processed (đã xử lý xong)
			tx.Exec("UPDATE outboxes SET status = 'processed' WHERE id = ?", event.ID)
		} else {
			// nếu đẩy thất bại (ví dụ kafka sập), cộng số lần thử lại (retry_count) lên 1.
			// bản ghi vẫn giữ trạng thái pending để lần quét sau tiếp tục lấy ra gửi lại
			tx.Exec("UPDATE outboxes SET retry_count = retry_count + 1 WHERE id = ?", event.ID)
		}
	}

	return tx.Commit().Error // chốt giao dịch, lúc này toàn bộ các lệnh UPDATE trạng thái bên trên mới thực sự được lưu vào database
}
