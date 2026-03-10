package repository

import (
	"chat-notify-system/internal/db"
	"chat-notify-system/internal/models"
	"context"
	"fmt"
	"log"
	"time"
)

// hàm GetActiveTokensBatch gom nhóm nhiều bộ lọc: Lọc Online, Lọc Mute, và truy xuất Token từ Cache/DB.
// Đầu vào: Context, kết nối DB, và mảng danh sách những người trong nhóm chat.
// Đầu ra: Một Map (từ điển) với Key là UserID và Value là mảng các Device Token hợp lệ của User đó.
func GetActiveTokensBatch(ctx context.Context, database *db.Database, userIDs []int64) (map[int64][]string, error) {
	// Khởi tạo map rỗng để chuẩn bị chứa kết quả trả về
	result := make(map[int64][]string)
	if len(userIDs) == 0 {
		return result, nil // Trả về rỗng nếu danh sách truyền vào trống
	}

	// ==========================================
	// BƯỚC 1: BỘ LỌC TRẠNG THÁI ONLINE/OFFLINE
	// ==========================================
	var offlineUserIDs []int64
	for _, uid := range userIDs {
		// Tạo biến key giống hệt với cách API Gateway đã lưu khi user cắm WebSocket
		onlineKey := fmt.Sprintf("online:%d", uid)

		// Dùng lệnh Exists của Redis để kiểm tra siêu tốc xem key này có tồn tại không
		isOnline, _ := database.Redis.Exists(ctx, onlineKey).Result()

		if isOnline > 0 {
			// Nếu có tồn tại (nghĩa là user đang mở app và đang xem màn hình), ta không cần bắn Push Notification làm phiền họ.
			// App Flutter sẽ tự động hiển thị tin nhắn nhờ WebSocket.
			log.Printf("⏩ Bỏ qua Push FCM: User %d đang Online trên App", uid)
		} else {
			// Nếu không có, gom user này vào danh sách "Những người đang tắt màn hình"
			offlineUserIDs = append(offlineUserIDs, uid)
		}
	}

	// Nếu lặp xong mà tất cả mọi người đều đang online, thì kết thúc hàm sớm (Early Return) để tiết kiệm tài nguyên
	if len(offlineUserIDs) == 0 {
		log.Printf("Bộ lọc: Tất cả người nhận đều đang Online. Hủy lệnh gọi FCM để tránh làm phiền!")
		return result, nil
	}

	// ==========================================
	// BƯỚC 2: BỘ LỌC CẤU HÌNH DO NGƯỜI DÙNG CÀI ĐẶT (MUTE/BLOCK)
	// ==========================================
	var settings []models.UserNotificationSetting
	// Truy vấn một lần duy nhất (Batch query) vào bảng cấu hình của tất cả những user đang offline
	database.PG.WithContext(ctx).Where("user_id IN ?", offlineUserIDs).Find(&settings)

	// Tạo một map để đánh dấu những user không muốn nhận thông báo
	mutedUsers := make(map[int64]bool)
	now := time.Now()

	for _, s := range settings {
		// Kiểm tra 2 điều kiện:
		// 1. Nếu hạn Mute (MuteUntil) vẫn còn nằm ở tương lai (After now).
		// 2. Hoặc user cố tình tắt hẳn tính năng Chat (AllowChat = false).
		if (s.MuteUntil != nil && s.MuteUntil.After(now)) || !s.AllowChat {
			mutedUsers[s.UserID] = true // Đánh dấu vào danh sách đen
			log.Printf(" Bỏ qua Push FCM: User %d đang tắt thông báo", s.UserID)
		}
	}

	// Lọc ra danh sách cuối cùng: Những người đang offline VÀ không bị Mute
	var finalTargetIDs []int64
	for _, uid := range offlineUserIDs {
		if !mutedUsers[uid] {
			finalTargetIDs = append(finalTargetIDs, uid)
		}
	}

	// Nếu sau khi qua bộ lọc Mute mà danh sách trống trơn thì cũng hủy luôn lệnh gọi FCM
	if len(finalTargetIDs) == 0 {
		log.Printf("Bộ lọc: Tất cả người nhận (offline) đều đã bật chế độ Không Làm Phiền (Mute). Hủy lệnh gọi FCM!")
		return result, nil
	}

	// ==========================================
	// BƯỚC 3: TRUY XUẤT DEVICE TOKEN (TỪ REDIS VÀ POSTGRESQL)
	// ==========================================
	var missIDs []int64 // Danh sách những user không tìm thấy token trong cache (Cache Miss)

	// Vòng lặp lấy Token siêu tốc từ RAM (Redis)
	for _, uid := range finalTargetIDs {
		cacheKey := fmt.Sprintf("user_tokens:%d", uid)
		// Redis SMembers: Lấy toàn bộ các phần tử trong một tập hợp (Set)
		tokens, err := database.Redis.SMembers(ctx, cacheKey).Result()

		if err == nil && len(tokens) > 0 {
			// Caching Hit: Tìm thấy trong RAM -> Bốc ra xài luôn
			result[uid] = tokens
		} else {
			// Caching Miss: Không thấy trong RAM -> Gom lại để xíu nữa xuống DB query 1 lượt
			missIDs = append(missIDs, uid)
		}
	}

	// Xử lý các trường hợp Cache Miss
	if len(missIDs) > 0 {
		var devices []models.Device
		// Lệnh SQL gom: SELECT * FROM devices WHERE user_id IN (1, 2, 3) AND is_active = true
		// Chỉ lấy những token của thiết bị còn đang hoạt động
		if err := database.PG.WithContext(ctx).Where("user_id IN ? AND is_active = ?", missIDs, true).Find(&devices).Error; err != nil {
			return nil, fmt.Errorf("query devices: %w", err)
		}

		// Nhóm các token tìm được vào một map theo UserID (vì 1 user có thể dùng cả đt Android và iPhone cùng lúc)
		userDevices := make(map[int64][]string)
		for _, d := range devices {
			userDevices[d.UserID] = append(userDevices[d.UserID], d.DeviceToken)
		}

		// Đổ dữ liệu từ DB vào Map kết quả, đồng thời Lưu lên Redis (Cache) để những lần sau đọc cho nhanh
		for uid, tokens := range userDevices {
			cacheKey := fmt.Sprintf("user_tokens:%d", uid)

			// Kỹ thuật Redis Pipeline: Đóng gói nhiều lệnh Redis lại và gửi đi 1 lần để tối ưu độ trễ mạng
			pipe := database.Redis.Pipeline()
			// SAdd: Thêm danh sách token vào cấu trúc Set của Redis
			pipe.SAdd(ctx, cacheKey, tokens)
			// Expire: Set thời gian hết hạn của cache này là 1 giờ (tránh rác RAM)
			pipe.Expire(ctx, cacheKey, 1*time.Hour)

			// Thực thi pipeline
			if _, err := pipe.Exec(ctx); err != nil {
				log.Printf("Lỗi cache tokens cho user %d: %v", uid, err)
			}

			// Cập nhật kết quả cuối cùng để trả về
			result[uid] = tokens
		}
	}

	return result, nil // Trả về bộ danh sách Token đã được lọc và tổng hợp kỹ càng
}

// InvalidateInvalidTokens là hàm để vô hiệu hóa (tắt) các Device Token rác mà Google FCM báo về là không còn hợp lệ (do user gỡ app).
// Việc này giúp DB và Redis luôn sạch sẽ, không bị gửi thông báo nhầm chỗ.
func InvalidateInvalidTokens(ctx context.Context, database *db.Database, invalidTokens []string) error {
	if len(invalidTokens) == 0 {
		return nil
	}

	// Truy vấn ngược lại DB để xem những cái Token hỏng này là của User nào
	// Tại sao phải làm vậy? Vì ta cần biết UserID để có thể xóa Cache trên Redis (key Redis được thiết kế theo UserID)
	var devices []models.Device
	if err := database.PG.WithContext(ctx).
		Where("device_token IN ?", invalidTokens).
		Find(&devices).Error; err != nil {
		return fmt.Errorf("find devices by tokens: %w", err)
	}

	// Tiến hành Update vào Postgres: Đánh dấu tất cả Token hỏng thành is_active = false
	// Hàm Model() chỉ định bảng cần update
	if err := database.PG.WithContext(ctx).
		Model(&models.Device{}).
		Where("device_token IN ?", invalidTokens).
		Update("is_active", false).Error; err != nil {
		return fmt.Errorf("update devices inactive: %w", err)
	}

	// Xóa Cache (Invalidate Cache) trên Redis cho những User bị ảnh hưởng
	// Nếu không xóa, hàm GetActiveTokensBatch ở trên vẫn sẽ moi Token hỏng từ RAM ra xài
	for _, d := range devices {
		cacheKey := fmt.Sprintf("user_tokens:%d", d.UserID)

		// Lệnh Del: Xóa trắng cái key này khỏi RAM
		if err := database.Redis.Del(ctx, cacheKey).Err(); err != nil {
			log.Printf("Lỗi xoá cache cho user %d: %v", d.UserID, err)
		}
	}

	return nil
}
