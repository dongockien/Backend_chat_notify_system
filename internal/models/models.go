package models

import "time"

// Message đại diện cho bảng 'messages' trong cơ sở dữ liệu.
// Đây là nơi lưu trữ nội dung tin nhắn chat của người dùng.
type Message struct {
	// Khóa chính (Primary Key) của bảng, tự động tăng giá trị khi có bản ghi mới.
	ID int64 `gorm:"primaryKey;autoIncrement"`

	// Tạo chỉ mục (Index) cho cột SenderID để tăng tốc độ truy vấn khi cần tìm "các tin nhắn do user A gửi".
	SenderID int64 `gorm:"index"`

	// Tạo chỉ mục (Index) cho cột ConversationID để tăng tốc độ truy vấn "tải lịch sử tin nhắn của phòng chat B".
	ConversationID int64 `gorm:"index"`

	// Ghi định dạng cột này là kiểu 'text' trong Postgres để lưu trữ được đoạn văn bản dài (thay vì varchar mặc định).
	Content string `gorm:"type:text"`

	// Cột lưu thời gian tạo. Default:now() dặn Database tự động lấy giờ hệ thống điền vào khi có lệnh Insert.
	CreatedAt time.Time `gorm:"default:now()"`
}

// Device đại diện cho bảng 'devices', chuyên lưu trữ FCM Token của thiết bị người dùng để phục vụ việc gửi Push Notification.
type Device struct {
	ID int64 `gorm:"primaryKey;autoIncrement"`

	// uniqueIndex:idx_user_platform: Tạo một chỉ mục duy nhất (Unique Index) gom nhóm 2 cột UserID và Platform lại với nhau.
	// Tức là một UserID trên cùng một Platform (ví dụ: android) chỉ được phép có 1 bản ghi duy nhất.
	UserID int64 `gorm:"uniqueIndex:idx_user_platform"`

	// Token không được phép để trống (not null) và không được trùng nhau trên toàn bộ hệ thống (uniqueIndex).
	DeviceToken string `gorm:"uniqueIndex;not null"`

	// Giới hạn độ dài chuỗi là 20 ký tự (vd: "ios", "android", "web"). Nằm chung Unique Index với UserID ở trên.
	Platform string `gorm:"uniqueIndex:idx_user_platform;size:20"`

	// Cờ đánh dấu thiết bị này có còn đang hoạt động không (true/false).
	IsActive  bool      `gorm:"default:true"`
	UpdatedAt time.Time `gorm:"default:now()"`
}

// MessageSentEvent KHÔNG PHẢI LÀ BẢNG TRONG DATABASE (vì nó không có thẻ gorm).
// Đây là một DTO (Data Transfer Object) - Cấu trúc dùng để đóng gói dữ liệu gom lại thành 1 cục,
// sau đó chuyển thành chuỗi JSON (thông qua thẻ `json:"..."`) để đẩy vào bảng Outbox và gửi lên Kafka.
type MessageSentEvent struct {
	EventType      string  `json:"event_type"`      // Loại sự kiện (vd: "message.sent")
	MessageID      int64   `json:"message_id"`      // ID của tin nhắn vừa được lưu
	SenderID       int64   `json:"sender_id"`       // Người gửi
	ConversationID int64   `json:"conversation_id"` // Gửi vào phòng nào
	ReceiverIDs    []int64 `json:"receiver_ids"`    // Danh sách những người cần nhận Push (Mảng int64)
	Content        string  `json:"content"`         // Nội dung tóm tắt để hiện lên màn hình điện thoại
}

// Outbox đại diện cho bảng 'outboxes' - Trái tim của mô hình Transactional Outbox.
// Nó đóng vai trò là "trạm trung chuyển", giữ lại các sự kiện chưa được gửi lên Kafka do nghẽn mạng.
type Outbox struct {
	ID        int64  `json:"id" gorm:"primaryKey"`
	EventType string `json:"event_type"` // Tên loại sự kiện

	// Payload lưu trữ toàn bộ dữ liệu của struct MessageSentEvent ở trên dưới dạng chuỗi JSON thô.
	Payload string `json:"payload"`

	// Trạng thái của sự kiện: 'pending' (chờ gửi), 'processed' (đã gửi thành công lên Kafka), 'failed' (lỗi rác).
	Status string `json:"status" gorm:"default:'pending'"`

	// Đếm số lần hệ thống đã thử gửi lại sự kiện này lên Kafka nếu gặp lỗi mạng.
	RetryCount int `json:"retry_count" gorm:"default:0"`

	// autoCreateTime của Gorm sẽ tự động gán thời gian hiện tại khi bản ghi được tạo bằng code Go.
	CreatedAt time.Time `json:"created_at" gorm:"autoCreateTime"`
}

// UserNotificationSetting đại diện cho bảng cấu hình thông báo cá nhân của từng người dùng.
type UserNotificationSetting struct {
	// Gắn PrimaryKey thẳng vào UserID, nghĩa là bảng này có quan hệ 1-1 với bảng Users (1 user chỉ có 1 dòng config).
	UserID int64 `json:"user_id" gorm:"primaryKey"`

	// Cho phép nhận thông báo chat hay không (nếu false thì hệ thống hoàn toàn không gọi FCM).
	AllowChat bool `json:"allow_chat" gorm:"default:true"`

	// Cột lưu thời gian người dùng muốn "Mute" (Tắt thông báo tạm thời).
	// Dùng con trỏ (*time.Time) để cho phép cột này mang giá trị NULL (nghĩa là không Mute).
	MuteUntil *time.Time `json:"mute_until"`
	UpdatedAt time.Time  `json:"updated_at" gorm:"default:now()"`
}

// User đại diện cho bảng 'users' lưu thông tin cơ bản của người dùng.
type User struct {
	ID   int64  `json:"id" gorm:"primaryKey;autoIncrement"`
	Name string `json:"name" gorm:"size:100"` // Giới hạn tên tối đa 100 ký tự
}

// Conversation đại diện cho bảng 'conversations' lưu thông tin các phòng chat / nhóm chat.
type Conversation struct {
	ID   int64  `json:"id" gorm:"primaryKey;autoIncrement"`
	Name string `json:"name" gorm:"size:255"` // Tên nhóm chat
}

// NotificationLog đại diện cho bảng 'notification_logs' - Cuốn sổ cái (Audit Log).
// Nó ghi lại lịch sử mọi hành động của Consumer Worker để giúp team kỹ thuật debug khi "người dùng than phiền không nhận được thông báo".
type NotificationLog struct {
	ID int64 `json:"id" gorm:"primaryKey;autoIncrement"`

	// Gắn index để sau này dễ dàng query "Tin nhắn X đã gửi Push cho những ai?".
	MessageID int64 `json:"message_id" gorm:"index"`

	// Gắn index để query "User Y hôm qua nhận được những thông báo FCM nào?".
	UserID int64 `json:"user_id" gorm:"index"`

	// Trạng thái cuối cùng: "Success", "Failed", hoặc "Skipped".
	Status string `json:"status" gorm:"size:20"`

	// Cột text lưu chi tiết lý do (vd: lỗi mạng, báo lỗi token hết hạn, hay bị chặn do đang online socket).
	Reason    string    `json:"reason" gorm:"type:text"`
	CreatedAt time.Time `json:"created_at" gorm:"default:now()"`
}
