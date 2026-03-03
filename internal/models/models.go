package models

import "time"

type Message struct {
	ID             int64     `gorm:"primaryKey;autoIncrement"`
	SenderID       int64     `gorm:"index"`
	ConversationID int64     `gorm:"index"`
	Content        string    `gorm:"type:text"`
	CreatedAt      time.Time `gorm:"default:now()"`
}

type Device struct {
	ID          int64     `gorm:"primaryKey;autoIncrement"`
	UserID      int64     `gorm:"uniqueIndex:idx_user_platform"` 
	DeviceToken string    `gorm:"uniqueIndex;not null"`
	Platform    string    `gorm:"uniqueIndex:idx_user_platform;size:20"` 
	IsActive    bool      `gorm:"default:true"`
	UpdatedAt   time.Time `gorm:"default:now()"`
}

type MessageSentEvent struct {
	EventType      string  `json:"event_type"`
	MessageID      int64   `json:"message_id"`
	SenderID       int64   `json:"sender_id"`
	ConversationID int64   `json:"conversation_id"`
	ReceiverIDs    []int64 `json:"receiver_ids"`
	Content        string  `json:"content"`
}

type Outbox struct {
	ID        int64     `json:"id" gorm:"primaryKey"`
	EventType string    `json:"event_type"`
	Payload   string    `json:"payload"`
	Status string		`json:"status" gorm:"default:'pending'"`
	RetryCount int       `json:"retry_count" gorm:"default:0"`    
	CreatedAt  time.Time `json:"created_at" gorm:"autoCreateTime"`
}

type UserNotificationSetting struct { 
	UserID    int64      `json:"user_id" gorm:"primaryKey"` 
	AllowChat bool       `json:"allow_chat" gorm:"default:true"`
	MuteUntil *time.Time `json:"mute_until"`
	UpdatedAt time.Time  `json:"updated_at" gorm:"default:now()"` 
}

type User struct {
	ID   int64  `json:"id" gorm:"primaryKey;autoIncrement"`
	Name string `json:"name" gorm:"size:100"`
}

type Conversation struct {
	ID   int64  `json:"id" gorm:"primaryKey;autoIncrement"`
	Name string `json:"name" gorm:"size:255"`
}

// Bảng lưu trữ lịch sử gửi Thông báo (Phase 3)
type NotificationLog struct {
	ID        int64     `json:"id" gorm:"primaryKey;autoIncrement"`
	MessageID int64     `json:"message_id" gorm:"index"`
	UserID    int64     `json:"user_id" gorm:"index"`
	Status    string    `json:"status" gorm:"size:20"`   // SUCCESS, FAILED, SKIPPED
	Reason    string    `json:"reason" gorm:"type:text"` // Lý do chi tiết
	CreatedAt time.Time `json:"created_at" gorm:"default:now()"`
}