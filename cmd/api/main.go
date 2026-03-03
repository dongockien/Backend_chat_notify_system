package main

import (
	"chat-notify-system/internal/config"
	"chat-notify-system/internal/db"
	"chat-notify-system/internal/kafka"
	"chat-notify-system/internal/middleware"
	"chat-notify-system/internal/models"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

var clients = make(map[*websocket.Conn]bool)
var broadcast = make(chan string)

func handleMessages() {
	for {
		msg := <-broadcast
		for client := range clients {
			err := client.WriteMessage(websocket.TextMessage, []byte(msg))
			if err != nil {
				log.Printf("Lỗi socket: %v", err)
				client.Close()
				delete(clients, client)
			}
		}
	}
}

func main() {
	cfg := config.Load()

	database, err := db.NewDatabase(cfg.PostgresDSN, cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB)
	if err != nil {
		log.Fatalf("Failed to init db: %v", err)
	}
	defer database.Close()

	kafka.InitProducer(cfg.KafkaBrokers, cfg.KafkaTopic)

	go handleMessages()

	r := gin.Default()
	corsConfig := cors.DefaultConfig()
	corsConfig.AllowAllOrigins = true
	corsConfig.AllowHeaders = []string{"Origin", "Content-Length", "Content-Type", "Authorization"}
	r.Use(cors.New(corsConfig))

	r.POST("/api/login", func(c *gin.Context) {

		name := c.Query("name")
		if name == "" {
			name = "Khách Vô Danh"
		}

		var user models.User
		if err := database.PG.Where("name = ?", name).FirstOrCreate(&user, models.User{Name: name}).Error; err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Lỗi Database"})
			return
		}

		database.PG.Exec(`INSERT INTO conversation_members (conversation_id, user_id) VALUES (99, ?) ON CONFLICT DO NOTHING`, user.ID)

		token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
			"user_id": float64(user.ID),
			"exp":     time.Now().Add(time.Hour * 24).Unix(),
		})
		tokenString, _ := token.SignedString(middleware.JwtSecret)

		c.JSON(http.StatusOK, gin.H{
			"access_token": tokenString,
			"user_id":      user.ID,
			"name":         user.Name,
		})
	})

	// Cổng WebSocket
	r.GET("/ws", func(c *gin.Context) {
		tokenString := c.Query("token")
		token, _ := jwt.Parse(tokenString, func(t *jwt.Token) (interface{}, error) {
			return middleware.JwtSecret, nil
		})

		var userID int64
		if claims, ok := token.Claims.(jwt.MapClaims); ok && token.Valid {
			userID = int64(claims["user_id"].(float64))
		} else {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Từ chối kết nối WebSocket: Thiếu JWT"})
			return
		}

		ws, err := upgrader.Upgrade(c.Writer, c.Request, nil)
		if err != nil {
			log.Printf("Lỗi upgrade websocket: %v", err)
			return
		}

		clients[ws] = true

		onlineKey := fmt.Sprintf("online:%d", userID)

		if err := database.Redis.Set(c.Request.Context(), onlineKey, "1", 24*time.Hour).Err(); err != nil {
			log.Printf("Lỗi set online key: %v", err)
		} else {
			log.Printf("🟢 User %d vừa Online qua Socket!", userID)
		}

		go func() {
			defer func() {
				ws.Close()
				delete(clients, ws)

				delCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()

				if err := database.Redis.Del(delCtx, onlineKey).Err(); err != nil {
					log.Printf("Lỗi xóa online key: %v", err)
				} else {
					log.Printf("🔴 User %d đã Offline", userID)
				}
			}()

			for {
				if _, _, err := ws.ReadMessage(); err != nil {
					break
				}
			}
		}()
	})

	protected := r.Group("/")
	protected.Use(middleware.RequireJWT())

	protected.POST("/api/devices", func(c *gin.Context) {
		var req struct {
			DeviceToken string `json:"device_token"`
			Platform    string `json:"platform"`
		}
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
			return
		}

		userID := c.MustGet("user_id").(int64)
		sql := `INSERT INTO devices (user_id, device_token, platform, is_active, updated_at)
                VALUES (?, ?, ?, true, NOW())
                ON CONFLICT (user_id, platform)
                DO UPDATE SET device_token = EXCLUDED.device_token, is_active = true, updated_at = NOW();`

		if err := database.PG.Exec(sql, userID, req.DeviceToken, req.Platform).Error; err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save device"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "Đã lưu FCM Token"})
	})

	
	protected.POST("/api/members/add", func(c *gin.Context) {
		userID := c.MustGet("user_id").(int64)
		newMemberName := c.DefaultQuery("name", "Thành viên mới")

		var inviter models.User
		database.PG.First(&inviter, userID)

		var newUser models.User
		database.PG.Where("name = ?", newMemberName).FirstOrCreate(&newUser, models.User{Name: newMemberName})

		database.PG.Exec(`INSERT INTO conversation_members (conversation_id, user_id) VALUES (99, ?) ON CONFLICT DO NOTHING`, newUser.ID)

		sysPayload, _ := json.Marshal(map[string]interface{}{
			"type":            "system_alert",
			"conversation_id": 99,
			"content":         fmt.Sprintf("Hệ thống: %s vừa thêm %s vào nhóm!", inviter.Name, newUser.Name),
		})
		broadcast <- string(sysPayload)
		c.JSON(http.StatusOK, gin.H{"status": "Đã thêm thành viên thật vào DB"})
	})


	protected.GET("/api/messages", func(c *gin.Context) {
		convIDStr := c.Query("conversation_id")
		convID, _ := strconv.ParseInt(convIDStr, 10, 64)

		type MessageResponse struct {
			SenderID       int64  `json:"sender_id"`
			SenderName     string `json:"sender_name"`
			ConversationID int64  `json:"conversation_id"`
			Content        string `json:"content"`
			Type           string `json:"type"`
		}
		var history []MessageResponse

		database.PG.Table("messages").
			Select("messages.sender_id, users.name as sender_name, messages.conversation_id, messages.content, 'chat' as type").
			Joins("LEFT JOIN users ON users.id = messages.sender_id").
			Where("messages.conversation_id = ?", convID).
			Order("messages.created_at ASC").
			Scan(&history)

		c.JSON(http.StatusOK, gin.H{"data": history})
	})

	protected.POST("/api/messages", func(c *gin.Context) {
		var req struct {
			ConversationID int64   `json:"conversation_id"`
			ReceiverIDs    []int64 `json:"receiver_ids"`
			Content        string  `json:"content"`
		}
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request body"})
			return
		}

		userID := c.MustGet("user_id").(int64)

		tx := database.PG.Begin()
		if tx.Error != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Cannot start transaction"})
			return
		}

		newMsg := models.Message{
			SenderID:       userID,
			ConversationID: req.ConversationID,
			Content:        req.Content,
		}
		if err := tx.Create(&newMsg).Error; err != nil {
			tx.Rollback()
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save message"})
			return
		}

		var receiverIDs []int64
		database.PG.Table("conversation_members").
			Where("conversation_id = ? AND user_id != ?", req.ConversationID, userID).
			Pluck("user_id", &receiverIDs)
		event := models.MessageSentEvent{
			EventType:      "message.sent",
			MessageID:      newMsg.ID,
			SenderID:       userID,
			ConversationID: req.ConversationID,
			ReceiverIDs:    receiverIDs,
			Content:        req.Content,
		}
		payload, _ := json.Marshal(event)

		outbox := models.Outbox{
			EventType: "message.sent",
			Payload:   string(payload),
		}
		if err := tx.Create(&outbox).Error; err != nil {
			tx.Rollback()
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save outbox"})
			return
		}
		tx.Commit()

		var sender models.User
		if err := database.PG.First(&sender, userID).Error; err != nil {
			sender.Name = fmt.Sprintf("User Ẩn Danh %d", userID)
		}

		var conv models.Conversation
		if err := database.PG.First(&conv, req.ConversationID).Error; err != nil {
			conv.Name = fmt.Sprintf("Phòng %d", req.ConversationID)
		}

		msgPayload, _ := json.Marshal(map[string]interface{}{
			"type":              "chat",
			"message_id":        newMsg.ID,
			"sender_id":         userID,
			"sender_name":       sender.Name,
			"conversation_id":   req.ConversationID,
			"conversation_name": conv.Name,
			"content":           req.Content,
		})
		broadcast <- string(msgPayload)

		c.JSON(http.StatusOK, gin.H{
			"status":     "success",
			"message_id": newMsg.ID,
		})
	})

	srv := &http.Server{
		Addr:    ":" + cfg.Port,
		Handler: r,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %s\n", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutdown Server ...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv.Shutdown(ctx)
}