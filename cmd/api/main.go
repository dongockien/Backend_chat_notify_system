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

// upgrader: Biến cấu hình để nâng cấp request HTTP thông thường thành kết nối WebSocket (hoạt động hai chiều).
// CheckOrigin: Bỏ qua kiểm tra nguồn gốc (CORS), cho phép các client từ mọi domain đều có thể kết nối tới socket.
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// clients: Một map lưu danh sách người dùng đang kết nối WebSocket.
// Key là con trỏ kết nối (*websocket.Conn) để định danh client, Value là trạng thái boolean.
var clients = make(map[*websocket.Conn]bool)

// broadcast: Một channel kiểu chuỗi (string). Nó hoạt động như một đường ống thông báo.
// Bất kỳ luồng (goroutine) nào đẩy dữ liệu vào đây, hàm lắng nghe sẽ lấy ra để gửi cho tất cả clients.
var broadcast = make(chan string)

// handleMessages: Hàm chạy ngầm có nhiệm vụ phân phối tin nhắn WebSocket.
func handleMessages() {
	for { // Vòng lặp vô hạn để liên tục lắng nghe.
		msg := <-broadcast // Luồng sẽ tạm dừng ở đây để đợi. Khi có dữ liệu trong channel broadcast, nó gán vào biến msg.

		for client := range clients { // Duyệt qua tất cả các client đang online trong map.
			err := client.WriteMessage(websocket.TextMessage, []byte(msg)) // Gửi tin nhắn văn bản (chuyển đổi từ string sang mảng byte) xuống client.
			if err != nil {                                                // Nếu gửi lỗi (do client mất mạng, tắt app ngang...).
				log.Printf("Lỗi socket: %v", err) // Ghi log lỗi.
				client.Close()                    // Đóng kết nối vật lý của client này để giải phóng tài nguyên.
				delete(clients, client)           // Xóa client khỏi danh sách quản lý.
			}
		}
	}
}

func main() {
	cfg := config.Load() // Tải các cấu hình môi trường từ file .env vào struct cfg.

	// Khởi tạo kết nối tới PostgreSQL và Redis thông qua hàm NewDatabase.
	database, err := db.NewDatabase(cfg.PostgresDSN, cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB)
	if err != nil {
		log.Fatalf("Failed to init db: %v", err) // Nếu không kết nối được DB, thoát chương trình ngay lập tức (Fatal).
	}
	defer database.Close() // defer dặn Golang rằng: Hãy gọi hàm Close() này trước khi hàm main() kết thúc để giải phóng kết nối DB.

	kafka.InitProducer(cfg.KafkaBrokers, cfg.KafkaTopic) // Khởi tạo Kafka producer để chuẩn bị cho việc gửi sự kiện message.sent.

	go handleMessages() // Từ khóa 'go' khởi tạo một goroutine mới. Hàm handleMessages sẽ chạy song song, không chặn quá trình chạy của server HTTP bên dưới.

	r := gin.Default() // Khởi tạo router mặc định của framework Gin (đã kèm sẵn middleware log và xử lý lỗi panic).

	// Cấu hình CORS để cho phép ứng dụng frontend (chạy ở port/domain khác) gọi API mà không bị trình duyệt chặn.
	corsConfig := cors.DefaultConfig()
	corsConfig.AllowAllOrigins = true                                                               // Chấp nhận request từ mọi nguồn.
	corsConfig.AllowHeaders = []string{"Origin", "Content-Length", "Content-Type", "Authorization"} // Cho phép các header cần thiết (đặc biệt là Authorization chứa token).
	r.Use(cors.New(corsConfig))                                                                     // Áp dụng cấu hình CORS vào toàn bộ API của Gin.

	// API 1: Đăng nhập (Mô phỏng quy trình cấp phát JWT)
	r.POST("/api/login", func(c *gin.Context) {
		name := c.Query("name") // Lấy tham số 'name' từ URL (vd: /api/login?name=Kien).
		if name == "" {
			name = "Khách Vô Danh" // Nếu không truyền tên, sử dụng tên mặc định.
		}

		var user models.User // Khởi tạo biến user rỗng dựa trên struct models.User.

		// GORM query: Tìm user theo tên. Nếu không tìm thấy, tự động tạo một user mới (FirstOrCreate) và gán thông tin vào biến con trỏ &user.
		if err := database.PG.Where("name = ?", name).FirstOrCreate(&user, models.User{Name: name}).Error; err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Lỗi Database"})
			return // Thoát sớm nếu có lỗi.
		}

		// Tự động thêm user vào phòng chat số 99. 'ON CONFLICT DO NOTHING' đảm bảo nếu user đã có trong phòng rồi thì sẽ không báo lỗi.
		database.PG.Exec(`INSERT INTO conversation_members (conversation_id, user_id) VALUES (99, ?) ON CONFLICT DO NOTHING`, user.ID)

		// Tạo JWT Token. jwt.MapClaims là payload của token chứa thông tin nhận diện người dùng.
		token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
			"user_id": float64(user.ID),                      // Lưu ID của user vào token.
			"exp":     time.Now().Add(time.Hour * 24).Unix(), // Thiết lập thời gian hết hạn (expiration) là 24 giờ.
		})

		tokenString, _ := token.SignedString(middleware.JwtSecret) // Sử dụng secret key để ký điện tử, tạo ra chuỗi token cuối cùng.

		// Trả chuỗi token và thông tin user về cho client bằng JSON.
		c.JSON(http.StatusOK, gin.H{
			"access_token": tokenString,
			"user_id":      user.ID,
			"name":         user.Name,
		})
	})

	// API 2: Mở kết nối WebSocket
	r.GET("/ws", func(c *gin.Context) {
		tokenString := c.Query("token") // Lấy JWT token truyền qua tham số URL khi client yêu cầu mở socket.

		// Phân tích và giải mã token bằng secret key.
		token, _ := jwt.Parse(tokenString, func(t *jwt.Token) (interface{}, error) {
			return middleware.JwtSecret, nil
		})

		var userID int64
		// Kiểm tra token có hợp lệ không. Kỹ thuật type assertion .(jwt.MapClaims) được dùng để ép kiểu và trích xuất dữ liệu payload.
		if claims, ok := token.Claims.(jwt.MapClaims); ok && token.Valid {
			userID = int64(claims["user_id"].(float64)) // Ép kiểu float64 từ JSON sang int64 để dùng.
		} else {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Từ chối kết nối WebSocket: Thiếu JWT"})
			return // Từ chối kết nối nếu token sai hoặc hết hạn.
		}

		// Nâng cấp request HTTP hiện tại thành kết nối WebSocket song phương.
		ws, err := upgrader.Upgrade(c.Writer, c.Request, nil)
		if err != nil {
			log.Printf("Lỗi upgrade websocket: %v", err)
			return
		}

		clients[ws] = true // Lưu kết nối thành công vào danh sách quản lý chung.

		onlineKey := fmt.Sprintf("online:%d", userID) // Định dạng key trên Redis, ví dụ: "online:15".

		// Lưu cờ đánh dấu user đang online vào Redis. Consumer Worker sẽ đọc cờ này để quyết định có gửi Push Notification hay không.
		if err := database.Redis.Set(c.Request.Context(), onlineKey, "1", 24*time.Hour).Err(); err != nil {
			log.Printf("Lỗi set online key: %v", err)
		} else {
			log.Printf("🟢 User %d vừa Online qua Socket!", userID)
		}

		// Tạo một goroutine riêng phục vụ vòng đời của kết nối websocket này.
		go func() {
			defer func() { // Khối lệnh defer này sẽ được thực thi khi user ngắt kết nối và thoát khỏi vòng lặp đọc bên dưới.
				ws.Close()          // Đóng kết nối mạng.
				delete(clients, ws) // Xóa kết nối khỏi map để giải phóng bộ nhớ.

				// Tạo một context với thời gian chờ 2 giây để đảm bảo thao tác xóa Redis không bị treo vĩnh viễn.
				delCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()

				// Xóa cờ online trên Redis. Thao tác này cực kỳ quan trọng để hệ thống biết user đã offline và bắt đầu gửi Push Notification.
				if err := database.Redis.Del(delCtx, onlineKey).Err(); err != nil {
					log.Printf("Lỗi xóa online key: %v", err)
				} else {
					log.Printf("🔴 User %d đã Offline", userID)
				}
			}()

			for { // Vòng lặp chờ đọc dữ liệu từ client.
				if _, _, err := ws.ReadMessage(); err != nil { // Đọc tin nhắn. Nếu có lỗi (như user đóng app).
					break // Thoát vòng lặp, lúc này khối defer phía trên sẽ tự động được gọi.
				}
			}
		}()
	})

	// Phân nhóm router. Các API trong biến 'protected' sẽ yêu cầu xác thực.
	protected := r.Group("/")
	protected.Use(middleware.RequireJWT()) // Áp dụng middleware xác thực. Chỉ request có token hợp lệ ở header mới được đi qua.

	// API 3: Lưu Device Token của thiết bị (để gửi FCM)
	protected.POST("/api/devices", func(c *gin.Context) {
		var req struct { // Khai báo một struct ẩn danh (anonymous struct) định hình dữ liệu mong đợi từ request.
			DeviceToken string `json:"device_token"` // Thẻ `json:"..."` giúp ánh xạ key từ body JSON vào trường tương ứng của struct.
			Platform    string `json:"platform"`
		}

		// c.ShouldBindJSON dùng để đọc body JSON và ép dữ liệu vào biến req thông qua con trỏ &req.
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
			return
		}

		userID := c.MustGet("user_id").(int64) // Lấy user_id mà middleware đã xác thực và gán sẵn vào context.

		// Cú pháp Upsert (Update or Insert) của PostgreSQL:
		// Cố gắng tạo bản ghi mới. Nếu user_id và platform đã tồn tại (ON CONFLICT), tiến hành cập nhật token và thời gian.
		sql := `INSERT INTO devices (user_id, device_token, platform, is_active, updated_at)
                VALUES (?, ?, ?, true, NOW())
                ON CONFLICT (user_id, platform)
                DO UPDATE SET device_token = EXCLUDED.device_token, is_active = true, updated_at = NOW();`

		if err := database.PG.Exec(sql, userID, req.DeviceToken, req.Platform).Error; err != nil { // Thực thi truy vấn.
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save device"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "Đã lưu FCM Token"})
	})

	// API 4: Thêm người dùng vào nhóm
	protected.POST("/api/members/add", func(c *gin.Context) {
		userID := c.MustGet("user_id").(int64)                    // Lấy ID của người gửi yêu cầu.
		newMemberName := c.DefaultQuery("name", "Thành viên mới") // Lấy tên người muốn add từ URL, gán giá trị mặc định nếu rỗng.

		var inviter models.User
		database.PG.First(&inviter, userID) // Lấy thông tin người mời từ DB.

		var newUser models.User
		database.PG.Where("name = ?", newMemberName).FirstOrCreate(&newUser, models.User{Name: newMemberName}) // Tìm hoặc tạo người được mời.

		// Ghi vào bảng trung gian. Bỏ qua nếu người này đã ở trong phòng.
		database.PG.Exec(`INSERT INTO conversation_members (conversation_id, user_id) VALUES (99, ?) ON CONFLICT DO NOTHING`, newUser.ID)

		// Đóng gói thông báo hệ thống và mã hóa thành mảng byte json.
		sysPayload, _ := json.Marshal(map[string]interface{}{
			"type":            "system_alert",
			"conversation_id": 99,
			"content":         fmt.Sprintf("Hệ thống: %s vừa thêm %s vào nhóm!", inviter.Name, newUser.Name),
		})
		broadcast <- string(sysPayload) // Truyền thông báo vào channel websocket để báo cho mọi người đang online.
		c.JSON(http.StatusOK, gin.H{"status": "Đã thêm thành viên thật vào DB"})
	})

	// API 5: Lấy lịch sử tin nhắn của một phòng
	protected.GET("/api/messages", func(c *gin.Context) {
		convIDStr := c.Query("conversation_id")
		convID, _ := strconv.ParseInt(convIDStr, 10, 64) // Chuyển đổi tham số conversation_id từ string sang int64.

		// Struct định dạng dữ liệu trả về cho client.
		type MessageResponse struct {
			SenderID       int64  `json:"sender_id"`
			SenderName     string `json:"sender_name"`
			ConversationID int64  `json:"conversation_id"`
			Content        string `json:"content"`
			Type           string `json:"type"`
		}
		var history []MessageResponse // Tạo mảng rỗng để chứa kết quả.

		// Truy vấn bảng messages, join với bảng users để lấy tên hiển thị của người gửi, lọc theo phòng và sắp xếp tăng dần theo thời gian.
		database.PG.Table("messages").
			Select("messages.sender_id, users.name as sender_name, messages.conversation_id, messages.content, 'chat' as type").
			Joins("LEFT JOIN users ON users.id = messages.sender_id").
			Where("messages.conversation_id = ?", convID).
			Order("messages.created_at ASC").
			Scan(&history) // Thực thi truy vấn và đổ dữ liệu vào mảng history thông qua con trỏ.

		c.JSON(http.StatusOK, gin.H{"data": history})
	})

	// API 6: LUỒNG CHÍNH - Xử lý gửi tin nhắn (áp dụng mô hình Transactional Outbox)
	protected.POST("/api/messages", func(c *gin.Context) {
		var req struct { // Struct để nhận dữ liệu body request.
			ConversationID int64   `json:"conversation_id"`
			ReceiverIDs    []int64 `json:"receiver_ids"` // Danh sách ID người nhận (client có thể truyền lên để test).
			Content        string  `json:"content"`
		}
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request body"})
			return
		}

		userID := c.MustGet("user_id").(int64) // Định danh người đang gửi tin nhắn.

		tx := database.PG.Begin() // Bắt đầu một Transaction. Mọi thao tác ghi sau đây cần thành công tất cả hoặc hủy bỏ tất cả (ACID).
		if tx.Error != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Cannot start transaction"})
			return
		}

		// Tạo đối tượng tin nhắn để lưu vào hệ thống.
		newMsg := models.Message{
			SenderID:       userID,
			ConversationID: req.ConversationID,
			Content:        req.Content,
		}

		if err := tx.Create(&newMsg).Error; err != nil { // Lưu tin nhắn chính vào database.
			tx.Rollback() // Nếu lỗi, hoàn tác transaction hiện tại.
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save message"})
			return
		}

		var receiverIDs []int64
		// Lấy danh sách thành viên cần thông báo.
		// Quan trọng: Sử dụng điều kiện 'user_id != userID' để loại trừ người đang gửi, không tự bắn thông báo cho chính mình.
		// Pluck là lệnh lấy duy nhất dữ liệu của một cột (user_id) và đưa vào mảng chỉ định.
		database.PG.Table("conversation_members").
			Where("conversation_id = ? AND user_id != ?", req.ConversationID, userID).
			Pluck("user_id", &receiverIDs)

		// Cấu trúc đối tượng Sự kiện (Event) để chuẩn bị đẩy cho hệ thống Kafka xử lý nền.
		event := models.MessageSentEvent{
			EventType:      "message.sent",
			MessageID:      newMsg.ID, // Lấy ID tin nhắn vừa được gán tự động từ DB.
			SenderID:       userID,
			ConversationID: req.ConversationID,
			ReceiverIDs:    receiverIDs, // Truyền danh sách cần gửi Push Notification sang cho Kafka.
			Content:        req.Content,
		}
		payload, _ := json.Marshal(event) // Mã hóa sự kiện từ dạng Object thành dạng chuỗi JSON chuẩn.

		// Tạo bản ghi lưu vào bảng outboxes (bảng trung chuyển).
		outbox := models.Outbox{
			EventType: "message.sent",
			Payload:   string(payload),
			// Trạng thái (status) thường được thiết lập là 'pending' mặc định trong database schema.
		}
		if err := tx.Create(&outbox).Error; err != nil { // Thực thi lưu sự kiện.
			tx.Rollback() // Nếu lưu sự kiện lỗi, hủy luôn cả thao tác lưu tin nhắn ở phía trên để dữ liệu đồng nhất.
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save outbox"})
			return
		}

		tx.Commit() // Hoàn tất Transaction: Ghi vĩnh viễn cả Message và Outbox vào ổ cứng.

		// Lấy thêm thông tin để chuẩn bị tin nhắn gửi qua Socket.
		var sender models.User
		if err := database.PG.First(&sender, userID).Error; err != nil {
			sender.Name = fmt.Sprintf("User Ẩn Danh %d", userID)
		}

		var conv models.Conversation
		if err := database.PG.First(&conv, req.ConversationID).Error; err != nil {
			conv.Name = fmt.Sprintf("Phòng %d", req.ConversationID)
		}

		// Tạo payload và chuyển thành JSON cho Socket.
		msgPayload, _ := json.Marshal(map[string]interface{}{
			"type":              "chat",
			"message_id":        newMsg.ID,
			"sender_id":         userID,
			"sender_name":       sender.Name,
			"conversation_id":   req.ConversationID,
			"conversation_name": conv.Name,
			"content":           req.Content,
		})

		broadcast <- string(msgPayload) // Đẩy payload vào channel broadcast. Luồng handleMessages() sẽ bắt và truyền đi tới client.

		// Cực kỳ quan trọng: Trả kết quả HTTP 200 OK ngay lập tức. Client không bị block đợi quá trình gửi Push Notification phức tạp phía sau.
		c.JSON(http.StatusOK, gin.H{
			"status":     "success",
			"message_id": newMsg.ID,
		})
	})

	// Cấu hình http.Server.
	srv := &http.Server{
		Addr:    ":" + cfg.Port, // Chạy trên port lấy từ cấu hình.
		Handler: r,              // Sử dụng bộ định tuyến Gin làm handler.
	}

	// Chạy server trên một goroutine riêng. Nếu không có 'go', luồng chạy sẽ chặn (block) tại lệnh ListenAndServe.
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %s\n", err)
		}
	}()

	// Cơ chế Graceful Shutdown (Dừng server an toàn)
	quit := make(chan os.Signal, 1)                      // Kênh nhận tín hiệu ngắt từ hệ điều hành.
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM) // Bắt tín hiệu Ctrl+C hoặc yêu cầu dừng từ Docker.
	<-quit                                               // Luồng main sẽ tạm dừng ở đây. Chỉ khi nhận được tín hiệu tắt, nó mới chạy xuống các lệnh bên dưới.
	log.Println("Shutdown Server ...")

	// Cung cấp cho server một khoảng thời gian chờ là 5 giây để xử lý nốt các kết nối/request đang chạy dở dang.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv.Shutdown(ctx) // Yêu cầu dừng server dần dần.
}
