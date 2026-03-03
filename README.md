# 🚀 Enterprise Distributed Chat & Notification System

Hệ thống Chat và Push Notification phân tán, chịu tải cao (High-throughput), được thiết kế để giải quyết bài toán nghẽn cổ chai (Bottleneck) khi xử lý lưu lượng tin nhắn lớn. Dự án áp dụng kiến trúc Hướng Sự Kiện (Event-Driven Architecture) với Apache Kafka và mẫu thiết kế Transactional Outbox để đảm bảo tính nhất quán dữ liệu (Data Consistency) và không thất thoát tin nhắn (Zero Data Loss).

## 🌳 SƠ ĐỒ KIẾN TRÚC HỆ THỐNG (ARCHITECTURAL LAYERS)

```text
📱 [TẦNG 1: CLIENT / FRONTEND] - Ứng dụng Flutter
│
├── 🔑 Xác thực & Định danh (Authentication & Device Registration)
│   └── Đăng nhập -> Nhận JWT Token -> Lấy Device Token (FCM) và lưu trữ trên Server.
│
├── ⚡ Giao tiếp Thời gian thực & Đồng bộ Lịch sử (Real-time & Sync)
│   ├── Duy trì kết nối WebSocket để nhận tin nhắn hai chiều với độ trễ thấp.
│   └── Gọi API `GET /api/messages` để đồng bộ lịch sử tin nhắn khi khởi tạo hoặc khôi phục kết nối.
│
└── ♻️ Quản lý Vòng đời Ứng dụng (App Lifecycle Management)
    ├── 🟢 Foreground (Đang hiển thị): Duy trì WebSocket, vô hiệu hóa thông báo đẩy (Push Notification).
    ├── 🔴 Background/Paused (Ẩn/Chạy ngầm): Đóng WebSocket để Server nhận biết trạng thái Offline.
    └── 🔄 Resumed (Khôi phục): Gọi API tải lịch sử tin nhắn bị lỡ -> Kết nối lại WebSocket.


🌐 [TẦNG 2: BACKEND API] - Golang Gin (API Gateway)
│
├── 🚪 RESTful APIs
│   └── Xử lý định danh, cấp phiên làm việc (Session), và quản lý thiết bị (Devices).
│
├── 💬 Xử lý Tin nhắn (`POST /api/messages`) -> ⚠️ Mẫu thiết kế Transactional Outbox
│   ├── Khởi tạo Database Transaction.
│   ├── Lưu tin nhắn vào bảng `messages` (Dữ liệu nghiệp vụ).
│   ├── Lưu sự kiện "message.sent" vào bảng `outbox` (Dữ liệu đồng bộ).
│   ├── Commit Transaction.
│   └── Phản hồi `200 OK` ngay lập tức cho Client (Tối ưu hóa thời gian phản hồi < 20ms).
│
└── 📡 Quản lý Kết nối (WebSocket Manager)
    ├── Client kết nối: Đánh dấu trạng thái Online trên Redis (`online:user_id`).
    └── Client ngắt kết nối: Xóa cờ Online, giải phóng tài nguyên.


⚙️ [TẦNG 3: MESSAGE BROKER & WORKERS] - Hệ thống Xử lý Bất đồng bộ (Asynchronous Processing)
│
├── 👷 [Outbox Poller] - Worker Thu thập Sự kiện
│   └── Liên tục quét bảng `outbox` -> Đẩy thông điệp (Message) vào Kafka.
│
├── 🚂 [Apache Kafka] - Hàng đợi Tin nhắn (Message Queue)
│   └── Cung cấp khả năng chịu tải (Stress test đạt 50 requests/500ms). Tách biệt tải trọng (Decoupling) 
│       giữa API Server và Service gửi thông báo.
│
└── 🕵️ [Consumer Worker] - Phân tích Điều phối & Thông báo (Notification Dispatcher)
    ├── Tiêu thụ (Consume) sự kiện từ Kafka.
    ├── 🛡️ Bộ lọc 1 (Redis Presence): Kiểm tra trạng thái Online của người nhận -> [CÓ = BỎ QUA].
    ├── 🛡️ Bộ lọc 2 (DB Settings): Kiểm tra cấu hình "Không làm phiền" (Mute) -> [CÓ = BỎ QUA].
    ├── 🚀 Đóng gói (Batching): Gom nhóm Token thiết bị hợp lệ, gọi API Google FCM dạng Multicast.
    └── 📔 Lưu vết (Audit Logging): Ghi nhận trạng thái xử lý (SUCCESS/FAILED/SKIPPED) vào DB.


☁️ [TẦNG 4: INFRASTRUCTURE & EXTERNAL SERVICES] - Hạ tầng & Dịch vụ bên thứ ba
│
├── 🔔 Google Firebase Cloud Messaging (FCM): Dịch vụ gửi Push Notification tới thiết bị người dùng.
├── 🐘 PostgreSQL (RDBMS): Đảm bảo tính ACID (Users, Messages, Devices, Outbox, Notification_Logs).
└── ⚡ Redis (In-memory Cache): Quản lý trạng thái hiện diện (Presence) và khóa Idempotency.

ÁC QUYẾT ĐỊNH THIẾT KẾ CỐT LÕI (CORE DESIGN DECISIONS)
1. Tại sao sử dụng Kafka thay vì gọi trực tiếp API FCM? (Kiểm soát Thông lượng - Throughput Control)
Vấn đề (Synchronous Blocking): Nếu API Server nhận tin nhắn và tiến hành gọi trực tiếp dịch vụ Firebase cho 50 thành viên trong nhóm, tiến trình sẽ bị nghẽn (Block) chờ phản hồi từ mạng. Khi lưu lượng tăng đột biến, API Server sẽ cạn kiệt tài nguyên (Thread/Memory) và dẫn đến sập hệ thống (Crash).

Giải pháp (Asynchronous Decoupling): Bằng việc đưa Kafka vào giữa, API Server chỉ thực hiện thao tác tốn ít thời gian nhất: Lưu DB và trả về 200 OK (Thực tế đạt tốc độ ~11ms/request). Các yêu cầu gửi thông báo phức tạp và tốn thời gian sẽ được Kafka lưu trữ an toàn, sau đó Consumer Worker sẽ lấy ra xử lý với tốc độ được kiểm soát (Rate limiting), bảo vệ API Server khỏi tình trạng quá tải.

2. Transactional Outbox Pattern (Đảm bảo tính Nhất quán - Zero Data Loss)
Mục tiêu: Tránh rủi ro hệ thống gặp sự cố (Network Failure) sau khi lưu DB nhưng chưa kịp đẩy sự kiện lên Kafka.

Giải pháp: Sử dụng DB Transaction: Dữ liệu tin nhắn (messages) và dữ liệu sự kiện (outbox) được lưu đồng thời. Nếu quá trình bị lỗi, toàn bộ sẽ được Rollback. Nếu thành công, Worker sẽ đảm bảo sự kiện được đưa lên Kafka (At-least-once delivery).

3. Tối ưu hóa API Call bằng Multicast Batching & Smart Filtering
Gửi thông báo đẩy là tác vụ tốn kém tài nguyên (Cost-heavy). Hệ thống áp dụng 2 cơ chế:

Smart Filter: Tra cứu trạng thái người dùng qua Redis (In-memory, độ trễ < 1ms). Nếu người dùng đang duy trì Socket (Foreground), hệ thống lập tức loại bỏ việc gửi Push FCM để tránh gửi thông báo rác.

Multicast Batching: Thay vì thực hiện 50 HTTP Requests riêng lẻ tới Google FCM, hệ thống gom (Chunk) các Device Token thành một lô (Batch) giới hạn 500 tokens/request. Lệnh SendEachForMulticast giúp tối ưu hóa băng thông mạng.

4. Cơ chế Lưu vết (Notification Audit Logging)
Hỗ trợ việc đối soát và gỡ lỗi (Troubleshooting) trên môi trường Production. Mọi quyết định của Consumer Worker đều được lưu vào bảng notification_logs:

SUCCESS: Hệ thống đã bàn giao thành công payload cho FCM.

SKIPPED: Sự kiện bị loại bỏ do bộ lọc (Người dùng đang Online hoặc cài đặt Mute).

FAILED: Sự kiện lỗi do cấu hình FCM hoặc token thiết bị không hợp lệ (Đi kèm Reason chi tiết).
