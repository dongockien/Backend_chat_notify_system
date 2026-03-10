package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// hàm chạy chính của chương trình test
func main() {

	// biến chứa chuỗi jwt token hợp lệ (ví dụ của user Kem Dev) để dùng cho việc xác thực gọi API
	token := "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3NzI2MTI1NDcsInVzZXJfaWQiOjd9.rGGT59a7bSDzFwGB-fkZeBKPuSL79dSLsz5s4D6Saks"

	// đường dẫn tới api gửi tin nhắn của server đang chạy ở dưới máy local
	url := "http://localhost:8088/api/messages"

	// chuẩn bị sẵn chuỗi dữ liệu json chứa nội dung tin nhắn và cấu hình gửi (phòng 99, gửi cho user 5)
	// chuỗi này được ép kiểu sang mảng byte ([]byte) để chuẩn bị đính kèm vào body của request http
	payload := []byte(`{"conversation_id": 99, "receiver_ids": [5], "content": "Tin nhắn này đích danh Kem Dev gửi!"}`)

	// khởi tạo một biến wg thuộc kiểu sync.WaitGroup
	// giúp hàm main theo dõi và đợi các luồng chạy song song (goroutine) hoàn thành công việc rồi mới kết thúc
	var wg sync.WaitGroup

	// khai báo tổng số request (tin nhắn) sẽ bắn đi để test tải
	totalRequests := 50

	// in ra màn hình console thông báo bắt đầu tiến trình test
	fmt.Printf("BẮT ĐẦU GỬI %d TIN NHẮN...\n", totalRequests)

	// ghi lại thời điểm bắt đầu chạy vòng lặp để tí nữa tính tổng thời gian chạy
	startTime := time.Now()

	// vòng lặp chạy đúng bằng số lượng request đã cấu hình (50 lần)
	for i := 1; i <= totalRequests; i++ {

		// báo cho WaitGroup biết là có thêm 1 luồng chuẩn bị chạy (cộng bộ đếm lên 1)
		wg.Add(1)

		// từ khóa 'go' giúp khởi tạo một luồng chạy song song (goroutine) tách biệt với luồng chính
		// hàm ẩn danh này nhận vào tham số idx để biết nó đang xử lý tin nhắn thứ mấy
		go func(idx int) {

			// lệnh defer sẽ được gọi khi hàm ẩn danh này kết thúc
			// wg.Done() báo cho WaitGroup biết luồng này đã xong việc, để giảm bộ đếm đi 1
			defer wg.Done()

			// khởi tạo một http request mới với phương thức POST, truyền vào url và nội dung payload đã chuẩn bị
			req, _ := http.NewRequest("POST", url, bytes.NewBuffer(payload))

			// gán thêm cấu hình header để báo cho server biết dữ liệu gửi lên định dạng JSON
			req.Header.Set("Content-Type", "application/json")

			// gán header Authorization chứa JWT token để vượt qua lớp bảo mật middleware của server
			req.Header.Set("Authorization", "Bearer "+token)

			// tạo một http client để thực hiện gửi request. cài đặt timeout là 10 giây để luồng không bị treo nếu server không phản hồi
			client := &http.Client{Timeout: 10 * time.Second}

			// thực thi việc gọi API và hứng kết quả trả về vào biến resp, hoặc lỗi vào biến err
			resp, err := client.Do(req)

			// kiểm tra nếu có lỗi ở mức độ network (ví dụ: server chưa bật, mất mạng)
			if err != nil {
				fmt.Printf("Lỗi mạng gửi tin %d: %v\n", idx, err) // in ra log
				return                                            // thoát khỏi luồng hiện tại sớm
			}

			// đóng body của response khi xử lý xong để tránh tình trạng rò rỉ bộ nhớ (memory leak)
			defer resp.Body.Close()

			// kiểm tra mã phản hồi từ server. 200 (StatusOK) nghĩa là API đã tiếp nhận và xử lý thành công
			if resp.StatusCode == 200 {
				fmt.Printf("Tin nhắn %d đã gửi vào API thành công\n", idx) // báo thành công
			} else {
				// nếu server trả về mã lỗi (như 400, 401, 500)
				// dùng io.ReadAll để đọc nội dung chi tiết mà server gửi về trong body để biết lý do lỗi
				bodyBytes, _ := io.ReadAll(resp.Body)
				fmt.Printf(" Tin nhắn %d bị từ chối: HTTP %d - LÝ DO: %s\n", idx, resp.StatusCode, string(bodyBytes))
			}
		}(i) // truyền giá trị i của vòng lặp hiện tại vào làm đối số idx cho hàm ẩn danh
	}

	// hàm main sẽ bị chặn lại (đứng chờ) ở dòng lệnh này.
	// nó chỉ chạy tiếp khi nào bộ đếm của WaitGroup về 0 (tức là 50 luồng đều đã gọi xong wg.Done())
	wg.Wait()

	// in ra thông báo hoàn tất và tính toán khoảng thời gian thực thi bằng cách lấy thời gian hiện tại trừ đi thời gian lúc bắt đầu (startTime)
	fmt.Printf("\n Hoàn thành gửi %d tin nhắn vào API Server trong: %v\n", totalRequests, time.Since(startTime))
}
