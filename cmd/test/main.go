package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

func main() {
	
	token := "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3NzI1OTg0MDMsInVzZXJfaWQiOjV9.rMIT6oz9W_MwZ9LSP4XTx6cNhHDQZNWY9sOUa7JPkcE"
	
	url := "http://localhost:8088/api/messages"
	payload := []byte(`{"conversation_id": 99, "receiver_ids": [1, 2, 3], "content": "Test batch 50 tin nhắn"}`)

	var wg sync.WaitGroup
	totalRequests := 50 

	fmt.Printf("BẮT ĐẦU GỬI %d TIN NHẮN...\n", totalRequests)
	startTime := time.Now()

	for i := 1; i <= totalRequests; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			req, _ := http.NewRequest("POST", url, bytes.NewBuffer(payload))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "Bearer "+token)

			client := &http.Client{Timeout: 10 * time.Second}
			resp, err := client.Do(req)
			
			if err != nil {
				fmt.Printf("Lỗi mạng gửi tin %d: %v\n", idx, err)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode == 200 {
				fmt.Printf("Tin nhắn %d đã gửi vào API thành công\n", idx)
			} else {
			
				bodyBytes, _ := io.ReadAll(resp.Body)
				fmt.Printf(" Tin nhắn %d bị từ chối: HTTP %d - LÝ DO: %s\n", idx, resp.StatusCode, string(bodyBytes))
			}
		}(i)
	}

	wg.Wait()
	fmt.Printf("\n Hoàn thành gửi %d tin nhắn vào API Server trong: %v\n", totalRequests, time.Since(startTime))
}