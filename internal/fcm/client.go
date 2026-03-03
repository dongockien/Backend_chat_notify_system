package fcm

import (
	"context"
	"errors"
	"fmt"
	"log"

	firebase "firebase.google.com/go/v4"
	"firebase.google.com/go/v4/messaging"
	"google.golang.org/api/option"
)

type Client struct {
	messagingClient *messaging.Client
}

func NewClient(credentialsFile string) (*Client, error) {
	opt := option.WithCredentialsFile(credentialsFile)
	app, err := firebase.NewApp(context.Background(), nil, opt)
	if err != nil {
		return nil, fmt.Errorf("firebase.NewApp: %w", err)
	}

	fcmClient, err := app.Messaging(context.Background())
	if err != nil {
		return nil, fmt.Errorf("app.Messaging: %w", err)
	}

	log.Println("Kết nối Firebase Cloud Messaging thành công")
	return &Client{messagingClient: fcmClient}, nil
}

func (c *Client) SendPushNotification(ctx context.Context, tokens []string, content string) ([]string, error) {
	if c.messagingClient == nil {
		return nil, errors.New("FCM client chưa được khởi tạo")
	}
	if len(tokens) == 0 {
		return nil, nil
	}

	const chunkSize = 500
	var allInvalidTokens []string

	for i := 0; i < len(tokens); i += chunkSize {
		end := i + chunkSize
		if end > len(tokens) {
			end = len(tokens)
		}
		batch := tokens[i:end]

		invalid, err := c.sendBatch(ctx, batch, content)
		if err != nil {
		
			log.Printf("Lỗi gửi batch %d-%d: %v", i, end, err)
		}
		allInvalidTokens = append(allInvalidTokens, invalid...)
	}

	return allInvalidTokens, nil
}


func (c *Client) sendBatch(ctx context.Context, tokens []string, content string) ([]string, error) {
	message := &messaging.MulticastMessage{
		Tokens: tokens,
		Notification: &messaging.Notification{
			Title: "Tin nhắn mới",
			Body:  content,
		},
	}

	response, err := c.messagingClient.SendEachForMulticast(ctx, message)
	if err != nil {
		return nil, fmt.Errorf("SendEachForMulticast: %w", err)
	}

	log.Printf("Đã gửi %d notifications, thành công: %d, thất bại: %d",
		len(tokens), response.SuccessCount, response.FailureCount)

	var invalidTokens []string
	for idx, resp := range response.Responses {
		if resp.Success {
			continue
		}
		if messaging.IsUnregistered(resp.Error) {
			invalidTokens = append(invalidTokens, tokens[idx])
		} else {
			log.Printf("Lỗi gửi cho token %s: %v", tokens[idx], resp.Error)
		}
	}
	return invalidTokens, nil
}