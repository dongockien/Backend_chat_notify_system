package repository

import (
	"chat-notify-system/internal/db"
	"chat-notify-system/internal/models"
	"context"
	"fmt"
	"log"
	"time"
)


func GetActiveTokensBatch(ctx context.Context, database *db.Database, userIDs []int64) (map[int64][]string, error) {
	result := make(map[int64][]string)
	if len(userIDs) == 0 {
		return result, nil
	}

	
	var offlineUserIDs []int64
	for _, uid := range userIDs {
		onlineKey := fmt.Sprintf("online:%d", uid)
		isOnline, _ := database.Redis.Exists(ctx, onlineKey).Result()
		
		if isOnline > 0 {
			log.Printf("⏩ Bỏ qua Push FCM: User %d đang Online trên App", uid)
		} else {
			offlineUserIDs = append(offlineUserIDs, uid)
		}
	}

	if len(offlineUserIDs) == 0 {
		log.Printf("Bộ lọc: Tất cả người nhận đều đang Online. Hủy lệnh gọi FCM để tránh làm phiền!")
		return result, nil 
	}

	
	var settings []models.UserNotificationSetting
	database.PG.WithContext(ctx).Where("user_id IN ?", offlineUserIDs).Find(&settings)

	mutedUsers := make(map[int64]bool)
	now := time.Now()
	for _, s := range settings {
		
		if s.MuteUntil.After(now) || !s.AllowChat {
			mutedUsers[s.UserID] = true
			log.Printf(" Bỏ qua Push FCM: User %d đang tắt thông báo", s.UserID)
		}
	}

	var finalTargetIDs []int64
	for _, uid := range offlineUserIDs {
		if !mutedUsers[uid] {
			finalTargetIDs = append(finalTargetIDs, uid)
		}
	}

	if len(finalTargetIDs) == 0 {
		log.Printf("Bộ lọc: Tất cả người nhận (offline) đều đã bật chế độ Không Làm Phiền (Mute). Hủy lệnh gọi FCM!")
		return result, nil
	}

	
	var missIDs []int64
	for _, uid := range finalTargetIDs {
		cacheKey := fmt.Sprintf("user_tokens:%d", uid)
		tokens, err := database.Redis.SMembers(ctx, cacheKey).Result()
		if err == nil && len(tokens) > 0 {
			result[uid] = tokens
		} else {
			missIDs = append(missIDs, uid)
		}
	}

	if len(missIDs) > 0 {
		var devices []models.Device
		if err := database.PG.WithContext(ctx).Where("user_id IN ? AND is_active = ?", missIDs, true).Find(&devices).Error; err != nil {
			return nil, fmt.Errorf("query devices: %w", err)
		}

		userDevices := make(map[int64][]string)
		for _, d := range devices {
			userDevices[d.UserID] = append(userDevices[d.UserID], d.DeviceToken)
		}

		for uid, tokens := range userDevices {
			cacheKey := fmt.Sprintf("user_tokens:%d", uid)
			pipe := database.Redis.Pipeline()
			pipe.SAdd(ctx, cacheKey, tokens)
			pipe.Expire(ctx, cacheKey, 1*time.Hour)
			if _, err := pipe.Exec(ctx); err != nil {
				log.Printf("Lỗi cache tokens cho user %d: %v", uid, err)
			}
			result[uid] = tokens
		}
	}

	return result, nil
}


func InvalidateInvalidTokens(ctx context.Context, database *db.Database, invalidTokens []string) error {
	if len(invalidTokens) == 0 {
		return nil
	}

	
	var devices []models.Device
	if err := database.PG.WithContext(ctx).
		Where("device_token IN ?", invalidTokens).
		Find(&devices).Error; err != nil {
		return fmt.Errorf("find devices by tokens: %w", err)
	}
	
	if err := database.PG.WithContext(ctx).
		Model(&models.Device{}).
		Where("device_token IN ?", invalidTokens).
		Update("is_active", false).Error; err != nil {
		return fmt.Errorf("update devices inactive: %w", err)
	}

	
	for _, d := range devices {
		cacheKey := fmt.Sprintf("user_tokens:%d", d.UserID)
		 
		if err := database.Redis.Del(ctx, cacheKey).Err(); err != nil {
			log.Printf("Lỗi xoá cache cho user %d: %v", d.UserID, err)
		}
		
		
	}

	return nil
}
