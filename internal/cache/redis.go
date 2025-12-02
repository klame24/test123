package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/gometeo/app/internal/model"
	"github.com/redis/go-redis/v9"
)

type WeatherCache struct {
	client *redis.Client
	ttl    time.Duration
	logger *slog.Logger
}

func New(addr, password string, db int, ttl time.Duration, logger *slog.Logger) (*WeatherCache, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	// Проверка подключения
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("не удалось подключиться к Redis: %w", err)
	}

	logger.Info("Успешное подключение к Redis", "addr", addr)

	return &WeatherCache{
		client: client,
		ttl:    ttl,
		logger: logger,
	}, nil
}

func (c *WeatherCache) Close() error {
	return c.client.Close()
}

func (c *WeatherCache) Set(ctx context.Context, key string, data model.WeatherData) error {
	bytes, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("ошибка сериализации: %w", err)
	}

	err = c.client.Set(ctx, key, bytes, c.ttl).Err()
	if err != nil {
		return fmt.Errorf("ошибка записи в Redis: %w", err)
	}

	c.logger.Debug("Данные сохранены в кэш", "key", key, "ttl", c.ttl)
	return nil
}

func (c *WeatherCache) Get(ctx context.Context, key string) (*model.WeatherData, error) {
	val, err := c.client.Get(ctx, key).Result()
	if err == redis.Nil {
		return nil, nil // Ключ не найден - это не ошибка
	}
	if err != nil {
		return nil, fmt.Errorf("ошибка чтения из Redis: %w", err)
	}

	var data model.WeatherData
	if err := json.Unmarshal([]byte(val), &data); err != nil {
		return nil, fmt.Errorf("ошибка десериализации: %w", err)
	}

	c.logger.Debug("Данные получены из кэша", "key", key)
	return &data, nil
}

func (c *WeatherCache) Delete(ctx context.Context, key string) error {
	err := c.client.Del(ctx, key).Err()
	if err != nil {
		return fmt.Errorf("ошибка удаления из Redis: %w", err)
	}

	c.logger.Debug("Данные удалены из кэша", "key", key)
	return nil
}

func (c *WeatherCache) Exists(ctx context.Context, key string) (bool, error) {
	exists, err := c.client.Exists(ctx, key).Result()
	if err != nil {
		return false, fmt.Errorf("ошибка проверки ключа: %w", err)
	}
	return exists > 0, nil
}

// Вспомогательные методы для генерации ключей
func CityKey(city string) string {
	return "weather:city:" + city
}

func AllCitiesKey() string {
	return "weather:cities:all"
}
