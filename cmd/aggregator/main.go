package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/gometeo/app/internal/model"
	"github.com/gometeo/app/internal/storage"
)

const (
	topic         = "weather_data"
	consumerGroup = "weather_aggregator_group"
	brokerAddress = "localhost:9092"
	// Строка подключения: user:password@host:port/dbname
	dbDSN = "postgres://postgres:password123@127.0.0.1:5432/gometeo?sslmode=disable"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	logger.Info("Запуск Weather Aggregator...")

	// 1. Подключение к Postgres
	var store *storage.WeatherStorage
	var err error
	maxRetries := 5

	for i := 0; i < maxRetries; i++ {
		store, err = storage.New(dbDSN, logger)
		if err == nil {
			logger.Info("Успешное подключение к Postgres")
			break
		}
		logger.Warn("Не удалось подключиться к БД. Повторная попытка через 3с...",
			"попытка", i+1, "всего", maxRetries, "error", err)
		time.Sleep(3 * time.Second)
	}

	if store == nil {
		logger.Error("Не удалось подключиться к БД после всех попыток. Выход.", "error", err)
		os.Exit(1)
	}
	defer store.Close()
	logger.Info("Успешное подключение к Postgres")

	// 2. Настройка Kafka Consumer
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumer, err := sarama.NewConsumerGroup([]string{brokerAddress}, consumerGroup, config)
	if err != nil {
		logger.Error("Ошибка создания Kafka consumer", "error", err)
		os.Exit(1)
	}

	// 3. Запуск цикла чтения
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		// Передаем store внутрь хендлера
		handler := &ConsumerHandler{logger: logger, store: store}
		for {
			if err := consumer.Consume(ctx, []string{topic}, handler); err != nil {
				logger.Error("Ошибка при чтении Kafka", "error", err)
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()

	// 4. Graceful Shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	logger.Info("Остановка сервиса...")
	cancel()
	wg.Wait()
	consumer.Close()
}

// ConsumerHandler теперь имеет доступ к базе
type ConsumerHandler struct {
	logger *slog.Logger
	store  *storage.WeatherStorage
}

func (h *ConsumerHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (h *ConsumerHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (h *ConsumerHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		var data model.WeatherData
		if err := json.Unmarshal(msg.Value, &data); err != nil {
			h.logger.Error("Битый JSON", "error", err)
			continue
		}

		// СОХРАНЕНИЕ В БАЗУ
		// Используем контекст сессии, чтобы отменить запись, если Kafka отвалилась
		if err := h.store.Save(sess.Context(), data); err != nil {
			h.logger.Error("Ошибка записи в БД", "city", data.City, "error", err)
			// Важный момент: если БД лежит, мы НЕ помечаем сообщение как прочитанное,
			// чтобы Kafka отдала его нам снова позже.
			continue
		}

		h.logger.Info("Данные сохранены в БД",
			"city", data.City,
			"temp", data.Temp)

		sess.MarkMessage(msg, "")
	}
	return nil
}
