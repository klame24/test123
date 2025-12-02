package main

import (
	"encoding/json"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/gometeo/app/internal/model"
)

const (
	topic         = "weather_data"
	brokerAddress = "localhost:9092"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	logger.Info("Запуск Weather Collector...")

	// 1. Настройка Kafka Producer
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	// Важно для надежности: ждать подтверждения от Kafka, что сообщение записано
	config.Producer.RequiredAcks = sarama.WaitForAll

	producer, err := sarama.NewSyncProducer([]string{brokerAddress}, config)
	if err != nil {
		logger.Error("Ошибка подключения к Kafka", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			logger.Error("Ошибка при закрытии продюсера", "error", err)
		}
	}()

	// 2. Канал для Graceful Shutdown (Ctrl+C)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// 3. Тикер для эмуляции CRON (каждые 3 секунды)
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	cities := []string{"Moscow", "London", "New York", "Berlin", "Tokyo"}

	logger.Info("Начинаем сбор данных...")

	for {
		select {
		case <-sigChan:
			logger.Info("Получен сигнал завершения. Остановка...")
			return
		case <-ticker.C:
			// Эмуляция получения данных от внешнего API
			data := model.WeatherData{
				City:      cities[rand.Intn(len(cities))],
				Temp:      float64(rand.Intn(40)-10) + rand.Float64(), // Случайная темп.
				Condition: "Cloudy",
				Provider:  "OpenWeatherMap",
				Timestamp: time.Now(),
			}

			// Сериализация
			bytes, err := json.Marshal(data)
			if err != nil {
				logger.Error("Ошибка JSON", "error", err)
				continue
			}

			// Отправка в Kafka
			msg := &sarama.ProducerMessage{
				Topic: topic,
				Value: sarama.ByteEncoder(bytes),
			}

			partition, offset, err := producer.SendMessage(msg)
			if err != nil {
				logger.Error("Не удалось отправить сообщение", "error", err)
			} else {
				logger.Info("Погода отправлена",
					"city", data.City,
					"temp", int(data.Temp),
					"partition", partition,
					"offset", offset)
			}
		}
	}
}
