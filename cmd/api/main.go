package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gorilla/mux"
	"github.com/gometeo/app/internal/api/handlers"
	"github.com/gometeo/app/internal/cache"
	"github.com/gometeo/app/internal/config"
	"github.com/gometeo/app/internal/storage"
)

func main() {
	// Настройка логирования
	logger := setupLogger()
	logger.Info("Запуск Weather API сервиса...")

	// Загрузка конфигурации
	cfg := config.Load()
	logger.Info("Конфигурация загружена",
		"port", cfg.HTTPPort,
		"redis", cfg.RedisAddr,
		"cache_ttl", cfg.CacheTTL)

	// 1. Подключение к Postgres
	store, err := storage.New(cfg.DBDSN, logger)
	if err != nil {
		logger.Error("Не удалось подключиться к БД", "error", err)
		os.Exit(1)
	}
	defer store.Close()
	logger.Info("Успешное подключение к Postgres")

	// 2. Подключение к Redis
	redisCache, err := cache.New(
		cfg.RedisAddr,
		cfg.RedisPassword,
		cfg.RedisDB,
		cfg.CacheTTL,
		logger,
	)
	if err != nil {
		logger.Error("Не удалось подключиться к Redis", "error", err)
		os.Exit(1)
	}
	defer redisCache.Close()
	logger.Info("Успешное подключение к Redis")

	// 3. Настройка маршрутизатора
	router := mux.NewRouter()
	weatherHandler := handlers.NewWeatherHandler(store, redisCache, logger)

	// API маршруты
	api := router.PathPrefix("/api/v1").Subrouter()
	
	// Weather endpoints
	api.HandleFunc("/weather/{city}", weatherHandler.GetWeather).Methods("GET")
	api.HandleFunc("/weather/{city}", weatherHandler.UpdateWeather).Methods("PUT")
	api.HandleFunc("/cities", weatherHandler.GetAllCities).Methods("GET")
	
	// Health check
	api.HandleFunc("/health", weatherHandler.HealthCheck).Methods("GET")
	
	// Middleware
	router.Use(loggingMiddleware(logger))
	router.Use(contentTypeMiddleware)

	// 4. Настройка HTTP сервера
	server := &http.Server{
		Addr:         ":" + cfg.HTTPPort,
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// 5. Graceful shutdown
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		logger.Info("Сервер запущен", "port", cfg.HTTPPort)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("Ошибка сервера", "error", err)
		}
	}()

	// Ожидание сигнала завершения
	<-stopChan
	logger.Info("Получен сигнал завершения...")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		logger.Error("Ошибка при остановке сервера", "error", err)
	} else {
		logger.Info("Сервер остановлен")
	}
}

func setupLogger() *slog.Logger {
	opts := &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}

	var handler slog.Handler = slog.NewTextHandler(os.Stdout, opts)
	
	// Для продакшена используем JSON формат
	if os.Getenv("ENV") == "production" {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	}

	return slog.New(handler)
}

// Middleware для логирования
func loggingMiddleware(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			
			// Создаем ResponseWriter для отслеживания статуса
			rw := &responseWriter{ResponseWriter: w, status: 200}
			
			next.ServeHTTP(rw, r)
			
			duration := time.Since(start)
			
			logger.Info("HTTP запрос",
				"method", r.Method,
				"path", r.URL.Path,
				"status", rw.status,
				"duration_ms", duration.Milliseconds(),
				"user_agent", r.UserAgent(),
				"remote_addr", r.RemoteAddr,
			)
		})
	}
}

// Кастомный ResponseWriter для отслеживания статуса
type responseWriter struct {
	http.ResponseWriter
	status int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.status = code
	rw.ResponseWriter.WriteHeader(code)
}

// Middleware для установки Content-Type
func contentTypeMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		next.ServeHTTP(w, r)
	})
}