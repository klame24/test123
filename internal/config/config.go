package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	HTTPPort      string
	DBDSN         string
	RedisAddr     string
	RedisPassword string
	RedisDB       int
	CacheTTL      time.Duration
	LogLevel      string
}

func Load() *Config {
	ttl, _ := strconv.Atoi(getEnv("CACHE_TTL_SECONDS", "300"))

	return &Config{
		HTTPPort:      getEnv("HTTP_PORT", "8080"),
		DBDSN:         getEnv("DB_DSN", "postgres://postgres:password123@localhost:5433/gometeo?sslmode=disable"),
		RedisAddr:     getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword: getEnv("REDIS_PASSWORD", ""),
		RedisDB:       getEnvInt("REDIS_DB", 0),
		CacheTTL:      time.Duration(ttl) * time.Second,
		LogLevel:      getEnv("LOG_LEVEL", "info"),
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intVal, err := strconv.Atoi(value); err == nil {
			return intVal
		}
	}
	return defaultValue
}

func getEnvSlice(key string, defaultValue []string) []string {
	if value := os.Getenv(key); value != "" {
		return strings.Split(value, ",")
	}
	return defaultValue
}
