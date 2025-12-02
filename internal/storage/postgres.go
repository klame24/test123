package storage

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/gometeo/app/internal/model"
	_ "github.com/jackc/pgx/v5/stdlib" // Регистрируем драйвер pgx
)

type WeatherStorage struct {
	db     *sql.DB
	logger *slog.Logger
}

func New(dsn string, logger *slog.Logger) (*WeatherStorage, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("ошибка открытия БД: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("ошибка подключения к БД: %w", err)
	}

	// Автоматическая миграция (создание таблицы) для простоты
	// В проде так не делают (используют goose или migrate), но для старта - идеально.
	query := `
	CREATE TABLE IF NOT EXISTS weather (
		city VARCHAR(100) PRIMARY KEY,
		temp DOUBLE PRECISION,
		condition VARCHAR(255),
		provider VARCHAR(100),
		updated_at TIMESTAMP
	);`
	
	if _, err := db.Exec(query); err != nil {
		return nil, fmt.Errorf("ошибка создания таблицы: %w", err)
	}

	return &WeatherStorage{db: db, logger: logger}, nil
}

func (s *WeatherStorage) Close() {
	s.db.Close()
}

// Save обновляет погоду или создает новую запись (Upsert)
func (s *WeatherStorage) Save(ctx context.Context, data model.WeatherData) error {
	query := `
		INSERT INTO weather (city, temp, condition, provider, updated_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (city) DO UPDATE 
		SET temp = EXCLUDED.temp,
		    condition = EXCLUDED.condition,
			updated_at = EXCLUDED.updated_at;
	`

	_, err := s.db.ExecContext(ctx, query, 
		data.City, 
		data.Temp, 
		data.Condition, 
		data.Provider, 
		time.Now(), // Записываем время сохранения
	)
	
	if err != nil {
		return fmt.Errorf("ошибка сохранения погоды для %s: %w", data.City, err)
	}

	return nil
}