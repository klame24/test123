package storage

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/gometeo/app/internal/model"
	_ "github.com/jackc/pgx/v5/stdlib"
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

	// Настройка пула соединений
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(25)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("ошибка подключения к БД: %w", err)
	}

	// Автоматическая миграция
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

	logger.Info("База данных инициализирована")
	return &WeatherStorage{db: db, logger: logger}, nil
}

func (s *WeatherStorage) Close() {
	s.db.Close()
}

func (s *WeatherStorage) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

// Save обновляет погоду или создает новую запись
func (s *WeatherStorage) Save(ctx context.Context, data model.WeatherData) error {
	query := `
		INSERT INTO weather (city, temp, condition, provider, updated_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (city) DO UPDATE 
		SET temp = EXCLUDED.temp,
		    condition = EXCLUDED.condition,
			provider = EXCLUDED.provider,
			updated_at = EXCLUDED.updated_at;
	`

	_, err := s.db.ExecContext(ctx, query, 
		data.City, 
		data.Temp, 
		data.Condition, 
		data.Provider, 
		time.Now(),
	)
	
	if err != nil {
		return fmt.Errorf("ошибка сохранения погоды для %s: %w", data.City, err)
	}

	s.logger.Debug("Данные сохранены в БД", "city", data.City)
	return nil
}

// GetByCity возвращает погоду для конкретного города
func (s *WeatherStorage) GetByCity(ctx context.Context, city string) (*model.WeatherData, error) {
	query := `
		SELECT city, temp, condition, provider, updated_at
		FROM weather
		WHERE city = $1
	`

	var data model.WeatherData
	err := s.db.QueryRowContext(ctx, query, city).Scan(
		&data.City,
		&data.Temp,
		&data.Condition,
		&data.Provider,
		&data.Timestamp,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("город %s не найден", city)
	}
	if err != nil {
		return nil, fmt.Errorf("ошибка получения данных: %w", err)
	}

	return &data, nil
}

// GetAllCities возвращает список всех городов
func (s *WeatherStorage) GetAllCities(ctx context.Context) ([]string, error) {
	query := `SELECT city FROM weather ORDER BY city`
	
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("ошибка получения городов: %w", err)
	}
	defer rows.Close()

	var cities []string
	for rows.Next() {
		var city string
		if err := rows.Scan(&city); err != nil {
			return nil, fmt.Errorf("ошибка сканирования: %w", err)
		}
		cities = append(cities, city)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("ошибка итерации: %w", err)
	}

	return cities, nil
}