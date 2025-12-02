package model

import (
	"time"
)

type WeatherData struct {
	City      string    `json:"city"`
	Temp      float64   `json:"temperature"`
	Condition string    `json:"condition"`
	Provider  string    `json:"provider"`
	Timestamp time.Time `json:"timestamp"`
}

type WeatherResponse struct {
	WeatherData
	Cached bool `json:"cached"` // Флаг, указывающий откуда данные
}

type CitiesResponse struct {
	Cities []string `json:"cities"`
	Total  int      `json:"total"`
}

type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message,omitempty"`
}