package model

import "time"

// WeatherData - структура, которая летает через Kafka
type WeatherData struct {
	City      string    `json:"city"`
	Temp      float64   `json:"temperature"`
	Condition string    `json:"condition"`
	Provider  string    `json:"provider"`
	Timestamp time.Time `json:"timestamp"`
}
