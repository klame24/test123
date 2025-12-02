package handlers

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"log/slog"

	"github.com/gometeo/app/internal/cache"
	"github.com/gometeo/app/internal/model"
	"github.com/gometeo/app/internal/storage"
)

type WeatherHandler struct {
	store  *storage.WeatherStorage
	cache  *cache.WeatherCache
	logger *slog.Logger
}

func NewWeatherHandler(store *storage.WeatherStorage, cache *cache.WeatherCache, logger *slog.Logger) *WeatherHandler {
	return &WeatherHandler{
		store:  store,
		cache:  cache,
		logger: logger,
	}
}

// GetWeather возвращает погоду для конкретного города
func (h *WeatherHandler) GetWeather(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	city := strings.ToLower(mux.Vars(r)["city"])
	
	h.logger.Info("Запрос погоды", "city", city, "method", r.Method)
	
	// 1. Пробуем получить из кэша
	ctx := r.Context()
	cachedData, err := h.cache.Get(ctx, cache.CityKey(city))
	if err != nil {
		h.logger.Error("Ошибка чтения из кэша", "city", city, "error", err)
		// Продолжаем - кэш не критичен
	}

	if cachedData != nil {
		h.logger.Debug("Данные из кэша", "city", city)
		
		response := model.WeatherResponse{
			WeatherData: *cachedData,
			Cached:      true,
		}
		
		sendJSON(w, http.StatusOK, response)
		
		h.logger.Info("Данные отданы из кэша", 
			"city", city, 
			"duration_ms", time.Since(start).Milliseconds(),
			"source", "cache")
		return
	}

	// 2. Получаем из базы данных
	dbData, err := h.store.GetByCity(ctx, city)
	if err != nil {
		h.logger.Error("Ошибка чтения из БД", "city", city, "error", err)
		sendError(w, http.StatusNotFound, "Город не найден", err.Error())
		return
	}

	// 3. Сохраняем в кэш для будущих запросов
	if err := h.cache.Set(ctx, cache.CityKey(city), *dbData); err != nil {
		h.logger.Warn("Не удалось сохранить в кэш", "city", city, "error", err)
	}

	response := model.WeatherResponse{
		WeatherData: *dbData,
		Cached:      false,
	}

	sendJSON(w, http.StatusOK, response)
	
	h.logger.Info("Данные отданы из БД", 
		"city", city, 
		"duration_ms", time.Since(start).Milliseconds(),
		"source", "database")
}

// GetAllCities возвращает список всех городов
func (h *WeatherHandler) GetAllCities(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	
	// Проверяем кэш
	ctx := r.Context()
	cached, err := h.cache.Get(ctx, cache.AllCitiesKey())
	if err != nil {
		h.logger.Error("Ошибка чтения кэша городов", "error", err)
	}
	
	if cached != nil {
		cities := strings.Split(cached.City, ",") // Храним список как строку
		response := model.CitiesResponse{
			Cities: cities,
			Total:  len(cities),
		}
		
		sendJSON(w, http.StatusOK, response)
		
		h.logger.Info("Список городов из кэша",
			"count", len(cities),
			"duration_ms", time.Since(start).Milliseconds())
		return
	}

	// Получаем из БД
	cities, err := h.store.GetAllCities(ctx)
	if err != nil {
		h.logger.Error("Ошибка получения городов из БД", "error", err)
		sendError(w, http.StatusInternalServerError, "Внутренняя ошибка сервера", "")
		return
	}

	// Сохраняем в кэш (используем структуру WeatherData для хранения списка)
	cacheData := model.WeatherData{
		City: strings.Join(cities, ","),
	}
	
	if err := h.cache.Set(ctx, cache.AllCitiesKey(), cacheData); err != nil {
		h.logger.Warn("Не удалось сохранить города в кэш", "error", err)
	}

	response := model.CitiesResponse{
		Cities: cities,
		Total:  len(cities),
	}

	sendJSON(w, http.StatusOK, response)
	
	h.logger.Info("Список городов из БД",
		"count", len(cities),
		"duration_ms", time.Since(start).Milliseconds())
}

// UpdateWeather (только для теста) - обновляет данные города
func (h *WeatherHandler) UpdateWeather(w http.ResponseWriter, r *http.Request) {
	city := strings.ToLower(mux.Vars(r)["city"])
	
	var data model.WeatherData
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		sendError(w, http.StatusBadRequest, "Неверный формат JSON", err.Error())
		return
	}
	
	data.City = city
	data.Timestamp = time.Now()
	
	ctx := r.Context()
	
	// Сохраняем в БД
	if err := h.store.Save(ctx, data); err != nil {
		h.logger.Error("Ошибка сохранения в БД", "city", city, "error", err)
		sendError(w, http.StatusInternalServerError, "Ошибка сохранения", err.Error())
		return
	}
	
	// Инвалидируем кэш
	if err := h.cache.Delete(ctx, cache.CityKey(city)); err != nil {
		h.logger.Warn("Не удалось удалить из кэша", "city", city, "error", err)
	}
	
	// Также инвалидируем кэш списка городов
	if err := h.cache.Delete(ctx, cache.AllCitiesKey()); err != nil {
		h.logger.Warn("Не удалось удалить список городов из кэша", "error", err)
	}
	
	sendJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	
	h.logger.Info("Данные обновлены", "city", city)
}

// HealthCheck проверяет доступность сервисов
func (h *WeatherHandler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	
	health := map[string]string{
		"status": "ok",
		"time":   time.Now().Format(time.RFC3339),
	}
	
	// Проверка БД
	if err := h.store.Ping(ctx); err != nil {
		health["database"] = "unhealthy"
		health["status"] = "degraded"
		h.logger.Error("Health check: DB недоступна", "error", err)
	} else {
		health["database"] = "healthy"
	}
	
	// Проверка Redis
	if _, err := h.cache.Exists(ctx, "health:check"); err != nil {
		health["redis"] = "unhealthy"
		health["status"] = "degraded"
		h.logger.Error("Health check: Redis недоступен", "error", err)
	} else {
		health["redis"] = "healthy"
	}
	
	status := http.StatusOK
	if health["status"] == "degraded" {
		status = http.StatusServiceUnavailable
	}
	
	sendJSON(w, status, health)
}

// Вспомогательные функции
func sendJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func sendError(w http.ResponseWriter, status int, errorMsg, details string) {
	response := model.ErrorResponse{
		Error:   errorMsg,
		Message: details,
	}
	
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(response)
}