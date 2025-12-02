# cmd/api/Dockerfile
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Копируем зависимости
COPY go.mod go.sum ./
RUN go mod download

# Копируем исходный код
COPY . .

# Собираем бинарник
RUN go build -o api ./cmd/api

# Финальный образ
FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /root/

# Копируем бинарник
COPY --from=builder /app/api .

# Экспонируем порт
EXPOSE 8080

# Запускаем сервис
CMD ["./api"]