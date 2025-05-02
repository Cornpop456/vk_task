package server

import (
	"fmt"
	"os"
	"strconv"
)

// Config содержит настройки для сервера
type Config struct {
	Port           int
	Ip             string
	MaxConnections int
	// Уровень логирования (debug, info, warn, error)
	LogLevel string
	// Путь к файлу логирования (если не задан, логи выводятся в stdout)
	LogFilePath string
}

func NewConfig() *Config {
	config := &Config{
		Port:           50051,
		Ip:             "",
		MaxConnections: 100,
		LogLevel:       "info",
		LogFilePath:    "", // По умолчанию пустая строка (stdout)
	}

	// Чтение порта из переменной окружения
	if port := os.Getenv("PUBSUB_PORT"); port != "" {
		if p, err := strconv.Atoi(port); err == nil {
			config.Port = p
		}
	}

	// Чтение максимального количества подключений из переменной окружения
	if maxConn := os.Getenv("PUBSUB_MAX_CONNECTIONS"); maxConn != "" {
		if mc, err := strconv.Atoi(maxConn); err == nil {
			config.MaxConnections = mc
		}
	}

	// Чтение уровня логирования из переменной окружения
	if logLevel := os.Getenv("PUBSUB_LOG_LEVEL"); logLevel != "" {
		config.LogLevel = logLevel
	}

	// Чтение пути к файлу логирования из переменной окружения
	if logFilePath := os.Getenv("PUBSUB_LOG_FILE"); logFilePath != "" {
		config.LogFilePath = logFilePath
	}

	return config
}

func (c *Config) Address() string {
	return fmt.Sprintf("%s:%d", c.Ip, c.Port)
}
