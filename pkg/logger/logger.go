// Package logger предоставляет структурированное логирование для всего приложения
package logger

import (
	"io"
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	// Log глобальный экземпляр логгера
	Log *zap.Logger
)

// Init инициализирует глобальный логгер с указанным уровнем логирования
// и путем к файлу логирования (если filePath пустой, используется stdout)
func Init(level string, filePath string) {
	// Настраиваем уровень логирования
	var logLevel zapcore.Level
	switch level {
	case "debug":
		logLevel = zapcore.DebugLevel
	case "info":
		logLevel = zapcore.InfoLevel
	case "warn":
		logLevel = zapcore.WarnLevel
	case "error":
		logLevel = zapcore.ErrorLevel
	default:
		logLevel = zapcore.InfoLevel
	}

	// Настройка энкодера для логов
	encoderConfig := zapcore.EncoderConfig{
		TimeKey:        "ts",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		FunctionKey:    zapcore.OmitKey,
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}

	// Выбираем вывод логов: в файл или в stdout
	var output io.Writer = os.Stdout
	if filePath != "" {
		file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err == nil {
			output = file
		} else {
			os.Stderr.WriteString("Failed to open log file: " + err.Error() + ", using stdout instead\n")
		}
	}

	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig),
		zapcore.AddSync(output),
		logLevel,
	)

	Log = zap.New(core, zap.AddCaller())
}

func Debug(msg string, fields ...zapcore.Field) {
	Log.WithOptions(zap.AddCallerSkip(1)).Debug(msg, fields...)
}

func Info(msg string, fields ...zapcore.Field) {
	Log.WithOptions(zap.AddCallerSkip(1)).Info(msg, fields...)
}

func Warn(msg string, fields ...zapcore.Field) {
	Log.WithOptions(zap.AddCallerSkip(1)).Warn(msg, fields...)
}

func Error(msg string, fields ...zapcore.Field) {
	Log.WithOptions(zap.AddCallerSkip(1)).Error(msg, fields...)
}

func Fatal(msg string, fields ...zapcore.Field) {
	Log.WithOptions(zap.AddCallerSkip(1)).Fatal(msg, fields...)
}

func With(fields ...zapcore.Field) *zap.Logger {
	return Log.With(fields...)
}

func Field(key string, value interface{}) zapcore.Field {
	return zap.Any(key, value)
}

func String(key string, value string) zapcore.Field {
	return zap.String(key, value)
}

func Int(key string, value int) zapcore.Field {
	return zap.Int(key, value)
}

func Err(err error) zapcore.Field {
	return zap.Error(err)
}
