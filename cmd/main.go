package main

import (
	"context"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Cornpop456/vk_task/api/proto"
	"github.com/Cornpop456/vk_task/pkg/logger"
	"github.com/Cornpop456/vk_task/server"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func main() {
	config := server.NewConfig()

	// Инициализируем логгер
	logger.Init(config.LogLevel, config.LogFilePath)
	defer logger.Log.Sync()

	logDestination := "stdout"
	if config.LogFilePath != "" {
		logDestination = config.LogFilePath
	}

	logger.Info("Starting PubSub gRPC server...",
		logger.String("address", config.Address()),
		logger.String("logLevel", config.LogLevel),
		logger.String("logDestination", logDestination))

	lis, err := net.Listen("tcp", config.Address())
	if err != nil {
		logger.Fatal("Failed to listen", logger.Err(err))
	}

	opts := []grpc.ServerOption{
		grpc.MaxConcurrentStreams(uint32(config.MaxConnections)),
	}

	grpcServer := grpc.NewServer(opts...)

	pubsubServer := server.NewPubSubServer()

	proto.RegisterPubsubServer(grpcServer, pubsubServer)

	// Канал для получения сигналов ОС
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Запускаем сервер в отдельной горутине
	go func() {
		logger.Info("Server listening",
			logger.String("address", config.Address()),
			logger.Int("maxConnections", config.MaxConnections))

		if err := grpcServer.Serve(lis); err != nil {
			logger.Fatal("Failed to serve", logger.Err(err))
		}
	}()

	// Ожидаем сигнала завершения
	sig := <-sigChan
	logger.Info("Received signal", zap.String("signal", sig.String()))

	logger.Info("Gracefully stopping gRPC server...")
	grpcServer.GracefulStop()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := pubsubServer.Close(ctx); err != nil {
		logger.Error("Error during SubPub close", logger.Err(err))
	} else {
		logger.Info("SubPub closed successfully")
	}

	logger.Info("Server stopped")
}
