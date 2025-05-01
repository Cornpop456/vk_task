package client

import (
	"context"
	"fmt"
	"io"
	"strconv"

	"github.com/Cornpop456/vk_task/api/proto"
	"github.com/Cornpop456/vk_task/pkg/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type PubSubClient struct {
	conn   *grpc.ClientConn
	client proto.PubsubClient
}

func NewPubSubClient(serverAddr string) (*PubSubClient, error) {
	logger.Debug("Подключение к серверу",
		logger.String("serverAddr", serverAddr))

	conn, err := grpc.NewClient(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Error("Не удалось подключиться к серверу",
			logger.String("serverAddr", serverAddr),
			logger.Err(err))
		return nil, fmt.Errorf("failed to connect: %v", err)
	}

	client := proto.NewPubsubClient(conn)

	logger.Info("Подключение к серверу установлено",
		logger.String("serverAddr", serverAddr))

	return &PubSubClient{
		conn:   conn,
		client: client,
	}, nil
}

func (c *PubSubClient) Close() error {
	logger.Debug("Закрытие клиентского подключения")
	err := c.conn.Close()
	if err != nil {
		logger.Error("Ошибка при закрытии клиентского подключения",
			logger.Err(err))
	} else {
		logger.Debug("Клиентское подключение закрыто")
	}
	return err
}

func (c *PubSubClient) Publish(ctx context.Context, key, data string) error {
	logger.Debug("Публикация сообщения",
		logger.String("key", key),
		logger.String("dataLength", strconv.Itoa(len(data))))

	_, err := c.client.Publish(ctx, &proto.PublishRequest{
		Key:  key,
		Data: data,
	})
	if err != nil {
		logger.Error("Не удалось опубликовать сообщение",
			logger.String("key", key),
			logger.Err(err))
		return fmt.Errorf("failed to publish: %v", err)
	}

	logger.Debug("Сообщение успешно опубликовано",
		logger.String("key", key))
	return nil
}

func (c *PubSubClient) Subscribe(ctx context.Context, key string, handler func(data string)) error {
	logger.Debug("Подписка на события",
		logger.String("key", key))

	req := &proto.SubscribeRequest{
		Key: key,
	}

	stream, err := c.client.Subscribe(ctx, req)
	if err != nil {
		logger.Error("Не удалось подписаться",
			logger.String("key", key),
			logger.Err(err))
		return fmt.Errorf("failed to subscribe: %v", err)
	}

	logger.Info("Подписка установлена",
		logger.String("key", key))

	// Читаем события из потока
	for {
		event, err := stream.Recv()
		if err == io.EOF {
			logger.Debug("Конец потока событий",
				logger.String("key", key))
			return nil
		}
		if err != nil {
			logger.Error("Ошибка при получении события",
				logger.String("key", key),
				logger.Err(err))
			return fmt.Errorf("error receiving event: %v", err)
		}

		logger.Debug("Получено событие",
			logger.String("key", key),
			logger.String("dataLength", strconv.Itoa(len(event.Data))))

		handler(event.Data)
	}
}

func (c *PubSubClient) SubscribeAsync(key string, handler func(data string)) (func(), error) {
	logger.Debug("Асинхронная подписка на события",
		logger.String("key", key))

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		err := c.Subscribe(ctx, key, handler)
		if err != nil && ctx.Err() == nil {
			logger.Error("Ошибка подписки",
				logger.String("key", key),
				logger.Err(err))
		}
	}()

	return cancel, nil
}
