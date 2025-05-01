package server

import (
	"context"
	"strconv"
	"sync"

	"github.com/Cornpop456/vk_task/api/proto"
	"github.com/Cornpop456/vk_task/pkg/logger"
	"github.com/Cornpop456/vk_task/subpub"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// PubSubServer реализует gRPC сервис Pubsub, описанный в proto файле
type PubSubServer struct {
	proto.UnimplementedPubsubServer
	bus           subpub.SubPub
	mu            sync.Mutex
	subscriptions map[string][]chan string
}

// NewPubSubServer создает новый экземпляр PubSubServer
func NewPubSubServer() *PubSubServer {
	return &PubSubServer{
		bus:           subpub.NewSubPub(),
		subscriptions: make(map[string][]chan string),
	}
}

func (s *PubSubServer) Subscribe(req *proto.SubscribeRequest, stream proto.Pubsub_SubscribeServer) error {
	key := req.GetKey()
	if key == "" {
		logger.Warn("Попытка подписки с пустым ключом")
		return status.Error(codes.InvalidArgument, "ключ не может быть пустым")
	}

	logger.Debug("Получен запрос на подписку",
		logger.String("key", key))

	msgChan := make(chan string, 100)

	// Регистрируем канал в списке подписчиков для данного ключа
	s.mu.Lock()
	if _, ok := s.subscriptions[key]; !ok {
		s.subscriptions[key] = []chan string{}
	}
	s.subscriptions[key] = append(s.subscriptions[key], msgChan)
	s.mu.Unlock()

	// Создаем обработчик, который будет получать сообщения из subpub и отправлять их в канал
	sub, err := s.bus.Subscribe(key, func(msg interface{}) {
		if strMsg, ok := msg.(string); ok {
			select {
			case msgChan <- strMsg:
			default:
				logger.Warn("Буфер канала переполнен",
					logger.String("key", key))
			}
		}
	})

	if err != nil {
		logger.Error("Ошибка при подписке",
			logger.String("key", key),
			logger.Err(err))
		return status.Errorf(codes.Internal, "ошибка при подписке: %v", err)
	}

	logger.Info("Клиент подписан",
		logger.String("key", key))

	// Отключаем подписчика при завершении RPC
	defer func() {
		sub.Unsubscribe()
		close(msgChan)
		logger.Info("Отключение подписчика",
			logger.String("key", key))

		s.mu.Lock()
		defer s.mu.Unlock()

		// Удаляем канал из списка подписчиков
		if channels, ok := s.subscriptions[key]; ok {
			for i, ch := range channels {
				if ch == msgChan {
					s.subscriptions[key] = append(channels[:i], channels[i+1:]...)
					break
				}
			}

			// Если больше нет подписчиков для этого ключа, удаляем ключ
			if len(s.subscriptions[key]) == 0 {
				delete(s.subscriptions, key)
				logger.Debug("Удален последний подписчик для ключа",
					logger.String("key", key))
			}
		}
	}()

	// Читаем сообщения из канала и отправляем их в gRPC стрим
	for {
		select {
		case data, ok := <-msgChan:
			if !ok {
				logger.Debug("Канал закрыт",
					logger.String("key", key))
				return nil
			}

			// Отправляем данные клиенту
			if err := stream.Send(&proto.Event{
				Data: data,
			}); err != nil {
				logger.Error("Ошибка отправки данных клиенту",
					logger.String("key", key),
					logger.Err(err))
				return status.Errorf(codes.Internal, "ошибка отправки данных: %v", err)
			}

			logger.Debug("Отправлены данные клиенту",
				logger.String("key", key),
				logger.String("data", data))

		case <-stream.Context().Done():
			logger.Info("Контекст стрима завершен",
				logger.String("key", key),
				logger.Err(stream.Context().Err()))
			return status.Error(codes.Canceled, stream.Context().Err().Error())
		}
	}
}

// Publish публикует событие для всех подписчиков по ключу
func (s *PubSubServer) Publish(ctx context.Context, req *proto.PublishRequest) (*emptypb.Empty, error) {
	key := req.GetKey()
	data := req.GetData()

	if key == "" {
		logger.Warn("Попытка публикации с пустым ключом")
		return nil, status.Error(codes.InvalidArgument, "ключ не может быть пустым")
	}

	logger.Info("Публикация сообщения",
		logger.String("key", key),
		logger.String("dataLength", strconv.Itoa((len(data)))))

	err := s.bus.Publish(key, data)
	if err != nil {
		logger.Error("Ошибка публикации сообщения",
			logger.String("key", key),
			logger.Err(err))
		return nil, status.Errorf(codes.Internal, "ошибка при публикации: %v", err)
	}

	return &emptypb.Empty{}, nil
}

func (s *PubSubServer) Close(ctx context.Context) error {
	logger.Info("Закрытие PubSubServer")

	err := s.bus.Close(ctx)
	if err != nil {
		logger.Error("Ошибка при закрытии SubPub",
			logger.Err(err))
		return status.Errorf(codes.Internal, "ошибка при закрытии: %v", err)
	}

	logger.Info("PubSubServer успешно закрыт")
	return nil
}
