package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Cornpop456/vk_task/subpub"
)

func main() {
	// Создаем новый EventBus
	bus := subpub.NewSubPub()

	// Подписываемся на событие
	subscription, err := bus.Subscribe("topic1", func(msg interface{}) {
		fmt.Printf("Получено сообщение: %v\n", msg)
	})
	if err != nil {
		log.Fatal(err)
	}

	// Публикуем сообщение
	err = bus.Publish("topic1", "Hello, subscribers!")
	if err != nil {
		log.Fatal(err)
	}

	// Немного ждем, чтобы обработать сообщения
	time.Sleep(1 * time.Second)

	// Отписываемся
	subscription.Unsubscribe()

	// Публикуем еще одно сообщение, но никто уже не получит его
	err = bus.Publish("topic1", "This won't be received")
	if err != nil {
		log.Fatal(err)
	}

	// Закрываем EventBus
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err = bus.Close(ctx)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("EventBus закрыт успешно")
}
