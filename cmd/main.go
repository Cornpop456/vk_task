package main

import (
	"context"
	"fmt"
	"time"

	"github.com/Cornpop456/vk_task/subpub"
)

func main() {
	// Создаем новый EventBus
	bus := subpub.NewSubPub()

	// Подписываемся на событие
	bus.Subscribe("topic1", func(msg interface{}) {
		time.Sleep(1 * time.Second)
		fmt.Printf("1 topic 1 Получено сообщение: %v\n", msg)
	})

	bus.Subscribe("topic1", func(msg interface{}) {
		time.Sleep(1 * time.Second)
		fmt.Printf("2 topic 1 Получено сообщение: %v\n", msg)
	})

	bus.Subscribe("topic2", func(msg interface{}) {
		time.Sleep(1 * time.Second)
		fmt.Printf("1 topic 2 Получено сообщение: %v\n", msg)
	})

	bus.Subscribe("topic2", func(msg interface{}) {
		time.Sleep(1 * time.Second)
		fmt.Printf("2 topic 2 Получено сообщение: %v\n", msg)
	})

	bus.Subscribe("topic2", func(msg interface{}) {
		time.Sleep(1 * time.Second)
		fmt.Printf("3 topic 2 Получено сообщение: %v\n", msg)
	})

	// Публикуем сообщение
	bus.Publish("topic2", "ываыва")

	bus.Publish("topic1", "AAAAAAAA")

	bus.Publish("topic2", "Hello, subscribers!")

	bus.Publish("topic2", "AAAAAAAA")

	fmt.Println(bus.GetLenQueue())

	time.Sleep(2 * time.Second)

	// Закрываем EventBus
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := bus.Close(ctx)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("EventBus закрыт успешно")
	}
}
