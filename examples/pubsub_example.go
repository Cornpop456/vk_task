package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/Cornpop456/vk_task/client"
	"github.com/Cornpop456/vk_task/pkg/logger"
)

func main() {
	logFilePath := ""
	logger.Init("info", logFilePath)

	serverAddr := "localhost:50051"

	pubsubClient, err := client.NewPubSubClient(serverAddr)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer pubsubClient.Close()

	fmt.Println("Connected to PubSub server at", serverAddr)

	var wg sync.WaitGroup

	// Демонстрация 1: подписка на топик и получение сообщений
	wg.Add(1)
	go func() {
		defer wg.Done()
		topic := "demo-topic-1"

		fmt.Printf("Demo 1: Subscribing to '%s'\n", topic)

		// Подписываемся на тему
		cancel, err := pubsubClient.SubscribeAsync(topic, func(data string) {
			fmt.Printf("Demo 1: Received message from '%s': %s\n", topic, data)
		})

		if err != nil {
			log.Printf("Demo 1: Failed to subscribe: %v", err)
			return
		}

		time.Sleep(10 * time.Second)
		cancel()
		fmt.Printf("Demo 1: Unsubscribed from '%s'\n", topic)
	}()

	// Демонстрация 2: несколько подписчиков на один топик
	wg.Add(1)
	go func() {
		defer wg.Done()
		topic := "demo-topic-2"

		fmt.Printf("Demo 2: Creating multiple subscribers for '%s'\n", topic)

		cancel1, err := pubsubClient.SubscribeAsync(topic, func(data string) {
			fmt.Printf("Demo 2: Subscriber 1 received from '%s': %s\n", topic, data)
		})
		if err != nil {
			log.Printf("Demo 2: Failed to subscribe (1): %v", err)
			return
		}

		cancel2, err := pubsubClient.SubscribeAsync(topic, func(data string) {
			fmt.Printf("Demo 2: Subscriber 2 received from '%s': %s\n", topic, data)
		})
		if err != nil {
			log.Printf("Demo 2: Failed to subscribe (2): %v", err)
			cancel1()
			return
		}

		time.Sleep(10 * time.Second)
		cancel1()
		cancel2()
		fmt.Printf("Demo 2: Unsubscribed all from '%s'\n", topic)
	}()

	// Демонстрация 3: публикация сообщений в разные топики
	wg.Add(1)
	go func() {
		defer wg.Done()

		fmt.Println("Demo 3: Publishing messages to various topics")

		// Даем время на установку подписок
		time.Sleep(2 * time.Second)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Публикуем в первый топик
		topic1 := "demo-topic-1"
		for i := 1; i <= 3; i++ {
			message := fmt.Sprintf("Message %d for topic 1", i)
			if err := pubsubClient.Publish(ctx, topic1, message); err != nil {
				log.Printf("Failed to publish to '%s': %v", topic1, err)
			} else {
				fmt.Printf("Published to '%s': %s\n", topic1, message)
			}
			time.Sleep(1 * time.Second)
		}

		// Публикуем во второй топик
		topic2 := "demo-topic-2"
		for i := 1; i <= 3; i++ {
			message := fmt.Sprintf("Message %d for topic 2", i)
			if err := pubsubClient.Publish(ctx, topic2, message); err != nil {
				log.Printf("Failed to publish to '%s': %v", topic2, err)
			} else {
				fmt.Printf("Published to '%s': %s\n", topic2, message)
			}
			time.Sleep(1 * time.Second)
		}

		// Публикуем в топик, на который никто не подписан
		topicUnused := "unused-topic"
		message := "This message will not be delivered to any subscriber"
		if err := pubsubClient.Publish(ctx, topicUnused, message); err != nil {
			log.Printf("Failed to publish to '%s': %v", topicUnused, err)
		} else {
			fmt.Printf("Published to '%s': %s\n", topicUnused, message)
		}
	}()

	wg.Wait()
	fmt.Println("All demonstrations completed")
}
