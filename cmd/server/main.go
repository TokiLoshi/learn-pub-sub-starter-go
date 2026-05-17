package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	pubsub "github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	routing "github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)



func main() {
	fmt.Println("Starting Peril server...")
	connectionString := "amqp://guest:guest@localhost:5672/"
	connection, err := amqp.Dial(connectionString)

	if err != nil {
		log.Fatalf("could not connect %v", err)
	} 

	defer connection.Close()
	fmt.Println("Successfully connected!")

	ch, err := connection.Channel()
	if err != nil {
		log.Fatalf("error creating channel: %v", err)
	}

	err = pubsub.PublishJSON(
		ch,
		routing.ExchangePerilDirect,
		routing.PauseKey,
		routing.PlayingState{IsPaused: true},
	)
	if err != nil {
		log.Fatalf("could not publish pause message: %v", err)
	}
	fmt.Println("Pause message published")


	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<- sigChan
	fmt.Println("Shutting down")
}
