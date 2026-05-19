package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")
	connectionString := "amqp://guest:guest@localhost:5672/"
	connection, err := amqp.Dial(connectionString)
	if err != nil {
		log.Fatalf("could not connect %v", err)
	}

	defer connection.Close()
	fmt.Println("Successfully connected")


	userName, err := gamelogic.ClientWelcome()


	if err != nil {
		log.Fatalf("Could not find username: %v", err)
	}

		fmt.Println("Hello: ", userName)

	queueName := fmt.Sprintf("%s.%s", routing.PauseKey, userName)
	ch, queue, err := pubsub.DeclareAndBind(
		connection, 
		routing.ExchangePerilDirect,
		queueName,
		routing.PauseKey,
		pubsub.SimpleQueueTransient,
	) 
	if err != nil {
		log.Fatalf("could not declare and bind")
	}


	defer ch.Close()

	fmt.Printf("Quueu %s delcared and bound\n", queue.Name)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<- sigChan 
	fmt.Println("Shutting down")

	
}
