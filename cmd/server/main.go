package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
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
	fmt.Println("Successfully connected to Rabbit!")

	ch, err := connection.Channel()
	if err != nil {
		log.Fatalf("error creating channel: %v", err)
	}

	_, _, err = pubsub.DeclareAndBind(
		connection, 
		routing.ExchangePerilTopic, 
		routing.GameLogSlug,
		routing.GameLogSlug + ".*",
		pubsub.SimpleQueueDurable,
	)
	if err != nil {
		log.Fatalf("error publishing topic: %v", err)
	}

	gamelogic.PrintServerHelp()

	for {
		input := gamelogic.GetInput()

		if len(input) == 0 {
			continue 
		}

		command := input[0]
		
		switch command {
	case "pause":
		fmt.Println("sending pause message") 
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
case "resume":
		fmt.Println("sending resume message")

		err = pubsub.PublishJSON(
			ch, 
			routing.ExchangePerilDirect,
			routing.PauseKey, 
			routing.PlayingState{IsPaused: false},
		)
		if err != nil {
			log.Fatalf("could not publish resume message: %v", err)
		}
	case "quit":
		fmt.Println("Quitting...")
		return
	default:
		fmt.Println("command not recognized... please try again")
		continue
		}
	}

}
