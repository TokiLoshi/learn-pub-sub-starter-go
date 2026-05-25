package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

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
	fmt.Println("Successfully connected!")

	ch, err := connection.Channel()
	if err != nil {
		log.Fatalf("error creating channel: %v", err)
	}

	gamelogic.PrintServerHelp()
	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}
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
	case "quit":
		fmt.Println("Quitting...")
		return
	default:
		fmt.Println("command not recognized... please try again")
		continue
		}
	}

	


	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<- sigChan
	fmt.Println("Shutting down")
}
