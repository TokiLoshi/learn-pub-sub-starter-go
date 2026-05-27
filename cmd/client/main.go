package main

import (
	"fmt"
	"log"

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

	channel, err := connection.Channel()
	if err != nil {
		log.Fatalf("error creating channel: %v", err)
	}


	userName, err := gamelogic.ClientWelcome()


	if err != nil {
		log.Fatalf("Could not find username: %v", err)
	}

		fmt.Println("Hello: ", userName)

	gameState := gamelogic.NewGameState(userName)

	// queueName := fmt.Sprintf("%s.%s", routing.PauseKey, userName)
	// ch, queue, err := pubsub.DeclareAndBind(
	// 	connection, 
	// 	routing.ExchangePerilDirect,
	// 	queueName,
	// 	routing.PauseKey,
	// 	pubsub.SimpleQueueTransient,
	// ) 
	err = pubsub.SubscribeJSON(
		connection, 
		routing.ExchangePerilDirect,
		"pause." + userName, 
		routing.PauseKey, 
		pubsub.SimpleQueueTransient,
		handlerPause(gameState),
	)
	if err != nil {
		log.Fatalf("could not declare and bind %v", err)
	}


	err = pubsub.SubscribeJSON(
		connection,
		routing.ExchangePerilTopic, 
		routing.ArmyMovesPrefix + "." + userName,
		routing.ArmyMovesPrefix + ".*",
		pubsub.SimpleQueueTransient, 
		handlerMove(gameState),
	)

	if err != nil {
		log.Fatalf("could not subscribe to moves: %v", err)
	}
	// defer ch.Close()

	// fmt.Printf("Quueu %s delcared and bound\n", queue.Name)

	// ch, err := connection.Channel()
	// if err != nil {
	// 	log.Fatalf("error creatign channel: %v", err)
	// }


	for {

		input := gamelogic.GetInput()

		if len(input) == 0 {
			continue
		}

		command := input[0]
		switch command {
		case "spawn":
			fmt.Println("Spawn command received")
			// allow user to add new unit to map 
			err = gameState.CommandSpawn(input)
			if err != nil {
				log.Fatalf("Could not spawn")
			}
		case "move":
			fmt.Println("move command")
			// allows player to move units to new location 
		
			move, err := gameState.CommandMove(input)
			if err != nil {
				fmt.Println("Could not make move:")
				continue
			}
			fmt.Printf("Succesfully made move: %v", move)
			// if successful print message it works 
			err = pubsub.PublishJSON(
				channel, 
				routing.ExchangePerilTopic, 
				routing.ArmyMovesPrefix + "." + userName, 
				move, 
			)
		case "status":
			fmt.Println("status command received")
			gameState.CommandStatus() 
		case "help":
			fmt.Println("help command received")
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet")
		case "quit":
			fmt.Println("quitting")
			gamelogic.PrintQuit()
			return 
		default:
			fmt.Println("command not recognized... please try again!")
			continue
		}

	}


	
}

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) {
	
	return func(ps routing.PlayingState) {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
	}
}

func handlerMove(gs *gamelogic.GameState) func(gamelogic.ArmyMove) {
	return func (move gamelogic.ArmyMove) {
		defer fmt.Print("> ")
		gs.HandleMove(move)
	}

}