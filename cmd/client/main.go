package main

import (
	"fmt"
	"log"
	"strconv"
	"time"

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
		handlerMove(gameState, channel),
	)

	if err != nil {
		log.Fatalf("could not subscribe to moves: %v", err)
	}

	err = pubsub.SubscribeJSON(
		connection, 
		routing.ExchangePerilTopic, 
		"war",
		routing.WarRecognitionsPrefix + ".*",
		pubsub.SimpleQueueDurable,
		handlerWar(gameState, channel),
	)

	if err != nil {
		log.Fatalf("could not subscribe to war: %v", err)
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
			if len(input) < 2{
				fmt.Println("convention is: spamming. [amount]")
				continue
			}
			spamAmount, err := strconv.Atoi(input[1])
			if err != nil {
				fmt.Println("amount must be a number")
				continue
			}
			for spam := range spamAmount {
				fmt.Printf("Sspammingggg %v", spam)
				msg := gamelogic.GetMaliciousLog()
				err := pubsub.PublishGob(
					channel,
					routing.ExchangePerilTopic,
					routing.GameLogSlug + "." + userName,
					msg,
				)
				if err != nil {
					fmt.Println("error publishing gob spam")
					continue
				}
			}
			
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

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	
	return func(ps routing.PlayingState) pubsub.AckType{
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.Ack
	}
}

func handlerMove(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.ArmyMove) pubsub.AckType{
	return func (move gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")
		moveOutcome := gs.HandleMove(move)
		switch moveOutcome {
		case gamelogic.MoveOutcomeSamePlayer:
			return pubsub.NackDiscard
		case gamelogic.MoveOutComeSafe:
			return pubsub.Ack
		case gamelogic.MoveOutcomeMakeWar:
			// publish a messsage to the topic 
			err := pubsub.PublishJSON(
				ch,
				routing.ExchangePerilTopic,  
				routing.WarRecognitionsPrefix + "." + gs.GetPlayerSnap().Username, 
				gamelogic.RecognitionOfWar{
					Attacker: move.Player,
					Defender: gs.GetPlayerSnap(),
				}, 
			)
			if err != nil {
				fmt.Printf("error publishing move: %v", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		
		}

fmt.Println("error: unknown move outcome")
		return pubsub.NackDiscard
	}

}

func handlerWar(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.RecognitionOfWar) pubsub.AckType {

return func(rw gamelogic.RecognitionOfWar) pubsub.AckType {
	defer fmt.Print("> ")

	outcome, winner, loser := gs.HandleWar(rw)

	switch outcome {
	case gamelogic.WarOutcomeNotInvolved:
		return pubsub.NackRequeue
	case gamelogic.WarOutcomeNoUnits:
		return pubsub.NackDiscard
	case gamelogic.WarOutcomeOpponentWon:
		err := pubsub.PublishGob(
			ch, 
			routing.ExchangePerilTopic, 
						routing.GameLogSlug + "." + rw.Attacker.Username, 
			routing.GameLog{
			CurrentTime: time.Now(),
			Message: fmt.Sprintf("%s won a war against %s", winner, loser),
				Username: gs.Player.Username,
		})
		if err != nil {
			return pubsub.NackRequeue
		}
		return pubsub.Ack 
	case gamelogic.WarOutcomeYouWon:
			err := pubsub.PublishGob(
				ch, 
				routing.ExchangePerilTopic, 
								routing.GameLogSlug + "." + rw.Attacker.Username, 
				routing.GameLog{
			CurrentTime: time.Now(),
			Message: fmt.Sprintf("%s won a war against %s", winner, loser),
			Username: winner,
		})
		if err != nil {
			return pubsub.NackRequeue
		}
		return pubsub.Ack 
	case gamelogic.WarOutcomeDraw:
			err := pubsub.PublishGob(
				ch, 
				routing.ExchangePerilTopic, 
				routing.GameLogSlug + "." + rw.Attacker.Username, 
				routing.GameLog{
			CurrentTime: time.Now(),
			Message: fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser),
			Username: gs.Player.Username,
		})
		if err != nil {
			return pubsub.NackRequeue
		}
		return pubsub.Ack
	}
	fmt.Println("error: unknown war outcome")
	return pubsub.NackDiscard
}

	
}