package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/rabbitmq/amqp091-go"
	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int 
const (
	SimpleQueueDurable SimpleQueueType = iota 
	SimpleQueueTransient 
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string, 
	queueType SimpleQueueType, 
) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("could not create channel: %w", err)
	}

	queue, err := ch.QueueDeclare(
		queueName,
		queueType == SimpleQueueDurable,
		queueType == SimpleQueueTransient,
		queueType == SimpleQueueTransient,
		false,
		nil,

	)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("could not declare queue %w", err)
	}
	err = ch.QueueBind(queueName, key, exchange, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("could not bind queue %w", err)
	}
	return ch, queue, nil
}

func PublishJSON[T any](ch *amqp091.Channel, exchange, key string, val T) error {
// marshall val to json bytes 
data, err := json.Marshal(val)
if err != nil {
	return fmt.Errorf("could not marshal value: %w", err)
}
// use channel's publish with context method to publish message:
// set ctx to context.background 
ctx := context.Background()
return ch.PublishWithContext(
	ctx,
	exchange, 
	key, 
	false, 
	false, 
	amqp091.Publishing{
		ContentType: "application.json()", 
		Body: data,
	},
)


}

func SubscribeJSON[T any](conn *amqp.Connection, exchange, queueName, key string, queue SimpleQueueType, handler func(T),) error {
	ch, validQueue, err := DeclareAndBind(conn, exchange, queueName, key, queue)
	if err != nil {
		log.Fatalf("Could not declare and bind!")
	}
	consumerString := ""
	deliveries, err := ch.Consume(
		validQueue.Name, 
		consumerString, 
		false, 
		false, 
		false, 
		false, 
		nil)

		go func() {
			for delivery := range deliveries {
				var target T
				err := json.Unmarshal(delivery.Body, &target)
				if err != nil {
					log.Fatalf("Could not unmarshall body")
				}
				handler(target)
				delivery.Ack(false)
			}
		}()

		return nil 
}

