package pubsub

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/rabbitmq/amqp091-go"
)

func PublishJSON[T any](ch *amqp091.Channel, exchange, key string, val T) error {
// marshall val to json bytes 
data, err := json.Marshal(val)
if err != nil {
	fmt.Errorf("could not marshal value: %w", err)
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