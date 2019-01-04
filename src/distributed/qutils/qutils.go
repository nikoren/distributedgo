package qutils

import (
	"fmt"
	"log"

	"github.com/streadway/amqp"
)

const SensorListQueue = "SensorList"

// FailOnError - fail on error
func FailOnError(err error, msg string) {
	if err != nil {

		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

// GetConnChan - establish connection and channel to rabbitMq
func GetConnChan(amqpURL string) (*amqp.Connection, *amqp.Channel) {
	conn, err := amqp.Dial(amqpURL)
	FailOnError(err, "Couldn't get connection from rabbit")

	ch, err := conn.Channel()
	FailOnError(err, "Couldn't get channel from connection")

	return conn, ch
}

// GetQueue - establish or get existing queue
func GetQueue(name string, ch *amqp.Channel) *amqp.Queue {

	// if this methods succeeds we get a queue that is bound to default exchange
	// the default exchange is of type direct, and it will routed directly to the output queue
	q, err := ch.QueueDeclare(
		name,
		false, //durable,
		false, // autoDelete messages with no active consumers
		false, // exclusive - make it only accessible from the same channel
		false, // noWait bool - do not create new queue if not exists
		nil)   // args amqp.Table - declares things like headers that would be matched by this queue
	FailOnError(err, "Failed to declare queue - hello")
	return &q
}

