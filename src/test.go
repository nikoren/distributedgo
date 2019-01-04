package main

import (
	"fmt"
	"log"

	"github.com/streadway/amqp"
)

func main() {
	messageGeneratorServer()
	messagesClient()
}

func failOnError(err error, msg string) {
	if err != nil {

		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}

}
func getQueue() (*amqp.Connection, *amqp.Channel, *amqp.Queue) {
	conn, err := amqp.Dial("amqp://test:test@localhost:5672")
	failOnError(err, "Couldn't get connection from rabbit")

	ch, err := conn.Channel()
	failOnError(err, "Couldn't get channel from connection")

	// if this methods succeeds we get a queue that is bound to default exchange
	// the default exchange is of type direct, and it will routed directly to the output queue
	q, err := ch.QueueDeclare(
		"hello",
		false, //durable,
		false, // autoDelete messages with no active consumers
		false, // exclusive - make it only accessible from the same channel
		false, // noWait bool - do not create new queue if not exists
		nil)   // args amqp.Table - declares things like headers that would be matched by this queue
	failOnError(err, "Failed to declare queue - hello")

	return conn, ch, &q
}

func messageGeneratorServer() {
	// get connection
	conn, ch, q := getQueue()
	defer conn.Close()
	defer ch.Close()

	// create message
	msg := amqp.Publishing{
		ContentType: "text/plan",
		Body:        []byte("HELLO TO RABBIT"),
	}

	for i := 0; i < 10000; i++ {
		// publish the message
		ch.Publish("", // exchange name - "" means default
			q.Name, //  key string -  routing key that determines which queue should get the message
			false,  //  mandatory bool - be sure that messages are delivered
			false,  //  immediate bool - be sure when the message was delivered
			msg)    //  msg Publishing
	}
}

func messagesClient() {

	// get connection
	conn, ch, q := getQueue()
	defer conn.Close()
	defer ch.Close()

	msgsChan, err := ch.Consume( // register channel to listen for messages
		q.Name, // queue string,
		"",     // consumer string - just to let Rabbit know who is the consumer , "" makes it
		true,   // autoAck bool - automatically acknowledge removal of messages from queue as they received
		false,  // exclusive bool - to make sure no other client can access to the this channel
		false,  // noLocal bool  - prevents Rabbitmq from sending messages to clients on same connection as sender
		false,  // noWait bool,
		nil)    // args amqp.Table

	failOnError(err, "Failed to register consumer channel")
	for message := range msgsChan {
		log.Printf("Received message with body %s", message.Body)
	}
	log.Println("Finished printing message")
}
