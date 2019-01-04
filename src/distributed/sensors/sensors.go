package main

// This package simulates single sensor by opening connection/channel to RabbitMQ
// Notifying on amq.fanout exchange about new sensor(name)
// create DataQ for publishing messages to the default exchange
// generate sample reading every X times per second
// encrypt the data and send to default exchange

import (
	"bytes"
	"distributed/dto"
	"distributed/qutils"
	"encoding/gob"
	"flag"
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/streadway/amqp"
)

var rabbitURL = "amqp://test:test@localhost:5672"

// this name is going to be used as routing key in rabbitmq so it should be unique
var sensorName = flag.String("name", "default", "Name of the sensor")
var freq = flag.Uint("freq", 5, "Number of data points to generate per seconds")
var max = flag.Float64("max", 1.0, "Max value of the generated value")
var min = flag.Float64("min", 1.0, "Max value of the generated value")
var step = flag.Float64("step", 0.1, "Allowed change per mesurement")
var randomness = rand.New(rand.NewSource(time.Now().UnixNano()))
var sensorValue = randomness.Float64()*(*max-*min) + *min
var nominalSensorValue = (*max+*min)/2 + *min

func main() {
	flag.Parse()

	// get rabbit connections/channel/queue
	egressConnection, egressChannel := qutils.GetConnChan(rabbitURL)
	defer egressConnection.Close()
	defer egressChannel.Close()

	// this will make sure that the data queue exists
	// even though we are not going to write to queue directly ,
	// we need to make sure that the queue exists because we should forward messages
	// from default exchange to the queue , if it is not exists the messages delivery will fail
	egressDataQ := qutils.GetQueue(*sensorName, egressChannel)

	notifyAboutNewSensor(*sensorName, egressChannel)

	// Generate sensor data and send over default exchange
	for range getTimeTickerChannel(*freq) {
		randomizeSensorReadingValue()
		encodedMessageBuffer := getEncodedMessage()
		sensorDataPublishing := amqp.Publishing{Body: encodedMessageBuffer.Bytes()}
		egressChannel.Publish(
			"",                   // exchange name - "" means default
			egressDataQ.Name,     //  key string -  routing key that determines which queue should get the message
			false,                //  mandatory bool - be sure that messages are delivered
			false,                //  immediate bool - fail if no consumers
			sensorDataPublishing) //  Publishing
		log.Printf("Sensor value sent: %v\n", sensorValue)
	}

}

func randomizeSensorReadingValue() {
	var maxStep, minStep float64
	if sensorValue < nominalSensorValue {
		maxStep = *step
		minStep = -1 * *step * (sensorValue - *min) / (nominalSensorValue - *min)
	} else {
		maxStep = *step * (*max - sensorValue) / (*max - nominalSensorValue)
		minStep = -1 * *step
	}

	sensorValue += randomness.Float64()*(maxStep-minStep) + minStep

}

func notifyAboutNewSensor(sensorName string, rabitChannel *amqp.Channel) {

	// notify about sensors queues
	newSensorNotification := amqp.Publishing{Body: []byte(sensorName)}
	rabitChannel.Publish(
		"amq.fanout", // fanout exchange that by default sends the messages to all the bound queues
		"",           // since all the clients are going to receive the message - there is no need in routing queue
		false, false,
		newSensorNotification)
}

func getTimeTickerChannel(freq uint) <-chan time.Time {

	// generate time ticks(signals) every timeDurationBetweenCycles
	timesPerSecond, _ := time.ParseDuration(strconv.Itoa(1000/int(freq)) + "ms")
	sensorReadingsIntervalsChannel := time.Tick(timesPerSecond)
	return sensorReadingsIntervalsChannel
}

func getEncodedMessage() *bytes.Buffer {
	sensorReadingMessage := dto.SensorMesssage{
		SensorName:  *sensorName,
		SensorValue: sensorValue,
		Time:        time.Now(),
	}
	// buffer to be used for serialization of messages
	messagesBuffer := new(bytes.Buffer)
	messagesEncoder := gob.NewEncoder(messagesBuffer)
	messagesEncoder.Encode(sensorReadingMessage)
	return messagesBuffer
}
