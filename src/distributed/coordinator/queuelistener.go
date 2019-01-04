package coordinator

/*
This package listens for new sensors messages on amq.fanout exchange
once message is received it extracts sensor name from the message,
(as documented in sensors package the queues with the actual readings  
of sensors create queues which are bound to default exchange and named by the name of the sensor)
when the name of new sensor is identified , this package will try to connect to readings queue available
on default exchange with the name of the sensor , get delivery channel, and start consume 
messages from this channel 
*/
import (
	"bytes"
	"distributed/dto"
	"distributed/qutils"
	"encoding/gob"
	"fmt"

	"github.com/streadway/amqp"
)

const brokerUrl = "amqp://test:test@localhost:5672"

// QListener - discovers data queues , receives their messages ,
// and  translates them into events in events aggregators
type QListener struct {
	conn    *amqp.Connection
	ch      *amqp.Channel
	sources map[string]<-chan amqp.Delivery
}

// NewQListener - Constrator of QListener
func NewQListener() *QListener {
	ql := QListener{
		sources: make(map[string]<-chan amqp.Delivery),
	}
	ql.conn, ql.ch = qutils.GetConnChan(brokerUrl)
	return &ql
}

// DiscoverNewSensors - listens on queue and discover new sensors
func (ql *QListener) DiscoverNewSensors() {
	// Create new queue - let rabbitmq name it
	sensorsDiscoveryQueue := qutils.GetQueue("", ql.ch)
	// since by default queues are bound to deafult exchange we need to rebind it to fanout exchange
	ql.ch.QueueBind(sensorsDiscoveryQueue.Name, "", "amq.fanout", false, nil)
	newSensorsUpDeliveriesChannel, err := ql.ch.Consume(sensorsDiscoveryQueue.Name, "", true, false, false, false, nil)
	qutils.FailOnError(err, "Can't create sensorsDiscoveryQueue")

	// listen for messages that indicate that new channel came online
	for newSensorCameOnlineDelivery := range newSensorsUpDeliveriesChannel {
		newSensorName := string(newSensorCameOnlineDelivery.Body)

		fmt.Printf("New sensor %s came online\n", newSensorName)
		// Get Delivery channel and consume messages from queue named by the new sensor name that just came online
		sensorMeasurementsDeliveriesChannel, err := ql.ch.Consume(newSensorName, "", true, false, false, false, nil)
		qutils.FailOnError(err, "Error consuming queue "+string(newSensorCameOnlineDelivery.Body))

		if ql.sources[newSensorName] == nil {
			ql.sources[newSensorName] = sensorMeasurementsDeliveriesChannel
			go ql.ConsumeMessagesFromDeliveryChannel(sensorMeasurementsDeliveriesChannel)
		}
	}
}

// ConsumeMessagesFromDeliveryChannel -
func (ql *QListener) ConsumeMessagesFromDeliveryChannel(msgs <-chan amqp.Delivery) {
	for msg := range msgs {
		reader := bytes.NewReader(msg.Body)
		decoder := gob.NewDecoder(reader)
		newSensorMessage := new(dto.SensorMesssage)
		decoder.Decode(newSensorMessage)
		fmt.Printf("New message received name %s , sensor value %f ,\n",
			newSensorMessage.SensorName, newSensorMessage.SensorValue)
	}
}
