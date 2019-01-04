package dto

// DTO - data transfer objects
// this pacakge is going to contain all the relevant information about the object
// types that going to be transfered via rabbitmq

import (
	"time"
	"encoding/gob"
)

type SensorMesssage struct {
	SensorName string
	SensorValue float64
	Time       time.Time // when the reading was taken
}

func init(){
	// this will guarantee that consumers can send SensorMessage via rabbitmq
	gob.Register(SensorMesssage{})
}


