package main

import (
	"distributed/coordinator"
	"fmt"
)

func main() {
	ql := coordinator.NewQListener()
	go ql.DiscoverNewSensors()
	var a string
	fmt.Scanln(&a)
}
