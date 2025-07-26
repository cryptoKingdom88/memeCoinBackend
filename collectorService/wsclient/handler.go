package wsclient

import (
	"log"

	"github.com/cryptoKingdom88/memeCoinBackend/collectorService/kafka"
)

func HandleMessage(msg []byte) {
	log.Printf("Received Msg: %s", string(msg))

	// In here parser.Parse()
	// parsed := parser.Parse(msg)
	// kafka.Produce(parsed)

	kafka.Produce(msg) // Temp Code
}
