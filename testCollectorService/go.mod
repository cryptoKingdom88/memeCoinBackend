module testCollectorService

go 1.24.2

require (
	github.com/cryptoKingdom88/memeCoinBackend/shared v0.0.0-00010101000000-000000000000
	github.com/segmentio/kafka-go v0.4.48
)

replace github.com/cryptoKingdom88/memeCoinBackend/shared => ../shared

require (
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.15 // indirect
)
