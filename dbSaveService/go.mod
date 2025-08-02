module github.com/cryptoKingdom88/memeCoinBackend/dbSaveService

go 1.24.2

require (
	github.com/cryptoKingdom88/memeCoinBackend/shared v0.0.0
	github.com/joho/godotenv v1.5.1
	github.com/lib/pq v1.10.9
	github.com/segmentio/kafka-go v0.4.47
)

require (
	github.com/klauspost/compress v1.15.9 // indirect
	github.com/pierrec/lz4/v4 v4.1.15 // indirect
)

replace github.com/cryptoKingdom88/memeCoinBackend/shared => ../shared
