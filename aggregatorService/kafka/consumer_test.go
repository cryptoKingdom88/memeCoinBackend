package kafka

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/cryptoKingdom88/memeCoinBackend/shared/packet"
)

func TestConsumer_Initialize(t *testing.T) {
	consumer := NewConsumer()
	
	// Test successful initialization
	err := consumer.Initialize([]string{"localhost:9092"}, "test-topic", "test-group")
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
	
	// Test empty brokers
	err = consumer.Initialize([]string{}, "test-topic", "test-group")
	if err == nil {
		t.Error("Expected error for empty brokers")
	}
	
	// Test empty topic
	err = consumer.Initialize([]string{"localhost:9092"}, "", "test-group")
	if err == nil {
		t.Error("Expected error for empty topic")
	}
	
	// Test empty consumer group
	err = consumer.Initialize([]string{"localhost:9092"}, "test-topic", "")
	if err == nil {
		t.Error("Expected error for empty consumer group")
	}
}

func TestConsumer_parseTradeMessage(t *testing.T) {
	consumer := &Consumer{}
	
	// Test single trade parsing
	singleTrade := packet.TokenTradeHistory{
		Token:        "0x123",
		Wallet:       "0x456",
		SellBuy:      "buy",
		NativeAmount: "1.5",
		TokenAmount:  "1000",
		PriceUsd:     "0.0015",
		TransTime:    "2023-01-01T00:00:00Z",
		TxHash:       "0x789",
	}
	
	singleTradeJSON, _ := json.Marshal(singleTrade)
	trades, err := consumer.parseTradeMessage(singleTradeJSON)
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
	if len(trades) != 1 {
		t.Errorf("Expected 1 trade, got: %d", len(trades))
	}
	
	// Test array of trades parsing
	tradeArray := []packet.TokenTradeHistory{singleTrade, singleTrade}
	arrayJSON, _ := json.Marshal(tradeArray)
	trades, err = consumer.parseTradeMessage(arrayJSON)
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
	if len(trades) != 2 {
		t.Errorf("Expected 2 trades, got: %d", len(trades))
	}
	
	// Test empty message
	_, err = consumer.parseTradeMessage([]byte{})
	if err == nil {
		t.Error("Expected error for empty message")
	}
	
	// Test invalid JSON
	_, err = consumer.parseTradeMessage([]byte("invalid json"))
	if err == nil {
		t.Error("Expected error for invalid JSON")
	}
}

func TestConsumer_validateTrade(t *testing.T) {
	consumer := &Consumer{}
	
	// Test valid trade
	validTrade := packet.TokenTradeHistory{
		Token:        "0x123",
		Wallet:       "0x456",
		SellBuy:      "buy",
		NativeAmount: "1.5",
		TokenAmount:  "1000",
		PriceUsd:     "0.0015",
		TransTime:    "2023-01-01T00:00:00Z",
		TxHash:       "0x789",
	}
	
	err := consumer.validateTrade(validTrade)
	if err != nil {
		t.Errorf("Expected no error for valid trade, got: %v", err)
	}
	
	// Test invalid sell_buy
	invalidTrade := validTrade
	invalidTrade.SellBuy = "invalid"
	err = consumer.validateTrade(invalidTrade)
	if err == nil {
		t.Error("Expected error for invalid sell_buy")
	}
	
	// Test empty token
	invalidTrade = validTrade
	invalidTrade.Token = ""
	err = consumer.validateTrade(invalidTrade)
	if err == nil {
		t.Error("Expected error for empty token")
	}
	
	// Test empty wallet
	invalidTrade = validTrade
	invalidTrade.Wallet = ""
	err = consumer.validateTrade(invalidTrade)
	if err == nil {
		t.Error("Expected error for empty wallet")
	}
}

func TestConsumer_GetConfig(t *testing.T) {
	consumer := NewConsumer()
	brokers := []string{"localhost:9092"}
	topic := "test-topic"
	group := "test-group"
	
	err := consumer.Initialize(brokers, topic, group)
	if err != nil {
		t.Fatalf("Failed to initialize consumer: %v", err)
	}
	
	config := consumer.(*Consumer).GetConfig()
	
	if config["topic"] != topic {
		t.Errorf("Expected topic %s, got %v", topic, config["topic"])
	}
	if config["consumer_group"] != group {
		t.Errorf("Expected consumer group %s, got %v", group, config["consumer_group"])
	}
	if config["is_running"] != false {
		t.Errorf("Expected is_running false, got %v", config["is_running"])
	}
}

func TestConsumer_HealthCheck(t *testing.T) {
	consumer := NewConsumer()
	ctx := context.Background()
	
	// Test health check on uninitialized consumer
	err := consumer.(*Consumer).HealthCheck(ctx)
	if err == nil {
		t.Error("Expected error for uninitialized consumer")
	}
	
	// Test health check on initialized but not running consumer
	err = consumer.Initialize([]string{"localhost:9092"}, "test-topic", "test-group")
	if err != nil {
		t.Fatalf("Failed to initialize consumer: %v", err)
	}
	
	err = consumer.(*Consumer).HealthCheck(ctx)
	if err == nil {
		t.Error("Expected error for non-running consumer")
	}
}

func TestConsumer_IsRunning(t *testing.T) {
	consumer := NewConsumer()
	
	// Test initial state
	if consumer.(*Consumer).IsRunning() {
		t.Error("Expected consumer to not be running initially")
	}
	
	// Initialize consumer
	err := consumer.Initialize([]string{"localhost:9092"}, "test-topic", "test-group")
	if err != nil {
		t.Fatalf("Failed to initialize consumer: %v", err)
	}
	
	// Should still not be running after initialization
	if consumer.(*Consumer).IsRunning() {
		t.Error("Expected consumer to not be running after initialization")
	}
}

// Mock processor for testing
type mockProcessor struct {
	processedTrades []packet.TokenTradeHistory
}

func (m *mockProcessor) ProcessTrades(ctx context.Context, trades []packet.TokenTradeHistory) error {
	m.processedTrades = append(m.processedTrades, trades...)
	return nil
}

func (m *mockProcessor) Initialize(redisManager interface{}, calculator interface{}, workerPool interface{}) error {
	return nil
}

func (m *mockProcessor) GetTokenProcessor(tokenAddress string) (interface{}, error) {
	return nil, nil
}

func (m *mockProcessor) Shutdown(ctx context.Context) error {
	return nil
}

func TestConsumer_StopWithTimeout(t *testing.T) {
	consumer := NewConsumer()
	
	// Test stopping non-running consumer
	err := consumer.(*Consumer).StopWithTimeout(time.Second)
	if err != nil {
		t.Errorf("Expected no error stopping non-running consumer, got: %v", err)
	}
	
	// Test multiple stops
	err = consumer.(*Consumer).StopWithTimeout(time.Second)
	if err != nil {
		t.Errorf("Expected no error on second stop, got: %v", err)
	}
}