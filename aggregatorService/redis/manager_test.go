package redis

import (
	"context"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"aggregatorService/models"
)

// TestRedisContainer manages a Redis test container
type TestRedisContainer struct {
	container testcontainers.Container
	host      string
	port      string
}

// NewTestRedisContainer creates a new Redis test container
func NewTestRedisContainer(ctx context.Context) (*TestRedisContainer, error) {
	req := testcontainers.ContainerRequest{
		Image:        "redis:7-alpine",
		ExposedPorts: []string{"6379/tcp"},
		WaitingFor:   wait.ForLog("Ready to accept connections"),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, err
	}

	host, err := container.Host(ctx)
	if err != nil {
		return nil, err
	}

	port, err := container.MappedPort(ctx, "6379")
	if err != nil {
		return nil, err
	}

	return &TestRedisContainer{
		container: container,
		host:      host,
		port:      port.Port(),
	}, nil
}

// GetConnectionString returns the Redis connection string
func (trc *TestRedisContainer) GetConnectionString() string {
	return trc.host + ":" + trc.port
}

// Terminate terminates the test container
func (trc *TestRedisContainer) Terminate(ctx context.Context) error {
	return trc.container.Terminate(ctx)
}

func TestRedisManager_Connect(t *testing.T) {
	ctx := context.Background()
	
	// Start Redis container
	redisContainer, err := NewTestRedisContainer(ctx)
	if err != nil {
		t.Fatalf("Failed to start Redis container: %v", err)
	}
	defer redisContainer.Terminate(ctx)

	// Test successful connection
	manager := NewManager(redisContainer.GetConnectionString(), "", 0, 10)
	err = manager.Connect(ctx)
	if err != nil {
		t.Errorf("Expected successful connection, got error: %v", err)
	}
	defer manager.Close()

	// Test ping
	err = manager.Ping(ctx)
	if err != nil {
		t.Errorf("Expected successful ping, got error: %v", err)
	}
}

func TestRedisManager_ConnectFailure(t *testing.T) {
	ctx := context.Background()
	
	// Test connection to non-existent Redis
	manager := NewManager("localhost:9999", "", 0, 10)
	err := manager.Connect(ctx)
	if err == nil {
		t.Error("Expected connection error for non-existent Redis")
	}
}

func TestRedisManager_TokenData(t *testing.T) {
	ctx := context.Background()
	
	// Start Redis container
	redisContainer, err := NewTestRedisContainer(ctx)
	if err != nil {
		t.Fatalf("Failed to start Redis container: %v", err)
	}
	defer redisContainer.Terminate(ctx)

	// Connect to Redis
	manager := NewManager(redisContainer.GetConnectionString(), "", 0, 10)
	err = manager.Connect(ctx)
	if err != nil {
		t.Fatalf("Failed to connect to Redis: %v", err)
	}
	defer manager.Close()

	tokenAddress := "0x123456789"
	
	// Test getting non-existent token data
	tokenData, err := manager.GetTokenData(ctx, tokenAddress)
	if err != nil {
		t.Errorf("Expected no error for non-existent token, got: %v", err)
	}
	if tokenData.TokenAddress != tokenAddress {
		t.Errorf("Expected token address %s, got %s", tokenAddress, tokenData.TokenAddress)
	}
	if len(tokenData.Trades) != 0 {
		t.Errorf("Expected empty trades for non-existent token, got %d trades", len(tokenData.Trades))
	}

	// Create test token data
	now := time.Now()
	testTokenData := &models.TokenData{
		TokenAddress: tokenAddress,
		Trades: []models.TradeData{
			{
				Token:        tokenAddress,
				Wallet:       "0xabc",
				SellBuy:      "buy",
				NativeAmount: 100.0,
				TokenAmount:  1000.0,
				PriceUsd:     0.1,
				TransTime:    now,
				TxHash:       "0x1",
			},
			{
				Token:        tokenAddress,
				Wallet:       "0xdef",
				SellBuy:      "sell",
				NativeAmount: 50.0,
				TokenAmount:  500.0,
				PriceUsd:     0.1,
				TransTime:    now.Add(-1 * time.Minute),
				TxHash:       "0x2",
			},
		},
		Indices: map[string]int64{
			"1min":  0,
			"5min":  0,
			"15min": 0,
		},
		Aggregates: map[string]*models.AggregateData{
			"1min": {
				SellCount:   0,
				BuyCount:    2,
				SellVolume:  0.0,
				BuyVolume:   150.0,
				TotalVolume: 150.0,
				LastUpdate:  now,
			},
		},
		LastUpdate: now,
	}

	// Test updating token data
	err = manager.UpdateTokenData(ctx, tokenAddress, testTokenData)
	if err != nil {
		t.Errorf("Expected no error updating token data, got: %v", err)
	}

	// Test retrieving updated token data
	retrievedData, err := manager.GetTokenData(ctx, tokenAddress)
	if err != nil {
		t.Errorf("Expected no error retrieving token data, got: %v", err)
	}

	// Verify retrieved data
	if retrievedData.TokenAddress != tokenAddress {
		t.Errorf("Expected token address %s, got %s", tokenAddress, retrievedData.TokenAddress)
	}
	if len(retrievedData.Trades) != 2 {
		t.Errorf("Expected 2 trades, got %d", len(retrievedData.Trades))
	}
	if len(retrievedData.Indices) != 3 {
		t.Errorf("Expected 3 indices, got %d", len(retrievedData.Indices))
	}
	if len(retrievedData.Aggregates) != 1 {
		t.Errorf("Expected 1 aggregate, got %d", len(retrievedData.Aggregates))
	}

	// Verify specific trade data
	if retrievedData.Trades[0].SellBuy != "buy" {
		t.Errorf("Expected first trade to be 'buy', got %s", retrievedData.Trades[0].SellBuy)
	}
	if retrievedData.Trades[0].NativeAmount != 100.0 {
		t.Errorf("Expected first trade amount 100.0, got %f", retrievedData.Trades[0].NativeAmount)
	}

	// Verify aggregate data
	oneMinAgg := retrievedData.Aggregates["1min"]
	if oneMinAgg == nil {
		t.Error("Expected 1min aggregate to exist")
	} else {
		if oneMinAgg.BuyCount != 2 {
			t.Errorf("Expected buy count 2, got %d", oneMinAgg.BuyCount)
		}
		if oneMinAgg.TotalVolume != 150.0 {
			t.Errorf("Expected total volume 150.0, got %f", oneMinAgg.TotalVolume)
		}
	}
}

func TestRedisManager_AddTrade(t *testing.T) {
	ctx := context.Background()
	
	// Start Redis container
	redisContainer, err := NewTestRedisContainer(ctx)
	if err != nil {
		t.Fatalf("Failed to start Redis container: %v", err)
	}
	defer redisContainer.Terminate(ctx)

	// Connect to Redis
	manager := NewManager(redisContainer.GetConnectionString(), "", 0, 10)
	err = manager.Connect(ctx)
	if err != nil {
		t.Fatalf("Failed to connect to Redis: %v", err)
	}
	defer manager.Close()

	tokenAddress := "0x123456789"
	
	// Create test trade
	trade := models.TradeData{
		Token:        tokenAddress,
		Wallet:       "0xabc",
		SellBuy:      "buy",
		NativeAmount: 100.0,
		TokenAmount:  1000.0,
		PriceUsd:     0.1,
		TransTime:    time.Now(),
		TxHash:       "0x1",
	}

	// Test adding trade
	err = manager.AddTrade(ctx, tokenAddress, trade)
	if err != nil {
		t.Errorf("Expected no error adding trade, got: %v", err)
	}

	// Verify trade was added
	trades, err := manager.GetTrades(ctx, tokenAddress, 0, -1)
	if err != nil {
		t.Errorf("Expected no error getting trades, got: %v", err)
	}
	if len(trades) != 1 {
		t.Errorf("Expected 1 trade, got %d", len(trades))
	}
	if trades[0].TxHash != "0x1" {
		t.Errorf("Expected trade hash 0x1, got %s", trades[0].TxHash)
	}

	// Test adding another trade
	trade2 := models.TradeData{
		Token:        tokenAddress,
		Wallet:       "0xdef",
		SellBuy:      "sell",
		NativeAmount: 50.0,
		TokenAmount:  500.0,
		PriceUsd:     0.1,
		TransTime:    time.Now(),
		TxHash:       "0x2",
	}

	err = manager.AddTrade(ctx, tokenAddress, trade2)
	if err != nil {
		t.Errorf("Expected no error adding second trade, got: %v", err)
	}

	// Verify both trades exist
	trades, err = manager.GetTrades(ctx, tokenAddress, 0, -1)
	if err != nil {
		t.Errorf("Expected no error getting trades, got: %v", err)
	}
	if len(trades) != 2 {
		t.Errorf("Expected 2 trades, got %d", len(trades))
	}
}

func TestRedisManager_IndicesAndAggregates(t *testing.T) {
	ctx := context.Background()
	
	// Start Redis container
	redisContainer, err := NewTestRedisContainer(ctx)
	if err != nil {
		t.Fatalf("Failed to start Redis container: %v", err)
	}
	defer redisContainer.Terminate(ctx)

	// Connect to Redis
	manager := NewManager(redisContainer.GetConnectionString(), "", 0, 10)
	err = manager.Connect(ctx)
	if err != nil {
		t.Fatalf("Failed to connect to Redis: %v", err)
	}
	defer manager.Close()

	tokenAddress := "0x123456789"
	
	// Test setting and getting index
	err = manager.SetIndex(ctx, tokenAddress, "1min", 10)
	if err != nil {
		t.Errorf("Expected no error setting index, got: %v", err)
	}

	index, err := manager.GetIndex(ctx, tokenAddress, "1min")
	if err != nil {
		t.Errorf("Expected no error getting index, got: %v", err)
	}
	if index != 10 {
		t.Errorf("Expected index 10, got %d", index)
	}

	// Test getting non-existent index
	index, err = manager.GetIndex(ctx, tokenAddress, "nonexistent")
	if err != nil {
		t.Errorf("Expected no error getting non-existent index, got: %v", err)
	}
	if index != 0 {
		t.Errorf("Expected index 0 for non-existent, got %d", index)
	}

	// Test setting and getting aggregate
	aggregate := &models.AggregateData{
		SellCount:   1,
		BuyCount:    2,
		SellVolume:  50.0,
		BuyVolume:   100.0,
		TotalVolume: 150.0,
		LastUpdate:  time.Now(),
	}

	err = manager.SetAggregate(ctx, tokenAddress, "1min", aggregate)
	if err != nil {
		t.Errorf("Expected no error setting aggregate, got: %v", err)
	}

	retrievedAgg, err := manager.GetAggregate(ctx, tokenAddress, "1min")
	if err != nil {
		t.Errorf("Expected no error getting aggregate, got: %v", err)
	}
	if retrievedAgg.BuyCount != 2 {
		t.Errorf("Expected buy count 2, got %d", retrievedAgg.BuyCount)
	}
	if retrievedAgg.TotalVolume != 150.0 {
		t.Errorf("Expected total volume 150.0, got %f", retrievedAgg.TotalVolume)
	}

	// Test getting non-existent aggregate
	retrievedAgg, err = manager.GetAggregate(ctx, tokenAddress, "nonexistent")
	if err != nil {
		t.Errorf("Expected no error getting non-existent aggregate, got: %v", err)
	}
	if retrievedAgg == nil {
		t.Error("Expected non-nil aggregate for non-existent timeframe")
	}
	if retrievedAgg.GetTotalTradeCount() != 0 {
		t.Errorf("Expected empty aggregate, got trade count %d", retrievedAgg.GetTotalTradeCount())
	}
}

func TestRedisManager_ActiveTokens(t *testing.T) {
	ctx := context.Background()
	
	// Start Redis container
	redisContainer, err := NewTestRedisContainer(ctx)
	if err != nil {
		t.Fatalf("Failed to start Redis container: %v", err)
	}
	defer redisContainer.Terminate(ctx)

	// Connect to Redis
	manager := NewManager(redisContainer.GetConnectionString(), "", 0, 10)
	err = manager.Connect(ctx)
	if err != nil {
		t.Fatalf("Failed to connect to Redis: %v", err)
	}
	defer manager.Close()

	// Test getting active tokens when none exist
	tokens, err := manager.GetActiveTokens(ctx)
	if err != nil {
		t.Errorf("Expected no error getting active tokens, got: %v", err)
	}
	if len(tokens) != 0 {
		t.Errorf("Expected 0 active tokens, got %d", len(tokens))
	}

	// Add some tokens
	token1 := "0x123"
	token2 := "0x456"
	
	err = manager.UpdateLastUpdate(ctx, token1, time.Now())
	if err != nil {
		t.Errorf("Expected no error updating last update, got: %v", err)
	}
	
	err = manager.UpdateLastUpdate(ctx, token2, time.Now())
	if err != nil {
		t.Errorf("Expected no error updating last update, got: %v", err)
	}

	// Test getting active tokens
	tokens, err = manager.GetActiveTokens(ctx)
	if err != nil {
		t.Errorf("Expected no error getting active tokens, got: %v", err)
	}
	if len(tokens) != 2 {
		t.Errorf("Expected 2 active tokens, got %d", len(tokens))
	}

	// Verify tokens are in the list
	tokenMap := make(map[string]bool)
	for _, token := range tokens {
		tokenMap[token] = true
	}
	if !tokenMap[token1] {
		t.Errorf("Expected token %s to be active", token1)
	}
	if !tokenMap[token2] {
		t.Errorf("Expected token %s to be active", token2)
	}
}

func TestRedisManager_LastUpdate(t *testing.T) {
	ctx := context.Background()
	
	// Start Redis container
	redisContainer, err := NewTestRedisContainer(ctx)
	if err != nil {
		t.Fatalf("Failed to start Redis container: %v", err)
	}
	defer redisContainer.Terminate(ctx)

	// Connect to Redis
	manager := NewManager(redisContainer.GetConnectionString(), "", 0, 10)
	err = manager.Connect(ctx)
	if err != nil {
		t.Fatalf("Failed to connect to Redis: %v", err)
	}
	defer manager.Close()

	tokenAddress := "0x123456789"
	
	// Test getting non-existent last update
	lastUpdate, err := manager.GetLastUpdate(ctx, tokenAddress)
	if err != nil {
		t.Errorf("Expected no error getting non-existent last update, got: %v", err)
	}
	if !lastUpdate.IsZero() {
		t.Errorf("Expected zero time for non-existent last update, got %v", lastUpdate)
	}

	// Test setting and getting last update
	now := time.Now().Truncate(time.Second) // Truncate to avoid precision issues
	err = manager.UpdateLastUpdate(ctx, tokenAddress, now)
	if err != nil {
		t.Errorf("Expected no error updating last update, got: %v", err)
	}

	retrievedTime, err := manager.GetLastUpdate(ctx, tokenAddress)
	if err != nil {
		t.Errorf("Expected no error getting last update, got: %v", err)
	}
	if !retrievedTime.Equal(now) {
		t.Errorf("Expected last update %v, got %v", now, retrievedTime)
	}
}

func TestRedisManager_TTL(t *testing.T) {
	ctx := context.Background()
	
	// Start Redis container
	redisContainer, err := NewTestRedisContainer(ctx)
	if err != nil {
		t.Fatalf("Failed to start Redis container: %v", err)
	}
	defer redisContainer.Terminate(ctx)

	// Connect to Redis
	manager := NewManager(redisContainer.GetConnectionString(), "", 0, 10)
	err = manager.Connect(ctx)
	if err != nil {
		t.Fatalf("Failed to connect to Redis: %v", err)
	}
	defer manager.Close()

	tokenAddress := "0x123456789"
	
	// Add some data first
	err = manager.UpdateLastUpdate(ctx, tokenAddress, time.Now())
	if err != nil {
		t.Errorf("Expected no error updating last update, got: %v", err)
	}

	// Test setting TTL
	ttl := 30 * time.Second
	err = manager.SetTTL(ctx, tokenAddress, ttl)
	if err != nil {
		t.Errorf("Expected no error setting TTL, got: %v", err)
	}

	// TTL test completed - we can't easily verify TTL through the interface
	// but the SetTTL method should work without error
}

func TestRedisManager_AtomicOperations(t *testing.T) {
	ctx := context.Background()
	
	// Start Redis container
	redisContainer, err := NewTestRedisContainer(ctx)
	if err != nil {
		t.Fatalf("Failed to start Redis container: %v", err)
	}
	defer redisContainer.Terminate(ctx)

	// Connect to Redis
	manager := NewManager(redisContainer.GetConnectionString(), "", 0, 10)
	err = manager.Connect(ctx)
	if err != nil {
		t.Fatalf("Failed to connect to Redis: %v", err)
	}
	defer manager.Close()

	tokenAddress := "0x123456789"
	
	// Test atomic operations through the public interface
	// We can test ExecuteAtomicUpdate with a simple Lua script
	script := `
		local key = KEYS[1]
		local value = ARGV[1]
		redis.call('SET', key, value)
		return 'OK'
	`
	
	err = manager.ExecuteAtomicUpdate(ctx, script, []string{"test:atomic"}, []interface{}{"test_value"})
	if err != nil {
		t.Errorf("Expected no error in atomic update, got: %v", err)
	}
}

func TestRedisManager_TokenExists(t *testing.T) {
	ctx := context.Background()
	
	// Start Redis container
	redisContainer, err := NewTestRedisContainer(ctx)
	if err != nil {
		t.Fatalf("Failed to start Redis container: %v", err)
	}
	defer redisContainer.Terminate(ctx)

	// Connect to Redis
	manager := NewManager(redisContainer.GetConnectionString(), "", 0, 10)
	err = manager.Connect(ctx)
	if err != nil {
		t.Fatalf("Failed to connect to Redis: %v", err)
	}
	defer manager.Close()

	tokenAddress := "0x123456789"
	
	// Test token existence indirectly through GetLastUpdate
	// Non-existent token should return zero time
	lastUpdate, err := manager.GetLastUpdate(ctx, tokenAddress)
	if err != nil {
		t.Errorf("Expected no error getting last update, got: %v", err)
	}
	if !lastUpdate.IsZero() {
		t.Error("Expected zero time for non-existent token")
	}

	// Add token data
	now := time.Now()
	err = manager.UpdateLastUpdate(ctx, tokenAddress, now)
	if err != nil {
		t.Errorf("Expected no error updating last update, got: %v", err)
	}

	// Test existing token
	lastUpdate, err = manager.GetLastUpdate(ctx, tokenAddress)
	if err != nil {
		t.Errorf("Expected no error getting last update, got: %v", err)
	}
	if lastUpdate.IsZero() {
		t.Error("Expected non-zero time for existing token")
	}
}

func TestRedisManager_TokenDataOperations(t *testing.T) {
	ctx := context.Background()
	
	// Start Redis container
	redisContainer, err := NewTestRedisContainer(ctx)
	if err != nil {
		t.Fatalf("Failed to start Redis container: %v", err)
	}
	defer redisContainer.Terminate(ctx)

	// Connect to Redis
	manager := NewManager(redisContainer.GetConnectionString(), "", 0, 10)
	err = manager.Connect(ctx)
	if err != nil {
		t.Fatalf("Failed to connect to Redis: %v", err)
	}
	defer manager.Close()

	tokenAddress := "0x123456789"
	
	// Add some token data
	err = manager.UpdateLastUpdate(ctx, tokenAddress, time.Now())
	if err != nil {
		t.Errorf("Expected no error updating last update, got: %v", err)
	}
	
	err = manager.SetIndex(ctx, tokenAddress, "1min", 10)
	if err != nil {
		t.Errorf("Expected no error setting index, got: %v", err)
	}

	// Verify token data exists
	lastUpdate, err := manager.GetLastUpdate(ctx, tokenAddress)
	if err != nil {
		t.Errorf("Expected no error getting last update, got: %v", err)
	}
	if lastUpdate.IsZero() {
		t.Error("Expected token to exist before operations")
	}

	index, err := manager.GetIndex(ctx, tokenAddress, "1min")
	if err != nil {
		t.Errorf("Expected no error getting index, got: %v", err)
	}
	if index != 10 {
		t.Errorf("Expected index 10, got %d", index)
	}
}

func TestRedisManager_BasicOperations(t *testing.T) {
	ctx := context.Background()
	
	// Start Redis container
	redisContainer, err := NewTestRedisContainer(ctx)
	if err != nil {
		t.Fatalf("Failed to start Redis container: %v", err)
	}
	defer redisContainer.Terminate(ctx)

	// Connect to Redis
	manager := NewManager(redisContainer.GetConnectionString(), "", 0, 10)
	err = manager.Connect(ctx)
	if err != nil {
		t.Fatalf("Failed to connect to Redis: %v", err)
	}
	defer manager.Close()

	// Test basic operations work without error
	tokenAddress := "0x123456789"
	
	// Test setting and getting basic data
	err = manager.UpdateLastUpdate(ctx, tokenAddress, time.Now())
	if err != nil {
		t.Errorf("Expected no error updating last update, got: %v", err)
	}
	
	_, err = manager.GetLastUpdate(ctx, tokenAddress)
	if err != nil {
		t.Errorf("Expected no error getting last update, got: %v", err)
	}
}

func TestRedisManager_InvalidData(t *testing.T) {
	ctx := context.Background()
	
	// Start Redis container
	redisContainer, err := NewTestRedisContainer(ctx)
	if err != nil {
		t.Fatalf("Failed to start Redis container: %v", err)
	}
	defer redisContainer.Terminate(ctx)

	// Connect to Redis
	manager := NewManager(redisContainer.GetConnectionString(), "", 0, 10)
	err = manager.Connect(ctx)
	if err != nil {
		t.Fatalf("Failed to connect to Redis: %v", err)
	}
	defer manager.Close()

	tokenAddress := "0x123456789"
	
	// Test adding invalid trade data
	invalidTrade := models.TradeData{
		Token:        "", // Invalid: empty token
		Wallet:       "0xabc",
		SellBuy:      "buy",
		NativeAmount: 100.0,
		TokenAmount:  1000.0,
		PriceUsd:     0.1,
		TransTime:    time.Now(),
		TxHash:       "0x1",
	}

	err = manager.AddTrade(ctx, tokenAddress, invalidTrade)
	if err == nil {
		t.Error("Expected error for invalid trade data")
	}

	// Test setting invalid aggregate data
	invalidAggregate := &models.AggregateData{
		SellCount:   -1, // Invalid: negative count
		BuyCount:    2,
		SellVolume:  50.0,
		BuyVolume:   100.0,
		TotalVolume: 150.0,
		LastUpdate:  time.Now(),
	}

	err = manager.SetAggregate(ctx, tokenAddress, "1min", invalidAggregate)
	if err == nil {
		t.Error("Expected error for invalid aggregate data")
	}
}