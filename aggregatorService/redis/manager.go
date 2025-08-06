package redis

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"aggregatorService/interfaces"
	"aggregatorService/models"
)

// Manager implements the RedisManager interface
type Manager struct {
	client *redis.Client
	config *redis.Options
	
	// Error handling components
	errorHandler    *ErrorHandler
	circuitBreaker  *CircuitBreaker
	fallbackManager *FallbackManager
	
	// Lua scripts for atomic operations
	updateTokenDataScript *redis.Script
	addTradeScript        *redis.Script
	updateIndicesScript   *redis.Script
}

// NewManager creates a new Redis manager with connection pooling
func NewManager(addr, password string, db, poolSize int) interfaces.RedisManager {
	manager := &Manager{
		config: &redis.Options{
			Addr:         addr,
			Password:     password,
			DB:           db,
			PoolSize:     poolSize,
			MinIdleConns: poolSize / 4, // Keep 25% of pool as idle connections
			MaxRetries:   3,
			DialTimeout:  5 * time.Second,
			ReadTimeout:  3 * time.Second,
			WriteTimeout: 3 * time.Second,
			PoolTimeout:  4 * time.Second,
		},
	}
	
	// Initialize error handling components
	manager.circuitBreaker = NewCircuitBreaker(5, 30*time.Second, 60*time.Second)
	manager.errorHandler = NewErrorHandler(manager.circuitBreaker, DefaultRetryConfig())
	manager.fallbackManager = NewFallbackManager()
	
	// Initialize Lua scripts for atomic operations
	manager.initLuaScripts()
	
	return manager
}

// initLuaScripts initializes Lua scripts for atomic operations
func (m *Manager) initLuaScripts() {
	// Script to atomically update token data (trades, indices, aggregates)
	m.updateTokenDataScript = redis.NewScript(`
		local token_address = KEYS[1]
		local trade_data = ARGV[1]
		local indices_data = ARGV[2]
		local aggregates_data = ARGV[3]
		local last_update = ARGV[4]
		local ttl_seconds = tonumber(ARGV[5])
		
		-- Add trade to trades list
		redis.call('LPUSH', 'token:' .. token_address .. ':trades', trade_data)
		
		-- Update indices (JSON string with all timeframes)
		redis.call('SET', 'token:' .. token_address .. ':indices', indices_data)
		
		-- Update aggregates (JSON string with all timeframes)
		redis.call('SET', 'token:' .. token_address .. ':aggregates', aggregates_data)
		
		-- Update last update timestamp
		redis.call('SET', 'token:' .. token_address .. ':last_update', last_update)
		
		-- Set TTL on all keys
		redis.call('EXPIRE', 'token:' .. token_address .. ':trades', ttl_seconds)
		redis.call('EXPIRE', 'token:' .. token_address .. ':indices', ttl_seconds)
		redis.call('EXPIRE', 'token:' .. token_address .. ':aggregates', ttl_seconds)
		redis.call('EXPIRE', 'token:' .. token_address .. ':last_update', ttl_seconds)
		
		return 'OK'
	`)
	
	// Script to atomically add a trade and update TTL
	m.addTradeScript = redis.NewScript(`
		local token_address = KEYS[1]
		local trade_data = ARGV[1]
		local ttl_seconds = tonumber(ARGV[2])
		
		-- Add trade to trades list
		redis.call('LPUSH', 'token:' .. token_address .. ':trades', trade_data)
		
		-- Set TTL on trades list
		redis.call('EXPIRE', 'token:' .. token_address .. ':trades', ttl_seconds)
		
		return redis.call('LLEN', 'token:' .. token_address .. ':trades')
	`)
	
	// Script to atomically update indices and aggregates
	m.updateIndicesScript = redis.NewScript(`
		local token_address = KEYS[1]
		local indices_data = ARGV[1]
		local aggregates_data = ARGV[2]
		local last_update = ARGV[3]
		local ttl_seconds = tonumber(ARGV[4])
		
		-- Update indices
		redis.call('SET', 'token:' .. token_address .. ':indices', indices_data)
		
		-- Update aggregates
		redis.call('SET', 'token:' .. token_address .. ':aggregates', aggregates_data)
		
		-- Update last update timestamp
		redis.call('SET', 'token:' .. token_address .. ':last_update', last_update)
		
		-- Set TTL on all keys
		redis.call('EXPIRE', 'token:' .. token_address .. ':indices', ttl_seconds)
		redis.call('EXPIRE', 'token:' .. token_address .. ':aggregates', ttl_seconds)
		redis.call('EXPIRE', 'token:' .. token_address .. ':last_update', ttl_seconds)
		
		return 'OK'
	`)
}

// Connect establishes connection to Redis with connection pooling
func (m *Manager) Connect(ctx context.Context) error {
	return m.errorHandler.ExecuteWithRetry(ctx, "connect", func() error {
		m.client = redis.NewClient(m.config)
		
		// Test connection
		if err := m.Ping(ctx); err != nil {
			return fmt.Errorf("failed to connect to Redis: %w", err)
		}
		
		log.Printf("Connected to Redis at %s with pool size %d", m.config.Addr, m.config.PoolSize)
		return nil
	})
}

// Close closes the Redis connection
func (m *Manager) Close() error {
	if m.client != nil {
		return m.client.Close()
	}
	return nil
}

// Ping tests the Redis connection
func (m *Manager) Ping(ctx context.Context) error {
	if m.client == nil {
		return fmt.Errorf("Redis client not connected")
	}
	
	return m.errorHandler.ExecuteWithRetry(ctx, "ping", func() error {
		return m.client.Ping(ctx).Err()
	})
}

// UpdateTokenData updates complete token data atomically
func (m *Manager) UpdateTokenData(ctx context.Context, tokenAddress string, data *models.TokenData) error {
	if m.client == nil {
		return fmt.Errorf("Redis client not connected")
	}
	
	return m.errorHandler.ExecuteWithRetry(ctx, "update_token_data", func() error {
		pipeline := m.client.Pipeline()
		
		// Store trades list
		tradesKey := fmt.Sprintf("token:%s:trades", tokenAddress)
		pipeline.Del(ctx, tradesKey) // Clear existing trades
		
		if len(data.Trades) > 0 {
			tradeStrings := make([]interface{}, len(data.Trades))
			for i, trade := range data.Trades {
				tradeJSON, err := trade.ToJSON()
				if err != nil {
					return fmt.Errorf("failed to serialize trade %d: %w", i, err)
				}
				tradeStrings[i] = string(tradeJSON)
			}
			pipeline.LPush(ctx, tradesKey, tradeStrings...)
		}
		
		// Store indices
		if len(data.Indices) > 0 {
			indicesJSON, err := models.SerializeIndices(data.Indices)
			if err != nil {
				return fmt.Errorf("failed to serialize indices: %w", err)
			}
			indicesKey := fmt.Sprintf("token:%s:indices", tokenAddress)
			pipeline.Set(ctx, indicesKey, string(indicesJSON), 0)
		}
		
		// Store aggregates
		if len(data.Aggregates) > 0 {
			aggregatesJSON, err := models.SerializeAggregates(data.Aggregates)
			if err != nil {
				return fmt.Errorf("failed to serialize aggregates: %w", err)
			}
			aggregatesKey := fmt.Sprintf("token:%s:aggregates", tokenAddress)
			pipeline.Set(ctx, aggregatesKey, string(aggregatesJSON), 0)
		}
		
		// Store last update timestamp
		lastUpdateKey := fmt.Sprintf("token:%s:last_update", tokenAddress)
		pipeline.Set(ctx, lastUpdateKey, data.LastUpdate.Format(time.RFC3339), 0)
		
		// Set TTL on all keys (1 hour as per requirements)
		ttl := 1 * time.Hour
		pipeline.Expire(ctx, tradesKey, ttl)
		pipeline.Expire(ctx, fmt.Sprintf("token:%s:indices", tokenAddress), ttl)
		pipeline.Expire(ctx, fmt.Sprintf("token:%s:aggregates", tokenAddress), ttl)
		pipeline.Expire(ctx, lastUpdateKey, ttl)
		
		// Execute pipeline
		_, err := pipeline.Exec(ctx)
		if err != nil {
			// Store in fallback cache if Redis fails
			m.fallbackManager.Store(fmt.Sprintf("token_data:%s", tokenAddress), data)
			return fmt.Errorf("failed to update token data: %w", err)
		}
		
		return nil
	})
}

// GetTokenData retrieves complete token data
func (m *Manager) GetTokenData(ctx context.Context, tokenAddress string) (*models.TokenData, error) {
	if m.client == nil {
		return nil, fmt.Errorf("Redis client not connected")
	}
	
	var tokenData *models.TokenData
	
	err := m.errorHandler.ExecuteWithRetry(ctx, "get_token_data", func() error {
		tokenData = &models.TokenData{
			TokenAddress: tokenAddress,
			Trades:       make([]models.TradeData, 0),
			Indices:      make(map[string]int64),
			Aggregates:   make(map[string]*models.AggregateData),
		}
		
		// Get trades
		tradesKey := fmt.Sprintf("token:%s:trades", tokenAddress)
		tradesResult := m.client.LRange(ctx, tradesKey, 0, -1)
		if tradesResult.Err() != nil && tradesResult.Err() != redis.Nil {
			return fmt.Errorf("failed to get trades: %w", tradesResult.Err())
		}
		
		tradeStrings := tradesResult.Val()
		for _, tradeStr := range tradeStrings {
			var trade models.TradeData
			if err := trade.FromJSON([]byte(tradeStr)); err != nil {
				log.Printf("Warning: failed to parse trade data: %v", err)
				continue
			}
			tokenData.Trades = append(tokenData.Trades, trade)
		}
		
		// Get indices
		indicesKey := fmt.Sprintf("token:%s:indices", tokenAddress)
		indicesResult := m.client.Get(ctx, indicesKey)
		if indicesResult.Err() == nil {
			indices, err := models.DeserializeIndices([]byte(indicesResult.Val()))
			if err != nil {
				log.Printf("Warning: failed to parse indices: %v", err)
			} else {
				tokenData.Indices = indices
			}
		}
		
		// Get aggregates
		aggregatesKey := fmt.Sprintf("token:%s:aggregates", tokenAddress)
		aggregatesResult := m.client.Get(ctx, aggregatesKey)
		if aggregatesResult.Err() == nil {
			aggregates, err := models.DeserializeAggregates([]byte(aggregatesResult.Val()))
			if err != nil {
				log.Printf("Warning: failed to parse aggregates: %v", err)
			} else {
				tokenData.Aggregates = aggregates
			}
		}
		
		// Get last update timestamp
		lastUpdateKey := fmt.Sprintf("token:%s:last_update", tokenAddress)
		lastUpdateResult := m.client.Get(ctx, lastUpdateKey)
		if lastUpdateResult.Err() == nil {
			if lastUpdate, err := time.Parse(time.RFC3339, lastUpdateResult.Val()); err == nil {
				tokenData.LastUpdate = lastUpdate
			}
		}
		
		return nil
	})
	
	// If Redis fails, try fallback cache
	if err != nil {
		if fallbackData, exists := m.fallbackManager.Retrieve(fmt.Sprintf("token_data:%s", tokenAddress)); exists {
			if cachedData, ok := fallbackData.(*models.TokenData); ok {
				log.Printf("Retrieved token data from fallback cache for token: %s", tokenAddress)
				return cachedData, nil
			}
		}
		return nil, err
	}
	
	return tokenData, nil
}

// AddTrade adds a trade to the token's trade list
func (m *Manager) AddTrade(ctx context.Context, tokenAddress string, trade models.TradeData) error {
	if m.client == nil {
		return fmt.Errorf("Redis client not connected")
	}
	
	// Validate trade data
	if err := trade.ValidateTradeData(); err != nil {
		return fmt.Errorf("invalid trade data: %w", err)
	}
	
	return m.errorHandler.ExecuteWithRetry(ctx, "add_trade", func() error {
		// Use atomic add trade script
		_, err := m.AtomicAddTrade(ctx, tokenAddress, trade, 1*time.Hour)
		if err != nil {
			// Store in fallback cache if Redis fails
			fallbackKey := fmt.Sprintf("pending_trade:%s:%d", tokenAddress, time.Now().UnixNano())
			m.fallbackManager.Store(fallbackKey, trade)
			return fmt.Errorf("failed to add trade: %w", err)
		}
		
		return nil
	})
}

// GetTrades retrieves trades for a token within a range
func (m *Manager) GetTrades(ctx context.Context, tokenAddress string, start, end int64) ([]models.TradeData, error) {
	if m.client == nil {
		return nil, fmt.Errorf("Redis client not connected")
	}
	
	tradesKey := fmt.Sprintf("token:%s:trades", tokenAddress)
	
	// Get trades from Redis list (LRANGE uses 0-based indexing)
	result := m.client.LRange(ctx, tradesKey, start, end)
	if result.Err() != nil {
		if result.Err() == redis.Nil {
			return []models.TradeData{}, nil
		}
		return nil, fmt.Errorf("failed to get trades: %w", result.Err())
	}
	
	tradeStrings := result.Val()
	trades := make([]models.TradeData, 0, len(tradeStrings))
	
	for i, tradeStr := range tradeStrings {
		var trade models.TradeData
		if err := trade.FromJSON([]byte(tradeStr)); err != nil {
			log.Printf("Warning: failed to parse trade at index %d: %v", i, err)
			continue
		}
		trades = append(trades, trade)
	}
	
	return trades, nil
}

// SetIndex sets the index for a timeframe
func (m *Manager) SetIndex(ctx context.Context, tokenAddress, timeframe string, index int64) error {
	if m.client == nil {
		return fmt.Errorf("Redis client not connected")
	}
	
	// Get current indices
	indices, err := m.getAllIndices(ctx, tokenAddress)
	if err != nil {
		return fmt.Errorf("failed to get current indices: %w", err)
	}
	
	// Update the specific timeframe index
	indices[timeframe] = index
	
	// Serialize and store updated indices
	indicesJSON, err := models.SerializeIndices(indices)
	if err != nil {
		return fmt.Errorf("failed to serialize indices: %w", err)
	}
	
	indicesKey := fmt.Sprintf("token:%s:indices", tokenAddress)
	result := m.client.Set(ctx, indicesKey, string(indicesJSON), 1*time.Hour)
	if result.Err() != nil {
		return fmt.Errorf("failed to set index: %w", result.Err())
	}
	
	return nil
}

// GetIndex gets the index for a timeframe
func (m *Manager) GetIndex(ctx context.Context, tokenAddress, timeframe string) (int64, error) {
	if m.client == nil {
		return 0, fmt.Errorf("Redis client not connected")
	}
	
	indices, err := m.getAllIndices(ctx, tokenAddress)
	if err != nil {
		return 0, fmt.Errorf("failed to get indices: %w", err)
	}
	
	index, exists := indices[timeframe]
	if !exists {
		return 0, nil // Return 0 if index doesn't exist
	}
	
	return index, nil
}

// SetAggregate sets the aggregate data for a timeframe
func (m *Manager) SetAggregate(ctx context.Context, tokenAddress, timeframe string, aggregate *models.AggregateData) error {
	if m.client == nil {
		return fmt.Errorf("Redis client not connected")
	}
	
	// Validate aggregate data
	if err := aggregate.ValidateAggregateData(); err != nil {
		return fmt.Errorf("invalid aggregate data: %w", err)
	}
	
	// Get current aggregates
	aggregates, err := m.getAllAggregates(ctx, tokenAddress)
	if err != nil {
		return fmt.Errorf("failed to get current aggregates: %w", err)
	}
	
	// Update the specific timeframe aggregate
	aggregates[timeframe] = aggregate
	
	// Serialize and store updated aggregates
	aggregatesJSON, err := models.SerializeAggregates(aggregates)
	if err != nil {
		return fmt.Errorf("failed to serialize aggregates: %w", err)
	}
	
	aggregatesKey := fmt.Sprintf("token:%s:aggregates", tokenAddress)
	result := m.client.Set(ctx, aggregatesKey, string(aggregatesJSON), 1*time.Hour)
	if result.Err() != nil {
		return fmt.Errorf("failed to set aggregate: %w", result.Err())
	}
	
	return nil
}

// GetAggregate gets the aggregate data for a timeframe
func (m *Manager) GetAggregate(ctx context.Context, tokenAddress, timeframe string) (*models.AggregateData, error) {
	if m.client == nil {
		return nil, fmt.Errorf("Redis client not connected")
	}
	
	aggregates, err := m.getAllAggregates(ctx, tokenAddress)
	if err != nil {
		return nil, fmt.Errorf("failed to get aggregates: %w", err)
	}
	
	aggregate, exists := aggregates[timeframe]
	if !exists {
		return models.NewAggregateData(), nil // Return empty aggregate if doesn't exist
	}
	
	return aggregate, nil
}

// SetTTL sets TTL for token data
func (m *Manager) SetTTL(ctx context.Context, tokenAddress string, duration time.Duration) error {
	if m.client == nil {
		return fmt.Errorf("Redis client not connected")
	}
	
	// Set TTL on all token-related keys
	keys := []string{
		fmt.Sprintf("token:%s:trades", tokenAddress),
		fmt.Sprintf("token:%s:indices", tokenAddress),
		fmt.Sprintf("token:%s:aggregates", tokenAddress),
		fmt.Sprintf("token:%s:last_update", tokenAddress),
	}
	
	pipeline := m.client.Pipeline()
	for _, key := range keys {
		pipeline.Expire(ctx, key, duration)
	}
	
	_, err := pipeline.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to set TTL: %w", err)
	}
	
	return nil
}

// GetActiveTokens retrieves list of active tokens
func (m *Manager) GetActiveTokens(ctx context.Context) ([]string, error) {
	if m.client == nil {
		return nil, fmt.Errorf("Redis client not connected")
	}
	
	var tokens []string
	
	err := m.errorHandler.ExecuteWithRetry(ctx, "get_active_tokens", func() error {
		// Scan for all token keys using the pattern "token:*:last_update"
		// This gives us all active tokens that have been updated
		pattern := "token:*:last_update"
		
		tokens = []string{} // Reset tokens slice
		iter := m.client.Scan(ctx, 0, pattern, 0).Iterator()
		
		for iter.Next(ctx) {
			key := iter.Val()
			// Extract token address from key: "token:{address}:last_update"
			parts := strings.Split(key, ":")
			if len(parts) >= 3 {
				tokenAddress := parts[1]
				tokens = append(tokens, tokenAddress)
			}
		}
		
		if err := iter.Err(); err != nil {
			return fmt.Errorf("failed to scan active tokens: %w", err)
		}
		
		return nil
	})
	
	if err != nil {
		return nil, err
	}
	
	return tokens, nil
}

// UpdateLastUpdate updates the last update timestamp for a token
func (m *Manager) UpdateLastUpdate(ctx context.Context, tokenAddress string, timestamp time.Time) error {
	if m.client == nil {
		return fmt.Errorf("Redis client not connected")
	}
	
	lastUpdateKey := fmt.Sprintf("token:%s:last_update", tokenAddress)
	result := m.client.Set(ctx, lastUpdateKey, timestamp.Format(time.RFC3339), 1*time.Hour)
	
	if result.Err() != nil {
		return fmt.Errorf("failed to update last update timestamp: %w", result.Err())
	}
	
	return nil
}

// GetLastUpdate gets the last update timestamp for a token
func (m *Manager) GetLastUpdate(ctx context.Context, tokenAddress string) (time.Time, error) {
	if m.client == nil {
		return time.Time{}, fmt.Errorf("Redis client not connected")
	}
	
	lastUpdateKey := fmt.Sprintf("token:%s:last_update", tokenAddress)
	result := m.client.Get(ctx, lastUpdateKey)
	
	if result.Err() != nil {
		if result.Err() == redis.Nil {
			return time.Time{}, nil // Return zero time if key doesn't exist
		}
		return time.Time{}, fmt.Errorf("failed to get last update timestamp: %w", result.Err())
	}
	
	timestamp, err := time.Parse(time.RFC3339, result.Val())
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse timestamp: %w", err)
	}
	
	return timestamp, nil
}

// getAllIndices retrieves all indices for a token
func (m *Manager) getAllIndices(ctx context.Context, tokenAddress string) (map[string]int64, error) {
	indicesKey := fmt.Sprintf("token:%s:indices", tokenAddress)
	result := m.client.Get(ctx, indicesKey)
	
	if result.Err() != nil {
		if result.Err() == redis.Nil {
			return make(map[string]int64), nil // Return empty map if key doesn't exist
		}
		return nil, fmt.Errorf("failed to get indices: %w", result.Err())
	}
	
	indices, err := models.DeserializeIndices([]byte(result.Val()))
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize indices: %w", err)
	}
	
	return indices, nil
}

// getAllAggregates retrieves all aggregates for a token
func (m *Manager) getAllAggregates(ctx context.Context, tokenAddress string) (map[string]*models.AggregateData, error) {
	aggregatesKey := fmt.Sprintf("token:%s:aggregates", tokenAddress)
	result := m.client.Get(ctx, aggregatesKey)
	
	if result.Err() != nil {
		if result.Err() == redis.Nil {
			return make(map[string]*models.AggregateData), nil // Return empty map if key doesn't exist
		}
		return nil, fmt.Errorf("failed to get aggregates: %w", result.Err())
	}
	
	aggregates, err := models.DeserializeAggregates([]byte(result.Val()))
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize aggregates: %w", err)
	}
	
	return aggregates, nil
}

// GetTradeCount returns the number of trades for a token
func (m *Manager) GetTradeCount(ctx context.Context, tokenAddress string) (int64, error) {
	if m.client == nil {
		return 0, fmt.Errorf("Redis client not connected")
	}
	
	tradesKey := fmt.Sprintf("token:%s:trades", tokenAddress)
	result := m.client.LLen(ctx, tradesKey)
	
	if result.Err() != nil {
		if result.Err() == redis.Nil {
			return 0, nil
		}
		return 0, fmt.Errorf("failed to get trade count: %w", result.Err())
	}
	
	return result.Val(), nil
}

// DeleteTokenData removes all data for a token
func (m *Manager) DeleteTokenData(ctx context.Context, tokenAddress string) error {
	if m.client == nil {
		return fmt.Errorf("Redis client not connected")
	}
	
	keys := []string{
		fmt.Sprintf("token:%s:trades", tokenAddress),
		fmt.Sprintf("token:%s:indices", tokenAddress),
		fmt.Sprintf("token:%s:aggregates", tokenAddress),
		fmt.Sprintf("token:%s:last_update", tokenAddress),
	}
	
	result := m.client.Del(ctx, keys...)
	if result.Err() != nil {
		return fmt.Errorf("failed to delete token data: %w", result.Err())
	}
	
	return nil
}

// TokenExists checks if a token has any data in Redis
func (m *Manager) TokenExists(ctx context.Context, tokenAddress string) (bool, error) {
	if m.client == nil {
		return false, fmt.Errorf("Redis client not connected")
	}
	
	lastUpdateKey := fmt.Sprintf("token:%s:last_update", tokenAddress)
	result := m.client.Exists(ctx, lastUpdateKey)
	
	if result.Err() != nil {
		return false, fmt.Errorf("failed to check token existence: %w", result.Err())
	}
	
	return result.Val() > 0, nil
}

// GetTokenTTL returns the TTL for a token's data
func (m *Manager) GetTokenTTL(ctx context.Context, tokenAddress string) (time.Duration, error) {
	if m.client == nil {
		return 0, fmt.Errorf("Redis client not connected")
	}
	
	lastUpdateKey := fmt.Sprintf("token:%s:last_update", tokenAddress)
	result := m.client.TTL(ctx, lastUpdateKey)
	
	if result.Err() != nil {
		return 0, fmt.Errorf("failed to get token TTL: %w", result.Err())
	}
	
	return result.Val(), nil
}

// ExecuteAtomicUpdate executes a Lua script atomically
func (m *Manager) ExecuteAtomicUpdate(ctx context.Context, script string, keys []string, args []interface{}) error {
	if m.client == nil {
		return fmt.Errorf("Redis client not connected")
	}
	
	// Create a new script and execute it
	luaScript := redis.NewScript(script)
	result := luaScript.Run(ctx, m.client, keys, args...)
	
	if err := result.Err(); err != nil {
		return fmt.Errorf("failed to execute atomic update: %w", err)
	}
	
	return nil
}

// ExecuteAtomicUpdateWithResult executes a Lua script atomically and returns the result
func (m *Manager) ExecuteAtomicUpdateWithResult(ctx context.Context, script string, keys []string, args []interface{}) (interface{}, error) {
	if m.client == nil {
		return nil, fmt.Errorf("Redis client not connected")
	}
	
	// Create a new script and execute it
	luaScript := redis.NewScript(script)
	result := luaScript.Run(ctx, m.client, keys, args...)
	
	if err := result.Err(); err != nil {
		return nil, fmt.Errorf("failed to execute atomic update: %w", err)
	}
	
	return result.Val(), nil
}

// GetPipeline returns a new Redis pipeline for batch operations
func (m *Manager) GetPipeline() redis.Pipeliner {
	if m.client == nil {
		return nil
	}
	return m.client.Pipeline()
}

// ExecutePipeline executes a pipeline of Redis commands atomically
func (m *Manager) ExecutePipeline(ctx context.Context, pipeline redis.Pipeliner) error {
	if pipeline == nil {
		return fmt.Errorf("pipeline is nil")
	}
	
	_, err := pipeline.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to execute pipeline: %w", err)
	}
	
	return nil
}

// AtomicUpdateTokenData atomically updates all token data using Lua script
func (m *Manager) AtomicUpdateTokenData(ctx context.Context, tokenAddress string, trade models.TradeData, indices map[string]int64, aggregates map[string]*models.AggregateData, ttl time.Duration) error {
	if m.client == nil {
		return fmt.Errorf("Redis client not connected")
	}
	
	// Serialize trade data
	tradeJSON, err := trade.ToJSON()
	if err != nil {
		return fmt.Errorf("failed to serialize trade data: %w", err)
	}
	
	// Serialize indices
	indicesJSON, err := models.SerializeIndices(indices)
	if err != nil {
		return fmt.Errorf("failed to serialize indices: %w", err)
	}
	
	// Serialize aggregates
	aggregatesJSON, err := models.SerializeAggregates(aggregates)
	if err != nil {
		return fmt.Errorf("failed to serialize aggregates: %w", err)
	}
	
	// Execute atomic update
	result := m.updateTokenDataScript.Run(ctx, m.client, 
		[]string{tokenAddress},
		string(tradeJSON),
		string(indicesJSON),
		string(aggregatesJSON),
		time.Now().Format(time.RFC3339),
		int(ttl.Seconds()),
	)
	
	if err := result.Err(); err != nil {
		return fmt.Errorf("failed to execute atomic token data update: %w", err)
	}
	
	return nil
}

// AtomicAddTrade atomically adds a trade to the token's trade list
func (m *Manager) AtomicAddTrade(ctx context.Context, tokenAddress string, trade models.TradeData, ttl time.Duration) (int64, error) {
	if m.client == nil {
		return 0, fmt.Errorf("Redis client not connected")
	}
	
	// Serialize trade data
	tradeJSON, err := trade.ToJSON()
	if err != nil {
		return 0, fmt.Errorf("failed to serialize trade data: %w", err)
	}
	
	// Execute atomic add trade
	result := m.addTradeScript.Run(ctx, m.client,
		[]string{tokenAddress},
		string(tradeJSON),
		int(ttl.Seconds()),
	)
	
	if err := result.Err(); err != nil {
		return 0, fmt.Errorf("failed to execute atomic add trade: %w", err)
	}
	
	// Return the new length of the trades list
	length, ok := result.Val().(int64)
	if !ok {
		return 0, fmt.Errorf("unexpected result type from add trade script")
	}
	
	return length, nil
}

// AtomicUpdateIndicesAndAggregates atomically updates indices and aggregates
func (m *Manager) AtomicUpdateIndicesAndAggregates(ctx context.Context, tokenAddress string, indices map[string]int64, aggregates map[string]*models.AggregateData, ttl time.Duration) error {
	if m.client == nil {
		return fmt.Errorf("Redis client not connected")
	}
	
	return m.errorHandler.ExecuteWithRetry(ctx, "atomic_update_indices_aggregates", func() error {
		// Serialize indices
		indicesJSON, err := models.SerializeIndices(indices)
		if err != nil {
			return fmt.Errorf("failed to serialize indices: %w", err)
		}
		
		// Serialize aggregates
		aggregatesJSON, err := models.SerializeAggregates(aggregates)
		if err != nil {
			return fmt.Errorf("failed to serialize aggregates: %w", err)
		}
		
		// Execute atomic update
		result := m.updateIndicesScript.Run(ctx, m.client,
			[]string{tokenAddress},
			string(indicesJSON),
			string(aggregatesJSON),
			time.Now().Format(time.RFC3339),
			int(ttl.Seconds()),
		)
		
		if err := result.Err(); err != nil {
			// Store in fallback cache if Redis fails
			fallbackData := map[string]interface{}{
				"indices":    indices,
				"aggregates": aggregates,
				"timestamp":  time.Now(),
			}
			m.fallbackManager.Store(fmt.Sprintf("indices_aggregates:%s", tokenAddress), fallbackData)
			return fmt.Errorf("failed to execute atomic indices and aggregates update: %w", err)
		}
		
		return nil
	})
}

// GetCircuitBreakerState returns the current state of the circuit breaker
func (m *Manager) GetCircuitBreakerState() CircuitBreakerState {
	return m.circuitBreaker.GetState()
}

// GetFallbackCacheSize returns the number of items in the fallback cache
func (m *Manager) GetFallbackCacheSize() int {
	return m.fallbackManager.GetCacheSize()
}

// ClearFallbackCache clears the fallback cache
func (m *Manager) ClearFallbackCache() {
	m.fallbackManager.Clear()
}

// IsCircuitBreakerOpen returns true if the circuit breaker is open
func (m *Manager) IsCircuitBreakerOpen() bool {
	return m.circuitBreaker.GetState() == CircuitBreakerOpen
}

// EnableFallback enables or disables fallback mode
func (m *Manager) EnableFallback(enabled bool) {
	m.fallbackManager.SetEnabled(enabled)
}