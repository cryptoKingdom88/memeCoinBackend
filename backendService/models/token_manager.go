package models

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/cryptoKingdom88/memeCoinBackend/shared/packet"
	"github.com/go-redis/redis/v8"
	"golang.org/x/net/context"
)

// TokenInfo represents enhanced token information for caching
type TokenInfo struct {
	Token       string    `json:"token"`
	Symbol      string    `json:"symbol"`
	Name        string    `json:"name"`
	MetaInfo    string    `json:"meta_info"`
	TotalSupply string    `json:"total_supply"`
	IsAMM       bool      `json:"is_amm"`
	CreateTime  time.Time `json:"create_time"`
	
	// Trending calculation fields
	CurrentPrice  float64 `json:"current_price,omitempty"`
	LaunchPrice   float64 `json:"launch_price,omitempty"`
	PriceChange   float64 `json:"price_change,omitempty"`
	Volume10m     float64 `json:"volume_10m,omitempty"`
	TradeCount10m int     `json:"trade_count_10m,omitempty"`
	LastUpdate    time.Time `json:"last_update,omitempty"`
}

// TokenManager manages token caching and trending calculations
type TokenManager struct {
	// Configuration
	newTokensCacheSize      int
	trendingTokensCacheSize int
	tokenCacheTTL           time.Duration
	
	// Redis client
	redisClient *redis.Client
	
	// Database connection (fallback)
	db *sql.DB
	
	// In-memory caches for fast access
	newTokens     []TokenInfo
	trendingTokens []TokenInfo
	
	// Mutexes for thread safety
	newTokensMutex     sync.RWMutex
	trendingTokensMutex sync.RWMutex
}

// NewTokenManager creates a new token manager
func NewTokenManager(redisClient *redis.Client, db *sql.DB, newCacheSize, trendingCacheSize int, cacheTTL time.Duration) *TokenManager {
	return &TokenManager{
		newTokensCacheSize:      newCacheSize,
		trendingTokensCacheSize: trendingCacheSize,
		tokenCacheTTL:           cacheTTL,
		redisClient:             redisClient,
		db:                      db,
		newTokens:               make([]TokenInfo, 0, newCacheSize),
		trendingTokens:          make([]TokenInfo, 0, trendingCacheSize),
	}
}

// AddNewToken adds a new token to the cache
func (tm *TokenManager) AddNewToken(tokenInfo packet.TokenInfo) {
	tm.newTokensMutex.Lock()
	defer tm.newTokensMutex.Unlock()
	
	// Convert packet.TokenInfo to models.TokenInfo
	newToken := TokenInfo{
		Token:       tokenInfo.Token,
		Symbol:      tokenInfo.Symbol,
		Name:        tokenInfo.Name,
		MetaInfo:    tokenInfo.MetaInfo,
		TotalSupply: tokenInfo.TotalSupply,
		IsAMM:       tokenInfo.IsAMM,
		CreateTime:  parseTime(tokenInfo.CreateTime),
	}
	
	// Add to the beginning of the slice
	tm.newTokens = append([]TokenInfo{newToken}, tm.newTokens...)
	
	// Keep only the configured number of tokens
	if len(tm.newTokens) > tm.newTokensCacheSize {
		tm.newTokens = tm.newTokens[:tm.newTokensCacheSize]
	}
}

// CacheTokenInfo stores token info in Redis with TTL
func (tm *TokenManager) CacheTokenInfo(tokenInfo packet.TokenInfo, ttl time.Duration) error {
	ctx := context.Background()
	key := fmt.Sprintf("token:%s", tokenInfo.Token)
	
	data, err := json.Marshal(tokenInfo)
	if err != nil {
		return fmt.Errorf("failed to marshal token info: %w", err)
	}
	
	return tm.redisClient.Set(ctx, key, data, ttl).Err()
}

// GetTokenInfo retrieves token info from Redis or DB
func (tm *TokenManager) GetTokenInfo(tokenAddr string) *TokenInfo {
	// Try Redis first
	if tokenInfo := tm.getFromRedis(tokenAddr); tokenInfo != nil {
		return tokenInfo
	}
	
	// Fallback to database
	if tokenInfo := tm.getFromDB(tokenAddr); tokenInfo != nil {
		// Cache in Redis for future use
		packetInfo := packet.TokenInfo{
			Token:       tokenInfo.Token,
			Symbol:      tokenInfo.Symbol,
			Name:        tokenInfo.Name,
			MetaInfo:    tokenInfo.MetaInfo,
			TotalSupply: tokenInfo.TotalSupply,
			IsAMM:       tokenInfo.IsAMM,
			CreateTime:  tokenInfo.CreateTime.Format(time.RFC3339),
		}
		tm.CacheTokenInfo(packetInfo, tm.tokenCacheTTL)
		return tokenInfo
	}
	
	return nil
}

// getFromRedis retrieves token info from Redis
func (tm *TokenManager) getFromRedis(tokenAddr string) *TokenInfo {
	ctx := context.Background()
	key := fmt.Sprintf("token:%s", tokenAddr)
	
	data, err := tm.redisClient.Get(ctx, key).Result()
	if err != nil {
		return nil
	}
	
	var packetInfo packet.TokenInfo
	if err := json.Unmarshal([]byte(data), &packetInfo); err != nil {
		return nil
	}
	
	return &TokenInfo{
		Token:       packetInfo.Token,
		Symbol:      packetInfo.Symbol,
		Name:        packetInfo.Name,
		MetaInfo:    packetInfo.MetaInfo,
		TotalSupply: packetInfo.TotalSupply,
		IsAMM:       packetInfo.IsAMM,
		CreateTime:  parseTime(packetInfo.CreateTime),
	}
}

// getFromDB retrieves token info from database
func (tm *TokenManager) getFromDB(tokenAddr string) *TokenInfo {
	if tm.db == nil {
		return nil
	}
	
	query := `
		SELECT token, symbol, name, meta_info, total_supply, is_amm, create_time 
		FROM tokens 
		WHERE token = $1
	`
	
	var tokenInfo TokenInfo
	var createTimeStr string
	
	err := tm.db.QueryRow(query, tokenAddr).Scan(
		&tokenInfo.Token,
		&tokenInfo.Symbol,
		&tokenInfo.Name,
		&tokenInfo.MetaInfo,
		&tokenInfo.TotalSupply,
		&tokenInfo.IsAMM,
		&createTimeStr,
	)
	
	if err != nil {
		return nil
	}
	
	tokenInfo.CreateTime = parseTime(createTimeStr)
	return &tokenInfo
}

// IsTrendingCandidate checks if a token can be a trending candidate
func (tm *TokenManager) IsTrendingCandidate(tokenAddr, priceStr string) bool {
	price, err := strconv.ParseFloat(priceStr, 64)
	if err != nil {
		return false
	}
	
	tm.trendingTokensMutex.RLock()
	defer tm.trendingTokensMutex.RUnlock()
	
	// If trending list is not full, any token is a candidate
	if len(tm.trendingTokens) < tm.trendingTokensCacheSize {
		return true
	}
	
	// Check if price is higher than the lowest trending token
	lowestPrice := tm.trendingTokens[len(tm.trendingTokens)-1].CurrentPrice
	return price > lowestPrice
}

// UpdateTrendingToken updates or adds a token to trending list
func (tm *TokenManager) UpdateTrendingToken(tokenInfo TokenInfo, tradeInfo packet.TokenTradeHistory) bool {
	tm.trendingTokensMutex.Lock()
	defer tm.trendingTokensMutex.Unlock()
	
	price, _ := strconv.ParseFloat(tradeInfo.PriceUsd, 64)
	tokenInfo.CurrentPrice = price
	tokenInfo.LastUpdate = time.Now()
	
	// Check if token already exists in trending list
	for i, existing := range tm.trendingTokens {
		if existing.Token == tokenInfo.Token {
			// Update existing token
			tm.trendingTokens[i] = tokenInfo
			tm.sortTrendingTokens()
			return false // Not newly added
		}
	}
	
	// Add new token
	tm.trendingTokens = append(tm.trendingTokens, tokenInfo)
	tm.sortTrendingTokens()
	
	// Keep only configured number of tokens
	if len(tm.trendingTokens) > tm.trendingTokensCacheSize {
		tm.trendingTokens = tm.trendingTokens[:tm.trendingTokensCacheSize]
	}
	
	return true // Newly added
}

// sortTrendingTokens sorts trending tokens by current price (descending)
func (tm *TokenManager) sortTrendingTokens() {
	sort.Slice(tm.trendingTokens, func(i, j int) bool {
		return tm.trendingTokens[i].CurrentPrice > tm.trendingTokens[j].CurrentPrice
	})
}

// IsRelevantToken checks if a token is in new or trending cache
func (tm *TokenManager) IsRelevantToken(tokenAddr string) bool {
	// Check new tokens
	tm.newTokensMutex.RLock()
	for _, token := range tm.newTokens {
		if token.Token == tokenAddr {
			tm.newTokensMutex.RUnlock()
			return true
		}
	}
	tm.newTokensMutex.RUnlock()
	
	// Check trending tokens
	tm.trendingTokensMutex.RLock()
	for _, token := range tm.trendingTokens {
		if token.Token == tokenAddr {
			tm.trendingTokensMutex.RUnlock()
			return true
		}
	}
	tm.trendingTokensMutex.RUnlock()
	
	return false
}

// GetNewTokens returns a copy of new tokens list
func (tm *TokenManager) GetNewTokens() []TokenInfo {
	tm.newTokensMutex.RLock()
	defer tm.newTokensMutex.RUnlock()
	
	result := make([]TokenInfo, len(tm.newTokens))
	copy(result, tm.newTokens)
	return result
}

// GetTrendingTokens returns a copy of trending tokens list
func (tm *TokenManager) GetTrendingTokens() []TokenInfo {
	tm.trendingTokensMutex.RLock()
	defer tm.trendingTokensMutex.RUnlock()
	
	result := make([]TokenInfo, len(tm.trendingTokens))
	copy(result, tm.trendingTokens)
	return result
}

// parseTime safely parses time string
func parseTime(timeStr string) time.Time {
	if t, err := time.Parse(time.RFC3339, timeStr); err == nil {
		return t
	}
	return time.Now()
}