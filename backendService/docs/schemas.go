package docs

// WebSocketMessage represents the structure of messages sent over WebSocket
type WebSocketMessage struct {
	Type      string      `json:"type" example:"token_info" enums:"token_info,trade_data,aggregate_data"`
	Channel   string      `json:"channel" example:"dashboard"`
	Data      interface{} `json:"data"`
	Timestamp string      `json:"timestamp" example:"2024-01-01T12:00:00Z"`
}

// SubscriptionMessage represents client subscription requests
type SubscriptionMessage struct {
	Type    string `json:"type" example:"subscribe" enums:"subscribe,unsubscribe"`
	Channel string `json:"channel" example:"dashboard"`
}

// TokenInfoData represents token information data
type TokenInfoData struct {
	TokenAddress string  `json:"token_address" example:"0x1234567890abcdef"`
	Symbol       string  `json:"symbol" example:"ETH"`
	Name         string  `json:"name" example:"Ethereum"`
	Price        float64 `json:"price" example:"2500.50"`
	MarketCap    float64 `json:"market_cap" example:"300000000000"`
}

// TradeData represents trade information
type TradeData struct {
	TokenAddress string  `json:"token_address" example:"0x1234567890abcdef"`
	Price        float64 `json:"price" example:"2500.50"`
	Volume       float64 `json:"volume" example:"1000.0"`
	Timestamp    string  `json:"timestamp" example:"2024-01-01T12:00:00Z"`
	TxHash       string  `json:"tx_hash" example:"0xabcdef1234567890"`
}

// AggregateData represents aggregated trading metrics
type AggregateData struct {
	TokenAddress   string  `json:"token_address" example:"0x1234567890abcdef"`
	TotalVolume    float64 `json:"total_volume" example:"50000.0"`
	AveragePrice   float64 `json:"average_price" example:"2500.50"`
	TradeCount     int     `json:"trade_count" example:"100"`
	TimeWindow     string  `json:"time_window" example:"1h"`
	WindowStart    string  `json:"window_start" example:"2024-01-01T12:00:00Z"`
	WindowEnd      string  `json:"window_end" example:"2024-01-01T13:00:00Z"`
}

// ErrorResponse represents error messages
type ErrorResponse struct {
	Error   string `json:"error" example:"Invalid subscription channel"`
	Code    int    `json:"code" example:"400"`
	Message string `json:"message" example:"Only 'dashboard' channel is supported"`
}