package packet

type TokenInfo struct {
	Token       string
	Symbol      string
	Name        string
	MetaInfo    string
	TotalSupply string
	IsAMM       bool
	CreateTime  string
}

type TokenTradeHistory struct {
	Token        string
	Wallet       string
	SellBuy      string
	NativeAmount string
	TokenAmount  string
	PriceUsd     string
	TransTime    string
	TxHash       string
}

type TokenAggregateItem struct {
	TimeWindow   string  `json:"time_window"`   // e.g., "1min", "5min", "15min"
	SellCount    int     `json:"sell_count"`
	BuyCount     int     `json:"buy_count"`
	TotalTrades  int     `json:"total_trades"`
	SellVolume   string  `json:"sell_volume"`   // Total sell volume in native currency
	BuyVolume    string  `json:"buy_volume"`    // Total buy volume in native currency
	TotalVolume  string  `json:"total_volume"`  // Total volume in native currency
	VolumeUsd    string  `json:"volume_usd"`    // Total volume in USD
	PriceChange  float64 `json:"price_change"`  // Price change percentage
	OpenPrice    string  `json:"open_price"`    // Opening price
	ClosePrice   string  `json:"close_price"`   // Closing price
	Timestamp    string  `json:"timestamp"`     // Window end timestamp
}

type TokenAggregateData struct {
	Token         string                `json:"token"`
	Symbol        string                `json:"symbol,omitempty"`
	Name          string                `json:"name,omitempty"`
	AggregateData []TokenAggregateItem  `json:"aggregate_data"`
	GeneratedAt   string                `json:"generated_at"`   // When this aggregate was generated
	Version       string                `json:"version"`        // Data structure version
}
