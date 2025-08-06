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
	Type string
	SellCount int
	BuyCount int
	TradeAmount int64
	PriceChange float64
}

type TokenSniperITem struct {

}

type TokenAggregateData struct {
	Token        string
	AggregateData []TokenAggregateItem
}
