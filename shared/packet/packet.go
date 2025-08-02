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
