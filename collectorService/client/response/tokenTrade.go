package client

type TokenTradeRawResponse struct {
	Solana TokenTradeSolana `json:"Solana"`
}

type TokenTradeSolana struct {
	DEXTrades []TokenTradeDataItem `json:"DEXTrades"`
}

type TokenTradeDataItem struct {
	Instruction TokenTradeInstructionData `json:"Instruction"`
	Trade       TokenTradeData            `json:"Trade"`
	Transaction TokenTradeTransactionData `json:"Transaction"`
	Block       TokenTradeBlockData       `json:"Block"`
}

type TokenTradeInstructionData struct {
	Program TokenTradeProgramData `json:"Program"`
}

type TokenTradeProgramData struct {
	Method string `json:"Method"`
}

type TokenTradeData struct {
	Dex  TokenTradeDexData  `json:"Dex"`
	Buy  TokenTradeBuyData  `json:"Buy"`
	Sell TokenTradeSellData `json:"Sell"`
}

type TokenTradeDexData struct {
	ProtocolFamily string `json:"ProtocolFamily"`
	ProtocolName   string `json:"ProtocolName"`
}

type TokenTradeBuyData struct {
	Amount      string             `json:"Amount"`
	AmountInUSD string             `json:"AmountInUSD"`
	Account     TokenTradeAccount  `json:"Account"`
	Currency    TokenTradeCurrency `json:"Currency"`
	PriceInUSD  float64            `json:"PriceInUSD"`
}

type TokenTradeSellData struct {
	Amount      string             `json:"Amount"`
	AmountInUSD string             `json:"AmountInUSD"`
	Account     TokenTradeAccount  `json:"Account"`
	Currency    TokenTradeCurrency `json:"Currency"`
	PriceInUSD  float64            `json:"PriceInUSD"`
}

type TokenTradeAccount struct {
	Address string `json:"Address"`
	Owner   string `json:"Owner"`
}

type TokenTradeCurrency struct {
	MintAddress string `json:"MintAddress"`
	Native      bool   `json:"Native"`
	Wrapped     bool   `json:"Wrapped"`
}

type TokenTradeTransactionData struct {
	Signature string `json:"Signature"`
}

type TokenTradeBlockData struct {
	Time string `json:"Time"`
}
