package models

import "github.com/cryptoKingdom88/memeCoinBackend/shared/packet"

// DBTokenInfo represents token information data structure for database storage
// Excludes IsAMM field as requested and uses snake_case field names
type DBTokenInfo struct {
	ID          int64  `db:"id"`           // Auto-increment primary key
	Token       string `db:"token"`        // Token address
	Symbol      string `db:"symbol"`       // Token symbol
	Name        string `db:"name"`         // Token name
	MetaInfo    string `db:"meta_info"`    // Token metadata information
	TotalSupply string `db:"total_supply"` // Total token supply
	CreateTime  string `db:"create_time"`  // Token creation timestamp
}

// DBTokenTradeHistory represents token trade history data structure for database storage
// Uses snake_case field names for database compatibility
type DBTokenTradeHistory struct {
	ID           int64  `db:"id"`            // Auto-increment primary key
	Token        string `db:"token"`         // Token address
	Wallet       string `db:"wallet"`        // Wallet address
	SellBuy      string `db:"sell_buy"`      // Trade type (sell/buy)
	NativeAmount string `db:"native_amount"` // Native currency amount
	TokenAmount  string `db:"token_amount"`  // Token amount
	PriceUsd     string `db:"price_usd"`     // USD price
	TransTime    string `db:"trans_time"`    // Transaction timestamp
	TxHash       string `db:"tx_hash"`       // Transaction hash
}

// ConvertToDBTokenInfo converts packet.TokenInfo to DBTokenInfo for database storage
// Excludes IsAMM field as requested
func ConvertToDBTokenInfo(tokenInfo packet.TokenInfo) DBTokenInfo {
	return DBTokenInfo{
		Token:       tokenInfo.Token,
		Symbol:      tokenInfo.Symbol,
		Name:        tokenInfo.Name,
		MetaInfo:    tokenInfo.MetaInfo,
		TotalSupply: tokenInfo.TotalSupply,
		CreateTime:  tokenInfo.CreateTime,
		// Note: IsAMM field is intentionally excluded
	}
}

// ConvertToDBTokenTradeHistory converts packet.TokenTradeHistory to DBTokenTradeHistory for database storage
// Includes all fields from the original struct
func ConvertToDBTokenTradeHistory(tradeHistory packet.TokenTradeHistory) DBTokenTradeHistory {
	return DBTokenTradeHistory{
		Token:        tradeHistory.Token,
		Wallet:       tradeHistory.Wallet,
		SellBuy:      tradeHistory.SellBuy,
		NativeAmount: tradeHistory.NativeAmount,
		TokenAmount:  tradeHistory.TokenAmount,
		PriceUsd:     tradeHistory.PriceUsd,
		TransTime:    tradeHistory.TransTime,
		TxHash:       tradeHistory.TxHash,
	}
}