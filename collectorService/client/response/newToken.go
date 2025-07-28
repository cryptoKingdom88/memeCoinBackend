package client

type TokenSupplyUpdateRawResponse struct {
	Solana TokenSupplyUpdateSolana `json:"Solana"`
}

type TokenSupplyUpdateSolana struct {
	TokenSupplyUpdates []TokenSupplyUpdateDataItem `json:"TokenSupplyUpdates"`
}

type TokenSupplyUpdateDataItem struct {
	Block             TokenSupplyUpdateBlockData       `json:"Block"`
	TokenSupplyUpdate TokenSupplyUpdateData            `json:"TokenSupplyUpdate"`
	Transaction       TokenSupplyUpdateTransactionData `json:"Transaction"`
}

type TokenSupplyUpdateBlockData struct {
	Time string `json:"Time"`
}

type TokenSupplyUpdateData struct {
	Amount      string                        `json:"Amount"`
	Currency    TokenSupplyUpdateCurrencyData `json:"Currency"`
	PostBalance string                        `json:"PostBalance"`
}

type TokenSupplyUpdateCurrencyData struct {
	Decimals            int                      `json:"Decimals"`
	EditionNonce        int                      `json:"EditionNonce"`
	Fungible            bool                     `json:"Fungible"`
	IsMutable           bool                     `json:"IsMutable"`
	Key                 string                   `json:"Key"`
	MetadataAddress     string                   `json:"MetadataAddress"`
	MintAddress         string                   `json:"MintAddress"`
	Name                string                   `json:"Name"`
	Native              bool                     `json:"Native"`
	PrimarySaleHappened bool                     `json:"PrimarySaleHappened"`
	ProgramAddress      string                   `json:"ProgramAddress"`
	Symbol              string                   `json:"Symbol"`
	TokenCreator        TokenSupplyUpdateCreator `json:"TokenCreator"`
	TokenStandard       string                   `json:"TokenStandard"`
	UpdateAuthority     string                   `json:"UpdateAuthority"`
	Uri                 string                   `json:"Uri"`
	VerifiedCollection  bool                     `json:"VerifiedCollection"`
	Wrapped             bool                     `json:"Wrapped"`
}

type TokenSupplyUpdateCreator struct {
	Address []string `json:"Address"`
}

type TokenSupplyUpdateTransactionData struct {
	Signer string `json:"Signer"`
}
