package batch

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/cryptoKingdom88/memeCoinBackend/dbSaveService/database"
	"github.com/cryptoKingdom88/memeCoinBackend/dbSaveService/models"
	"github.com/cryptoKingdom88/memeCoinBackend/shared/packet"
)

// Processor handles batch processing of token data
type Processor struct {
	db       *database.DB
	interval time.Duration
	
	// Buffers for batch processing - using packet structs for input
	tokenInfoBuffer      []packet.TokenInfo
	tradeHistoryBuffer   []packet.TokenTradeHistory
	
	// Mutexes for thread-safe operations
	tokenInfoMutex       sync.Mutex
	tradeHistoryMutex    sync.Mutex
}

// NewProcessor creates a new batch processor
func NewProcessor(db *database.DB, intervalSeconds int) *Processor {
	return &Processor{
		db:                   db,
		interval:            time.Duration(intervalSeconds) * time.Second,
		tokenInfoBuffer:      make([]packet.TokenInfo, 0),
		tradeHistoryBuffer:   make([]packet.TokenTradeHistory, 0),
	}
}

// Start begins the batch processing routine
func (p *Processor) Start(ctx context.Context) {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	
	log.Printf("🚀 Batch processor started with interval: %v", p.interval)
	
	for {
		select {
		case <-ctx.Done():
			log.Println("🛑 Stopping batch processor")
			// Process remaining data before stopping
			p.processBatch()
			return
		case <-ticker.C:
			p.processBatch()
		}
	}
}

// AddTokenInfo adds token info to the buffer for batch processing
func (p *Processor) AddTokenInfo(tokenInfo packet.TokenInfo) {
	p.tokenInfoMutex.Lock()
	defer p.tokenInfoMutex.Unlock()
	
	p.tokenInfoBuffer = append(p.tokenInfoBuffer, tokenInfo)
	log.Printf("📝 Added token info to buffer. Buffer size: %d", len(p.tokenInfoBuffer))
}

// AddTokenTradeHistory adds trade history to the buffer for batch processing
func (p *Processor) AddTokenTradeHistory(tradeHistory packet.TokenTradeHistory) {
	p.tradeHistoryMutex.Lock()
	defer p.tradeHistoryMutex.Unlock()
	
	p.tradeHistoryBuffer = append(p.tradeHistoryBuffer, tradeHistory)
	log.Printf("📝 Added trade history to buffer. Buffer size: %d", len(p.tradeHistoryBuffer))
}

// processBatch processes all buffered data and inserts into database
// Converts packet structs to DB models before insertion
func (p *Processor) processBatch() {
	// Process token info buffer
	p.tokenInfoMutex.Lock()
	tokenInfoBatch := make([]packet.TokenInfo, len(p.tokenInfoBuffer))
	copy(tokenInfoBatch, p.tokenInfoBuffer)
	p.tokenInfoBuffer = p.tokenInfoBuffer[:0] // Clear buffer
	p.tokenInfoMutex.Unlock()
	
	// Process trade history buffer
	p.tradeHistoryMutex.Lock()
	tradeHistoryBatch := make([]packet.TokenTradeHistory, len(p.tradeHistoryBuffer))
	copy(tradeHistoryBatch, p.tradeHistoryBuffer)
	p.tradeHistoryBuffer = p.tradeHistoryBuffer[:0] // Clear buffer
	p.tradeHistoryMutex.Unlock()
	
	// Convert and insert token info batch
	if len(tokenInfoBatch) > 0 {
		// Convert packet.TokenInfo to models.DBTokenInfo
		dbTokenInfoBatch := make([]models.DBTokenInfo, len(tokenInfoBatch))
		for i, tokenInfo := range tokenInfoBatch {
			dbTokenInfoBatch[i] = models.ConvertToDBTokenInfo(tokenInfo)
		}
		
		if err := p.db.BatchInsertTokenInfo(dbTokenInfoBatch); err != nil {
			log.Printf("❌ Failed to batch insert token info: %v", err)
		}
	}
	
	// Convert and insert trade history batch
	if len(tradeHistoryBatch) > 0 {
		// Convert packet.TokenTradeHistory to models.DBTokenTradeHistory
		dbTradeHistoryBatch := make([]models.DBTokenTradeHistory, len(tradeHistoryBatch))
		for i, tradeHistory := range tradeHistoryBatch {
			dbTradeHistoryBatch[i] = models.ConvertToDBTokenTradeHistory(tradeHistory)
		}
		
		if err := p.db.BatchInsertTokenTradeHistory(dbTradeHistoryBatch); err != nil {
			log.Printf("❌ Failed to batch insert trade history: %v", err)
		}
	}
	
	if len(tokenInfoBatch) > 0 || len(tradeHistoryBatch) > 0 {
		log.Printf("🔄 Batch processing completed - TokenInfo: %d, TradeHistory: %d", 
			len(tokenInfoBatch), len(tradeHistoryBatch))
	}
}