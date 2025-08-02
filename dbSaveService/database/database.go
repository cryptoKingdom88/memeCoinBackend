package database

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/cryptoKingdom88/memeCoinBackend/dbSaveService/config"
	"github.com/cryptoKingdom88/memeCoinBackend/dbSaveService/models"
	_ "github.com/lib/pq"
)

// DB represents database connection and operations
type DB struct {
	conn *sql.DB
}

// NewDB creates a new database connection
func NewDB(cfg config.Config) (*DB, error) {
	// Build PostgreSQL connection string
	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		cfg.DBHost, cfg.DBPort, cfg.DBUser, cfg.DBPassword, cfg.DBName)
	
	conn, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}
	
	// Test the connection
	if err := conn.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}
	
	db := &DB{conn: conn}
	
	// Create tables if they don't exist
	if err := db.createTables(); err != nil {
		return nil, fmt.Errorf("failed to create tables: %w", err)
	}
	
	log.Println("✅ Database connection established and tables created")
	return db, nil
}

// createTables creates the required tables with snake_case field names
func (db *DB) createTables() error {
	// Create token_info table
	tokenInfoSQL := `
	CREATE TABLE IF NOT EXISTS token_info (
		id SERIAL PRIMARY KEY,
		token VARCHAR(255) NOT NULL,
		symbol VARCHAR(100),
		name VARCHAR(255),
		meta_info TEXT,
		total_supply VARCHAR(255),
		create_time VARCHAR(255),
		UNIQUE(token)
	)`
	
	if _, err := db.conn.Exec(tokenInfoSQL); err != nil {
		return fmt.Errorf("failed to create token_info table: %w", err)
	}
	
	// Create token_trade_history table
	tradeHistorySQL := `
	CREATE TABLE IF NOT EXISTS token_trade_history (
		id SERIAL PRIMARY KEY,
		token VARCHAR(255) NOT NULL,
		wallet VARCHAR(255) NOT NULL,
		sell_buy VARCHAR(10),
		native_amount VARCHAR(255),
		token_amount VARCHAR(255),
		price_usd VARCHAR(255),
		trans_time VARCHAR(255),
		tx_hash VARCHAR(255) UNIQUE
	)`
	
	if _, err := db.conn.Exec(tradeHistorySQL); err != nil {
		return fmt.Errorf("failed to create token_trade_history table: %w", err)
	}
	
	return nil
}

// BatchInsertTokenInfo performs true batch insert of token information using single SQL statement
func (db *DB) BatchInsertTokenInfo(tokens []models.DBTokenInfo) error {
	if len(tokens) == 0 {
		return nil
	}
	
	// PostgreSQL parameter limit is 65535, with 6 fields per record, max ~10922 records per batch
	const maxBatchSize = 1000
	
	// Process in chunks if necessary
	for i := 0; i < len(tokens); i += maxBatchSize {
		end := i + maxBatchSize
		if end > len(tokens) {
			end = len(tokens)
		}
		
		batch := tokens[i:end]
		if err := db.batchInsertTokenInfoChunk(batch); err != nil {
			return fmt.Errorf("failed to insert token info batch chunk: %w", err)
		}
	}
	
	log.Printf("✅ Successfully batch inserted %d token info records", len(tokens))
	return nil
}

// batchInsertTokenInfoChunk inserts a chunk of token info records in single SQL statement
func (db *DB) batchInsertTokenInfoChunk(tokens []models.DBTokenInfo) error {
	if len(tokens) == 0 {
		return nil
	}
	
	// Build VALUES clause with placeholders
	valueStrings := make([]string, 0, len(tokens))
	valueArgs := make([]interface{}, 0, len(tokens)*6)
	
	for i, token := range tokens {
		// Each record has 6 fields
		valueStrings = append(valueStrings, fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d)",
			i*6+1, i*6+2, i*6+3, i*6+4, i*6+5, i*6+6))
		
		valueArgs = append(valueArgs, token.Token, token.Symbol, token.Name,
			token.MetaInfo, token.TotalSupply, token.CreateTime)
	}
	
	// Build complete SQL with UPSERT
	query := fmt.Sprintf(`
		INSERT INTO token_info (token, symbol, name, meta_info, total_supply, create_time)
		VALUES %s
		ON CONFLICT (token) DO UPDATE SET
			symbol = EXCLUDED.symbol,
			name = EXCLUDED.name,
			meta_info = EXCLUDED.meta_info,
			total_supply = EXCLUDED.total_supply,
			create_time = EXCLUDED.create_time`,
		strings.Join(valueStrings, ","))
	
	// Execute single batch insert
	_, err := db.conn.Exec(query, valueArgs...)
	if err != nil {
		return fmt.Errorf("failed to execute batch insert: %w", err)
	}
	
	return nil
}

// BatchInsertTokenTradeHistory performs true batch insert of token trade history using single SQL statement
func (db *DB) BatchInsertTokenTradeHistory(trades []models.DBTokenTradeHistory) error {
	if len(trades) == 0 {
		return nil
	}
	
	// PostgreSQL parameter limit is 65535, with 8 fields per record, max ~8191 records per batch
	const maxBatchSize = 1000
	
	// Process in chunks if necessary
	for i := 0; i < len(trades); i += maxBatchSize {
		end := i + maxBatchSize
		if end > len(trades) {
			end = len(trades)
		}
		
		batch := trades[i:end]
		if err := db.batchInsertTokenTradeHistoryChunk(batch); err != nil {
			return fmt.Errorf("failed to insert trade history batch chunk: %w", err)
		}
	}
	
	log.Printf("✅ Successfully batch inserted %d trade history records", len(trades))
	return nil
}

// batchInsertTokenTradeHistoryChunk inserts a chunk of trade history records in single SQL statement
func (db *DB) batchInsertTokenTradeHistoryChunk(trades []models.DBTokenTradeHistory) error {
	if len(trades) == 0 {
		return nil
	}
	
	// Build VALUES clause with placeholders
	valueStrings := make([]string, 0, len(trades))
	valueArgs := make([]interface{}, 0, len(trades)*8)
	
	for i, trade := range trades {
		// Each record has 8 fields
		valueStrings = append(valueStrings, fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			i*8+1, i*8+2, i*8+3, i*8+4, i*8+5, i*8+6, i*8+7, i*8+8))
		
		valueArgs = append(valueArgs, trade.Token, trade.Wallet, trade.SellBuy,
			trade.NativeAmount, trade.TokenAmount, trade.PriceUsd, trade.TransTime, trade.TxHash)
	}
	
	// Build complete SQL with conflict handling
	query := fmt.Sprintf(`
		INSERT INTO token_trade_history (token, wallet, sell_buy, native_amount, token_amount, price_usd, trans_time, tx_hash)
		VALUES %s
		ON CONFLICT (tx_hash) DO NOTHING`,
		strings.Join(valueStrings, ","))
	
	// Execute single batch insert
	_, err := db.conn.Exec(query, valueArgs...)
	if err != nil {
		return fmt.Errorf("failed to execute batch insert: %w", err)
	}
	
	return nil
}

// Close closes the database connection
func (db *DB) Close() error {
	if db.conn != nil {
		return db.conn.Close()
	}
	return nil
}