-- Database initialization script for memecoin backend
-- Creates tables with snake_case field names and auto-increment IDs

-- Create token_info table
-- Stores token information excluding IsAMM field as requested
CREATE TABLE IF NOT EXISTS token_info (
    id SERIAL PRIMARY KEY,
    token VARCHAR(255) NOT NULL UNIQUE,
    symbol VARCHAR(100),
    name VARCHAR(255),
    meta_info TEXT,
    total_supply VARCHAR(255),
    create_time VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create token_trade_history table
-- Stores all token trade history data
CREATE TABLE IF NOT EXISTS token_trade_history (
    id SERIAL PRIMARY KEY,
    token VARCHAR(255) NOT NULL,
    wallet VARCHAR(255) NOT NULL,
    sell_buy VARCHAR(10),
    native_amount VARCHAR(255),
    token_amount VARCHAR(255),
    price_usd VARCHAR(255),
    trans_time VARCHAR(255),
    tx_hash VARCHAR(255) UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_token_info_token ON token_info(token);
CREATE INDEX IF NOT EXISTS idx_token_info_symbol ON token_info(symbol);
CREATE INDEX IF NOT EXISTS idx_trade_history_token ON token_trade_history(token);
CREATE INDEX IF NOT EXISTS idx_trade_history_wallet ON token_trade_history(wallet);
CREATE INDEX IF NOT EXISTS idx_trade_history_tx_hash ON token_trade_history(tx_hash);
CREATE INDEX IF NOT EXISTS idx_trade_history_trans_time ON token_trade_history(trans_time);

-- Add comments to tables and columns
COMMENT ON TABLE token_info IS 'Stores token information data excluding IsAMM field';
COMMENT ON COLUMN token_info.id IS 'Auto-increment primary key';
COMMENT ON COLUMN token_info.token IS 'Token contract address';
COMMENT ON COLUMN token_info.symbol IS 'Token symbol (e.g., BTC, ETH)';
COMMENT ON COLUMN token_info.name IS 'Full token name';
COMMENT ON COLUMN token_info.meta_info IS 'Additional token metadata';
COMMENT ON COLUMN token_info.total_supply IS 'Total token supply';
COMMENT ON COLUMN token_info.create_time IS 'Token creation timestamp';

COMMENT ON TABLE token_trade_history IS 'Stores complete token trade history data';
COMMENT ON COLUMN token_trade_history.id IS 'Auto-increment primary key';
COMMENT ON COLUMN token_trade_history.token IS 'Token contract address';
COMMENT ON COLUMN token_trade_history.wallet IS 'Wallet address involved in trade';
COMMENT ON COLUMN token_trade_history.sell_buy IS 'Trade type: sell or buy';
COMMENT ON COLUMN token_trade_history.native_amount IS 'Amount in native currency';
COMMENT ON COLUMN token_trade_history.token_amount IS 'Amount of tokens traded';
COMMENT ON COLUMN token_trade_history.price_usd IS 'USD price at time of trade';
COMMENT ON COLUMN token_trade_history.trans_time IS 'Transaction timestamp';
COMMENT ON COLUMN token_trade_history.tx_hash IS 'Unique transaction hash';