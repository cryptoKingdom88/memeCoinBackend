package client

import (
	"bufio"
	"encoding/json"
	"log"
	"os"
	"sync"

	"github.com/gorilla/websocket"
)

type Client struct {
	conn *websocket.Conn
	mu   sync.Mutex
}

func Connect(apiKey string) (*Client, error) {
	header := map[string][]string{
		"Sec-WebSocket-Protocol": {"graphql-transport-ws"},
	}
	dialer := websocket.DefaultDialer
	conn, _, err := dialer.Dial("wss://streaming.bitquery.io/eap?token="+apiKey, header)
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn}, nil
}

func LoadQuery(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	query := ""
	for scanner.Scan() {
		query += scanner.Text() + "\n"
	}
	return query, scanner.Err()
}

func (c *Client) Subscribe(query string, handler func([]byte)) (string, error) {
	id := generateID()

	// 1. connection_init
	initMsg := map[string]interface{}{
		"type": "connection_ack",
	}
	c.mu.Lock()
	err := c.conn.WriteJSON(initMsg)
	c.mu.Unlock()
	if err != nil {
		return "", err
	}

	go func() {
		for {
			_, msg, err := c.conn.ReadMessage()
			if err != nil {
				log.Printf("WebSocket read error: %v", err)
				return
			}
			var resp map[string]interface{}
			if err := json.Unmarshal(msg, &resp); err != nil {
				log.Printf("JSON unmarshal error: %v", err)
				continue
			}

			log.Printf("WebSocket Message : %s", msg)
			switch resp["type"] {
			case "connection_ack":
				// 2. start subscription
				startMsg := map[string]interface{}{
					"type":    "start",
					"id":      id,
					"payload": map[string]interface{}{"query": query},
				}
				c.mu.Lock()
				err := c.conn.WriteJSON(startMsg)
				c.mu.Unlock()
				if err != nil {
					log.Printf("WebSocket write error: %v", err)
					return
				}
			case "data":
				if resp["id"] == id {
					payload, _ := json.Marshal(resp["payload"])
					handler(payload)
				}
			case "error", "connection_error":
				log.Printf("WebSocket error: %v", resp["payload"])
			case "complete", "close":
				log.Printf("WebSocket closed")
				return
			}
		}
	}()
	return id, nil
}

func (c *Client) Unsubscribe(id string) error {
	unsubMsg := map[string]interface{}{
		"id":   id,
		"type": "stop",
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn.WriteJSON(unsubMsg)
}

func PrintHandler(msg []byte) {
	log.Printf("Received: %s", string(msg))
}

// 간단한 ID 생성기
var idSeq = 0

func generateID() string {
	idSeq++
	return "sub" + string(idSeq)
}
