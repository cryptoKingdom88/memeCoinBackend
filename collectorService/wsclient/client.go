package wsclient

import (
	"log"
	"net/url"

	"github.com/cryptoKingdom88/memeCoinBackend/collectorService/config"

	"github.com/gorilla/websocket"
)

type Client struct {
	conn *websocket.Conn
	cfg  config.Config
}

func NewClient(cfg config.Config) *Client {
	return &Client{cfg: cfg}
}

func (c *Client) Connect() {
	u := url.URL{Scheme: "wss", Host: "stream.bitquery.io", Path: "/eap"}
	log.Printf("Connecting to BitQuery WebSocket: %s", u.String())

	header := make(map[string][]string)
	header["X-API-KEY"] = []string{c.cfg.BitqueryAPIKey}

	conn, _, err := websocket.DefaultDialer.Dial(u.String(), header)
	if err != nil {
		log.Fatalf("WebSocket Connect Failed: %v", err)
	}

	c.conn = conn
	log.Println("WebSocket Connect Success")
}

func (c *Client) Listen() {
	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			log.Printf("Message Listen Error: %v", err)
			continue
		}
		go HandleMessage(message)
	}
}
