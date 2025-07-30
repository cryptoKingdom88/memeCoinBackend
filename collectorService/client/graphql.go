package client

import (
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"sync"

	"github.com/gorilla/websocket"
)

type Client struct {
	conn *websocket.Conn
	mu   sync.Mutex
}

type InitHeaders struct {
	Authorization string `json:"Authorization"`
}

type InitPayload struct {
	Headers InitHeaders `json:"headers"`
}

type InitMessage struct {
	Type    string      `json:"type"`
	Payload InitPayload `json:"payload"`
}

func getWebSocketDialer(proxyMode bool) *websocket.Dialer {
	if proxyMode {
		caCert, err := ioutil.ReadFile("/Volumes/Resource/Temp/http.crt")
		if err != nil {
			panic("CA Certificate Load Failed: " + err.Error())
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)

		tlsConfig := &tls.Config{
			RootCAs: caCertPool,
		}

		proxyURL, err := url.Parse("http://127.0.0.1:8000")
		if err != nil {
			panic("Proxy URL Parsing Failed: " + err.Error())
		}

		return &websocket.Dialer{
			Proxy:           http.ProxyURL(proxyURL),
			TLSClientConfig: tlsConfig,
		}
	}

	// if proxyMode is false, use default Dialer
	return websocket.DefaultDialer
}

func Connect(apiKey string) (*Client, error) {
	header := map[string][]string{
		"Sec-WebSocket-Protocol": {"graphql-transport-ws"},
		"Accept-Encoding":        {"gzip"},
	}

	dialer := getWebSocketDialer(false)

	u := url.URL{
		Scheme:   "wss",
		Host:     "streaming.bitquery.io",
		Path:     "/eap",
		RawQuery: "token=" + apiKey,
	}

	conn, resp, err := dialer.Dial(u.String(), header)
	if err != nil {
		if resp != nil && resp.Body != nil {
			body, _ := ioutil.ReadAll(resp.Body)
			log.Printf("WebSocket Dial error: %v", err)
			log.Printf("Response Status: %s", resp.Status)
			log.Printf("Response Headers: %+v", resp.Header)
			log.Printf("Response Body: %s", body)
		} else {
			log.Printf("WebSocket Dial failed: %v (no response received)", err)
		}

		panic(err)
	}

	//dialer := websocket.DefaultDialer
	// conn, _, err := dialer.Dial("wss://streaming.bitquery.io/eap?token="+apiKey, header)
	// if err != nil {
	// 	return nil, err
	// }

	// authorize
	msg := InitMessage{
		Type: "connection_init",
		Payload: InitPayload{
			Headers: InitHeaders{
				Authorization: fmt.Sprintf("Bearer %s", apiKey),
			},
		},
	}
	err = conn.WriteJSON(msg)
	if err != nil {
		log.Println("Write error:", err)
		conn.Close()
		return nil, err
	}

	// wait connection_ack
	_, _, err = conn.ReadMessage()
	if err != nil {
		log.Printf("WebSocket read error: %v", err)
		conn.Close()
		return nil, err
	}

	return &Client{conn: conn}, nil
}

func (c *Client) Close() {
	c.conn.Close()
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

type Payload struct {
	Query     string                 `json:"query"`
	Variables map[string]interface{} `json:"variables"`
}

type SubscriptionMessage struct {
	ID      string  `json:"id"`
	Type    string  `json:"type"`
	Payload Payload `json:"payload"`
}

func (c *Client) Subscribe(query string, handler func([]byte), done <-chan struct{}) (string, error) {
	id := generateID()

	msg := SubscriptionMessage{
		ID:   generateID(),
		Type: "subscribe",
		Payload: Payload{
			Query:     query,
			Variables: map[string]interface{}{},
		},
	}

	err := c.conn.WriteJSON(msg)
	if err != nil {
		return "", err
	}

	go func() {
		for {
			select {
			case <-done:
				log.Printf("Subscribe done: %s", id)

				c.Unsubscribe(id)
				return
			default:
				_, rawMsg, err := c.conn.ReadMessage()
				if err != nil {
					log.Printf("WebSocket read error: %v", err)
					return
				}
				var resp map[string]interface{}
				if err := json.Unmarshal(rawMsg, &resp); err != nil {
					log.Printf("JSON unmarshal error: %v", err)
					continue
				}
				// transfer data packet to handler
				if resp["type"] == "next" {
					payloadMap, ok := resp["payload"].(map[string]interface{})
					if !ok {
						log.Println("payload is not a map")
						return
					}

					dataBytes, err := json.Marshal(payloadMap["data"])
					if err != nil {
						log.Printf("Failed to marshal data: %v", err)
						return
					}
					handler(dataBytes)
				}
			}
		}
	}()
	return id, nil
}

func (c *Client) Unsubscribe(id string) error {
	unsubMsg := map[string]interface{}{
		"id":   id,
		"type": "complete",
	}
	return c.conn.WriteJSON(unsubMsg)
}

func PrintHandler(msg []byte) {
	log.Printf("Received: %s", string(msg))
}

var idSeq = 0

func generateID() string {
	idSeq++
	return "sub" + strconv.Itoa(idSeq)
}
