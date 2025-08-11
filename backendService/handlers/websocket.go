package handlers

import (
	"log"
	"net/http"

	"backendService/models"
	"backendService/websocket"
	"github.com/gin-gonic/gin"
)

// WebSocketHandler handles WebSocket-related HTTP endpoints
type WebSocketHandler struct {
	wsHandler *websocket.Handler
}

// NewWebSocketHandler creates a new WebSocket handler
func NewWebSocketHandler(hub *models.Hub) *WebSocketHandler {
	return &WebSocketHandler{
		wsHandler: websocket.NewHandler(hub),
	}
}

// WebSocketUpgrade godoc
// @Summary WebSocket connection endpoint
// @Description Upgrade HTTP connection to WebSocket for real-time data streaming
// @Tags websocket
// @Accept json
// @Produce json
// @Param Upgrade header string true "Upgrade header" default(websocket)
// @Param Connection header string true "Connection header" default(Upgrade)
// @Param Sec-WebSocket-Key header string true "WebSocket key"
// @Param Sec-WebSocket-Version header string true "WebSocket version" default(13)
// @Success 101 {string} string "Switching Protocols"
// @Failure 400 {object} map[string]string "Bad Request"
// @Router /ws [get]
func (h *WebSocketHandler) WebSocketUpgrade(c *gin.Context) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Panic in WebSocket upgrade handler: %v", r)
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "Internal server error during WebSocket upgrade",
			})
		}
	}()

	// Validate required headers for WebSocket upgrade
	if c.GetHeader("Upgrade") != "websocket" {
		log.Printf("Invalid Upgrade header: %s", c.GetHeader("Upgrade"))
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Invalid Upgrade header, expected 'websocket'",
		})
		return
	}

	if c.GetHeader("Connection") != "Upgrade" {
		log.Printf("Invalid Connection header: %s", c.GetHeader("Connection"))
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Invalid Connection header, expected 'Upgrade'",
		})
		return
	}

	if c.GetHeader("Sec-WebSocket-Key") == "" {
		log.Printf("Missing Sec-WebSocket-Key header")
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Missing Sec-WebSocket-Key header",
		})
		return
	}

	// Attempt WebSocket upgrade with error handling
	h.wsHandler.HandleWebSocket(c)
}