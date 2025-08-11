package handlers

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"backendService/models"
	"backendService/websocket"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
)

func TestHTTPEndpoints(t *testing.T) {
	// Set Gin to test mode
	gin.SetMode(gin.TestMode)

	// Create test hub and hub manager
	hub := models.NewHub()
	hubManager := websocket.NewHubManager(hub)
	wsHandler := NewWebSocketHandler(hub)

	// Create test router with all endpoints
	router := gin.New()
	
	// Add all endpoints
	router.GET("/ws", wsHandler.WebSocketUpgrade)
	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status":   "healthy",
			"clients":  hubManager.GetClientCount(),
			"channels": hubManager.GetActiveChannels(),
		})
	})
	router.GET("/", func(c *gin.Context) {
		c.Redirect(http.StatusMovedPermanently, "/docs/")
	})

	tests := []struct {
		name           string
		method         string
		path           string
		expectedStatus int
		checkBody      bool
		expectedBody   string
	}{
		{
			name:           "Health Check Endpoint",
			method:         "GET",
			path:           "/health",
			expectedStatus: http.StatusOK,
			checkBody:      true,
			expectedBody:   "healthy",
		},
		{
			name:           "Root Redirect",
			method:         "GET",
			path:           "/",
			expectedStatus: http.StatusMovedPermanently,
			checkBody:      false,
		},
		{
			name:           "WebSocket Upgrade - Missing Headers",
			method:         "GET",
			path:           "/ws",
			expectedStatus: http.StatusBadRequest,
			checkBody:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, _ := http.NewRequest(tt.method, tt.path, nil)
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)

			assert.Equal(t, tt.expectedStatus, w.Code)

			if tt.checkBody {
				assert.Contains(t, w.Body.String(), tt.expectedBody)
			}
		})
	}
}

// Note: WebSocket upgrade testing requires a real HTTP server
// since httptest.ResponseRecorder doesn't support connection hijacking.
// The WebSocket functionality is tested in the websocket package tests.