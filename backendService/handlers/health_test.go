package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"backendService/models"
	"backendService/websocket"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
)

func TestHealthCheck(t *testing.T) {
	// Set Gin to test mode
	gin.SetMode(gin.TestMode)

	// Create test hub and hub manager
	hub := models.NewHub()
	hubManager := websocket.NewHubManager(hub)

	// Create health check handler
	healthHandler := func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status":   "healthy",
			"clients":  hubManager.GetClientCount(),
			"channels": hubManager.GetActiveChannels(),
		})
	}

	// Create test router
	router := gin.New()
	router.GET("/health", healthHandler)

	// Create test request
	req, _ := http.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()

	// Perform request
	router.ServeHTTP(w, req)

	// Assert response
	assert.Equal(t, http.StatusOK, w.Code)

	// Parse response body
	var response map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(t, err)

	// Verify response structure
	assert.Equal(t, "healthy", response["status"])
	assert.Contains(t, response, "clients")
	assert.Contains(t, response, "channels")
}