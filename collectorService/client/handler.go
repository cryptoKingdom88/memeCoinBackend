package client

import (
	"fmt"
)

func TransfersHandler(msg []byte) {
	fmt.Println("🔄 [Transfers] 메시지:", string(msg))
}

func DexTradesHandler(msg []byte) {
	fmt.Println("📈 [DexTrades] 메시지:", string(msg))
}
