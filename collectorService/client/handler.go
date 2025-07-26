package client

import (
	"fmt"
)

func TransfersHandler(msg []byte) {
	fmt.Println("🔄 [Transfers] Message:", string(msg))
}

func DexTradesHandler(msg []byte) {
	fmt.Println("📈 [DexTrades] Message:", string(msg))
}
