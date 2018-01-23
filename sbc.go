// sbc
package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/tnakagawa/sbc/spv"
	"github.com/tnakagawa/sbc/wallet"
)

func main() {
	spv := spv.NewSpv()
	wallet := wallet.NewWallet()
	spv.AddCheckTxIn(wallet.CheckTxIn)
	spv.AddCheckTxOut(wallet.CheckTxOut)
	spv.Start()
	defer spv.Stop()
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Print("$ ")
	for scanner.Scan() {
		line := scanner.Text()
		items := strings.Split(line, " ")
		if len(items) > 0 {
			cmd := items[0]
			if cmd == "exit" {
				break
			}
		}
		fmt.Print("$ ")
	}
	fmt.Println("bye!")
}
