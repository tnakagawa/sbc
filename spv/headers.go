// Package spv project headers.go
package spv

import (
	"fmt"
	"log"
	"time"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
)

func (spv *Spv) initHeaders() error {
	height, err := spv.data.GetInt(KeyCheckHeight, -1)
	if err != nil {
		log.Printf("spv.data.GetInt Error : %+v", err)
		return err
	}
	if height >= 0 {
		spv.checkHeight = height
		return nil
	}
	var header *wire.BlockHeader
	switch spv.btcnet {
	case wire.TestNet:
		// 0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206
		prevHash, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000000")
		merkleRootHash, _ := chainhash.NewHashFromStr("4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b")
		header = &wire.BlockHeader{
			Version:    1,
			PrevBlock:  *prevHash,                // 0000000000000000000000000000000000000000000000000000000000000000
			MerkleRoot: *merkleRootHash,          // 4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b
			Timestamp:  time.Unix(1296688602, 0), // 2011-02-02 23:16:42 +0000 UTC
			Bits:       0x207fffff,               // 545259519 [7fffff0000000000000000000000000000000000000000000000000000000000]
			Nonce:      2,
		}
		height = 0
	default:
		return fmt.Errorf("Unknown btcnet %d", spv.btcnet)
	}
	err = spv.data.PutHeaders([]*wire.BlockHeader{header}, height)
	if err != nil {
		log.Printf("spv.data.PutHeader Error : %+v", err)
		return err
	}
	err = spv.data.PutInt(KeyCheckHeight, height)
	if err != nil {
		log.Printf("spv.data.PutInt Error : %+v", err)
		return err
	}
	spv.checkHeight = height
	return nil
}

func (spv *Spv) updateHeaders() {
	msg := wire.NewMsgGetHeaders()
	msg.ProtocolVersion = spv.pver
	min, max, err := spv.data.GetMinMaxHeight()
	if err != nil {
		log.Printf("spv.data.GetMinMaxHeight Error : %+v", err)
		spv.errHeaders = true
		return
	}
	height := min
	if max-min > 6 {
		height = max - 6
	}
	header, _, err := spv.data.GetHeaderByHeight(height)
	if err != nil {
		log.Printf("spv.data.GetMaxHeight Error : %+v", err)
		spv.errHeaders = true
		return
	}
	blockhash := header.BlockHash()
	msg.AddBlockLocatorHash(&blockhash)
	spv.sendMsg(msg)
}

func (spv *Spv) recvHeaders(msg *wire.MsgHeaders) {
	if len(msg.Headers) == 0 {
		spv.updateBlock()
		return
	}
	_, lastHeight, err := spv.data.GetMinMaxHeight()
	if err != nil {
		log.Printf("spv.data.GetMinMaxHeight Error : %+v", err)
		spv.errHeaders = true
		return
	}
	var blockhash chainhash.Hash
	for i, header := range msg.Headers {
		hash := header.BlockHash()
		gheader, _, err := spv.data.GetHeaderByHash(hash)
		if err != nil {
			log.Printf("spv.data.GetHeaderByHash Error : %+v", err)
			spv.errHeaders = true
			return
		}
		if gheader != nil {
			continue
		}
		_, height, err := spv.data.GetHeaderByHash(header.PrevBlock)
		if lastHeight != height {
			// TODO Fork
			log.Printf("Fork!")
			return
		}
		err = spv.data.PutHeaders(msg.Headers[i:], height+1)
		if err != nil {
			log.Printf("spv.data.PutHeader Error : %+v", err)
			spv.errHeaders = true
			return
		}
		blockhash = msg.Headers[len(msg.Headers)-1].BlockHash()
		break
	}
	if len(msg.Headers) == 2000 {
		smsg := wire.NewMsgGetHeaders()
		smsg.ProtocolVersion = spv.pver
		smsg.AddBlockLocatorHash(&blockhash)
		spv.sendMsg(smsg)
	} else {
		spv.updateBlock()
	}
	return
}
