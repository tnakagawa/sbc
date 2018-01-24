// Package spv project headers.go
package spv

import (
	"log"

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
	_, height = spv.getInitHashHeight()
	err = spv.data.PutInt(KeyCheckHeight, height)
	if err != nil {
		log.Printf("spv.data.PutInt Error : %+v", err)
		return err
	}
	spv.checkHeight = height
	return nil
}

func (spv *Spv) getInitHashHeight() (*chainhash.Hash, int) {
	hash := spv.params.GenesisHash
	height := 0
	if len(spv.params.Checkpoints) > 0 {
		checkpoint := spv.params.Checkpoints[len(spv.params.Checkpoints)-1]
		hash = checkpoint.Hash
		height = int(checkpoint.Height)
	}
	return hash, height
}

func (spv *Spv) updateHeaders() {
	msg := wire.NewMsgGetHeaders()
	msg.ProtocolVersion = spv.pver
	height := spv.checkHeight
	cnt, min, max, err := spv.data.GetCntMinMaxHeight()
	if err != nil {
		log.Printf("spv.data.GetCntMinMaxHeight Error : %+v", err)
		spv.errHeaders = true
		return
	}
	if cnt == 0 {
		hash, height := spv.getInitHashHeight()
		log.Printf("get first header : %d , %v", height, hash)
		msg := wire.NewMsgGetData()
		inv := wire.NewInvVect(wire.InvTypeBlock, hash)
		msg.AddInvVect(inv)
		spv.sendMsg(msg)
		return
	}
	if max-min > 6 {
		height = max - 6
	}
	header, _, err := spv.data.GetHeaderByHeight(height)
	if err != nil {
		log.Printf("spv.data.GetMaxHeight Error : %+v", err)
		spv.errHeaders = true
		return
	}
	blockHash := header.BlockHash()
	msg.AddBlockLocatorHash(&blockHash)
	spv.sendMsg(msg)
}

func (spv *Spv) recvHeaders(msg *wire.MsgHeaders) {
	if len(msg.Headers) == 0 {
		spv.updateBlock()
		return
	}
	cnt, _, lastHeight, err := spv.data.GetCntMinMaxHeight()
	if err != nil {
		log.Printf("spv.data.GetCntMinMaxHeight Error : %+v", err)
		spv.errHeaders = true
		return
	}
	if cnt == 0 {
		log.Printf("headers count is zero")
		spv.errHeaders = true
		return
	}
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
		break
	}
	if len(msg.Headers) == 2000 {
		smsg := wire.NewMsgGetHeaders()
		smsg.ProtocolVersion = spv.pver
		blockhash := msg.Headers[len(msg.Headers)-1].BlockHash()
		smsg.AddBlockLocatorHash(&blockhash)
		spv.sendMsg(smsg)
	} else {
		spv.updateBlock()
	}
	return
}
