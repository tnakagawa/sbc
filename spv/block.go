// Package spv project block.go
package spv

import (
	"log"

	"github.com/btcsuite/btcd/wire"
)

func (spv *Spv) updateBlock() {
	header, _, err := spv.data.GetHeaderByHeight(spv.checkHeight)
	if err != nil {
		log.Printf("spv.data.GetHeaderByHeight Error : %+v", err)
		spv.errBlock = true
		return
	}
	if header == nil {
		err := spv.data.PutInt(KeyCheckHeight, spv.checkHeight)
		if err != nil {
			log.Printf("spv.data.PutInt Error : %+v", err)
			spv.errBlock = true
			return
		}
		return
	}
	hash := header.BlockHash()
	msg := wire.NewMsgGetData()
	inv := wire.NewInvVect(wire.InvTypeBlock, &hash)
	msg.AddInvVect(inv)
	spv.sendMsg(msg)
}

func (spv *Spv) recvBlock(block *wire.MsgBlock) {
	header, height, err := spv.data.GetHeaderByHash(block.BlockHash())
	if err != nil {
		log.Printf("spv.data.GetHeaderByHash Error : %+v", err)
		spv.errBlock = true
		return
	}
	if header == nil {
		hash, height := spv.getInitHashHeight()
		blockHash := block.BlockHash()
		if !blockHash.IsEqual(hash) {
			log.Printf("Not found header height : %d", height)
			spv.errBlock = true
			return
		}
		err := spv.data.PutHeaders([]*wire.BlockHeader{&block.Header}, height)
		if err != nil {
			log.Printf("spv.data.PutHeaders Error : %+v", err)
			spv.errHeaders = true
			return
		}
		spv.updateHeaders()
		return
	}
	if height != spv.checkHeight {
		log.Printf("unmatch height : %d %d", height, spv.checkHeight)
		spv.errBlock = true
		return
	}
	for _, tx := range block.Transactions {
		for _, txin := range tx.TxIn {
			for _, checkTxIn := range spv.checkTxIns {
				checkTxIn(txin)
			}
		}
		for idx, txout := range tx.TxOut {
			for _, checkTxOut := range spv.checkTxOuts {
				checkTxOut(height, tx.TxHash(), idx, txout)
			}
		}
	}
	spv.checkHeight++
	spv.updateBlock()
}
