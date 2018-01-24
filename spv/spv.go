// Package spv project spv.go
package spv

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
)

// Spv is main type
type Spv struct {
	msgQueue    chan wire.Message
	status      int
	con         net.Conn
	pver        uint32
	params      chaincfg.Params
	errHeaders  bool
	errBlock    bool
	data        *Data
	ticker      *time.Ticker
	inv         bool
	checkHeight int
	checkTxIns  []func(*wire.TxIn)
	checkTxOuts []func(int, chainhash.Hash, int, *wire.TxOut)
	notifyFork  []func(int, int)
}

// NewSpv returns a new Spv
func NewSpv(params chaincfg.Params) (*Spv, error) {
	spv := &Spv{}
	spv.params = params
	spv.inv = false
	spv.errHeaders = false
	spv.errBlock = false
	dir, _ := filepath.Abs(filepath.Dir(os.Args[0]))
	dir = dir + "/data/"
	err := os.MkdirAll(dir, 0777)
	if err != nil {
		log.Printf("os.MkdirAll Error : %+v", err)
		return nil, err
	}
	data, err := NewData(params.Name, dir)
	if err != nil {
		log.Printf("NewData Error : %+v", err)
		return nil, err
	}
	spv.data = data
	err = spv.initHeaders()
	if err != nil {
		log.Printf("spv.initHeaders Error : %+v", err)
		return nil, err
	}
	return spv, nil
}

// AddCheckTxIn adds checkTxIn function
func (spv *Spv) AddCheckTxIn(checkTxIn func(*wire.TxIn)) error {
	exist := false
	f1 := reflect.ValueOf(checkTxIn)
	for _, f := range spv.checkTxIns {
		f2 := reflect.ValueOf(f)
		if f1.Pointer() == f2.Pointer() {
			exist = true
			break
		}
	}
	if exist {
		return fmt.Errorf("checkTxIn is already exist")
	}
	spv.checkTxIns = append(spv.checkTxIns, checkTxIn)
	return nil
}

// AddCheckTxOut adds checkTxOut function
func (spv *Spv) AddCheckTxOut(checkTxOut func(int, chainhash.Hash, int, *wire.TxOut)) error {
	exist := false
	f1 := reflect.ValueOf(checkTxOut)
	for _, f := range spv.checkTxOuts {
		f2 := reflect.ValueOf(f)
		if f1.Pointer() == f2.Pointer() {
			exist = true
			break
		}
	}
	if exist {
		return fmt.Errorf("checkTxOut is already exist")
	}
	spv.checkTxOuts = append(spv.checkTxOuts, checkTxOut)
	return nil
}

// Start is start spv
func (spv *Spv) Start() {
	if spv.ticker == nil {
		spv.ticker = time.NewTicker(time.Duration(3) * time.Second)
		err := spv.Connect()
		if err != nil {
			log.Printf("spv.Connect Error : %+v", err)
		}
		go spv.cyclic()
	}
}

// Stop is stop spv
func (spv *Spv) Stop() {
	if spv.ticker != nil {
		spv.ticker.Stop()
		spv.ticker = nil
	}
	spv.Close()
}

// IsConnect returns whether it is connected
func (spv *Spv) IsConnect() bool {
	if spv.con == nil {
		return false
	}
	return true
}

// Connect connects to the node
func (spv *Spv) Connect() error {
	if spv.IsConnect() {
		return fmt.Errorf("already connect")
	}
	// TODO
	con, err := net.Dial("tcp", "127.0.0.1:18444")
	if err != nil {
		return err
	}
	localAddr, err := net.ResolveTCPAddr("tcp", con.LocalAddr().String())
	if err != nil {
		defer spv.Close()
		return err
	}
	remoteAddr, err := net.ResolveTCPAddr("tcp", con.RemoteAddr().String())
	if err != nil {
		defer spv.Close()
		return err
	}
	me := wire.NewNetAddress(localAddr, 0)
	you := wire.NewNetAddress(remoteAddr, 0)
	msg := wire.NewMsgVersion(me, you, 0, 0)
	msg.AddService(wire.SFNodeBloom)
	msg.AddService(wire.SFNodeWitness)
	msg.AddUserAgent("samplespv", "0.0.1")

	spv.pver = uint32(msg.ProtocolVersion)
	spv.con = con

	go spv.recvHandler()

	spv.msgQueue = make(chan wire.Message)
	go spv.sendHandler()

	spv.msgQueue <- msg

	return nil
}

// Close close the connection
func (spv *Spv) Close() {
	if spv.msgQueue != nil {
		close(spv.msgQueue)
		spv.msgQueue = nil
	}
	if spv.con != nil {
		err := spv.con.Close()
		if err != nil {
			log.Printf("spv.con.Close error : %v", err)
		}
		spv.con = nil
	}
	err := spv.data.PutInt(KeyCheckHeight, spv.checkHeight)
	if err != nil {
		log.Printf("spv.data.PutInt error : %v", err)
	}
}

// SendMsgTx sends MsgTx
func (spv *Spv) SendMsgTx(tx *wire.MsgTx) error {
	err := spv.data.PutTx(tx)
	if err != nil {
		log.Printf("spv.data.PutTx Error : %+v", err)
		return err
	}
	hash := tx.TxHash()
	msg := wire.NewMsgInv()
	inv := wire.NewInvVect(wire.InvTypeTx, &hash)
	msg.AddInvVect(inv)
	spv.sendMsg(msg)
	return nil
}

func (spv *Spv) sendMsg(msg wire.Message) {
	spv.msgQueue <- msg
}

func (spv *Spv) recvHandler() {
	for {
		size, rmsg, _, err := wire.ReadMessageWithEncodingN(spv.con, spv.pver, spv.params.Net, wire.LatestEncoding)
		if err != nil {
			log.Printf("wire.ReadMessageWithEncodingN error : %v", err)
			if err == io.EOF {
				spv.con = nil
			}
			return
		}
		switch msg := rmsg.(type) {
		case *wire.MsgPing:
			log.Printf("<<< MsgPing:%x", msg.Nonce)
			sm := wire.NewMsgPong(msg.Nonce)
			spv.msgQueue <- sm
		case *wire.MsgHeaders:
			log.Printf("<<< MsgHeaders")
			spv.recvHeaders(msg)
		case *wire.MsgBlock:
			log.Printf("<<< MsgBlock %v", msg.Header.BlockHash())
			spv.recvBlock(msg)
		case *wire.MsgInv:
			log.Printf("<<< MsgInv")
			for _, inv := range msg.InvList {
				if inv.Type != wire.InvTypeBlock {
					continue
				}
				spv.inv = true
			}
		case *wire.MsgGetData:
			log.Printf("<<< MsgGetData")
			for _, inv := range msg.InvList {
				if inv.Type != wire.InvTypeTx {
					continue
				}
				tx, err := spv.data.GetTx(inv.Hash)
				if err != nil {
					log.Printf("spv.data.GetTx Error : %+v", err)
					continue
				}
				if tx == nil {
					log.Printf("Unknown hash %v", inv.Hash)
					continue
				}
				spv.sendMsg(tx)
				err = spv.data.DelTx(tx.TxHash())
				if err != nil {
					log.Printf("spv.data.DelTx Error : %+v", err)
				}
			}
		case *wire.MsgAddr:
			log.Printf("<<< MsgAddr:%v", msg.AddrList)
		case *wire.MsgVersion:
			log.Printf("<<< MsgVersion")
			sm := wire.NewMsgVerAck()
			spv.msgQueue <- sm
		case *wire.MsgVerAck:
			log.Printf("<<< MsgVerAck")
			spv.updateHeaders()
		default:
			log.Printf("<<< NoLogic %v:%v", msg.Command(), size)
		}
	}
}

func (spv *Spv) sendHandler() {
	for msg := range spv.msgQueue {
		size, err := wire.WriteMessageWithEncodingN(spv.con, msg, spv.pver, spv.params.Net, wire.LatestEncoding)
		if err != nil {
			log.Printf("wire.WriteMessageWithEncodingN error : %v", err)
			return
		}
		buf := &bytes.Buffer{}
		msg.BtcEncode(buf, 0, wire.LatestEncoding)
		log.Printf(">>> %v:%v %x", msg.Command(), size, buf)
	}
}

func (spv *Spv) cyclic() {
	for _ = range spv.ticker.C {
		if !spv.IsConnect() {
			err := spv.Connect()
			if err != nil {
				log.Printf("Spv Connect Error : %+v", err)
			}
		} else {
			if spv.inv {
				spv.inv = false
				spv.updateHeaders()
			}
			if spv.errHeaders {
				spv.errHeaders = false
				spv.updateHeaders()
			}
			if spv.errBlock {
				spv.errBlock = false
				spv.updateBlock()
			}
		}
	}
}
