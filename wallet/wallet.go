// wallet project wallet.go
package wallet

import (
	"log"
	"math/rand"
	"time"

	"github.com/adiabat/btcutil"
	"github.com/btcsuite/btcd/btcec"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcutil/hdkeychain"
)

// Wallet is wallet type
type Wallet struct {
	extKey *hdkeychain.ExtendedKey
	utxom  map[wire.OutPoint]*Utxo
	pkhs   []*Pkh
}

// Utxo is utxo type
type Utxo struct {
	height   int
	outpoint *wire.OutPoint
	value    int64
	path     int
	kind     int
	status   int
}

// Pkh is pulickey hash type
type Pkh struct {
	hash []byte
	path int
}

// WalletPublickeyNumber is publickeys number
const WalletPublickeyNumber = 255

// WalletUtxoKind
const (
	WalletUtxoKindP2PKH = iota + 1
	WalletUtxoKindP2WPKH
	WalletUtxoKindUnknown = 999
)

// WalletUtxoStatus
const (
	WalletUtxoStatusCanUse = iota + 1
	WalletUtxoStatusLock
	WalletUtxoStatusUsed
	WalletUtxoStatusFork = 999
)

// NewWallet returns a new Wallet
func NewWallet() *Wallet {
	rand.Seed(time.Now().UnixNano())
	wallet := &Wallet{}
	seed := chainhash.HashB([]byte("wallet"))
	var err error
	wallet.extKey, err = hdkeychain.NewMaster(seed, &chaincfg.RegressionNetParams)
	if err != nil {
		log.Printf("hdkeychain.NewMaster error : %v", err)
		return nil
	}
	wallet.utxom = make(map[wire.OutPoint]*Utxo)
	err = wallet.loadPublickKeys()
	if err != nil {
		log.Printf("wallet.loadPublickKeys error : %v", err)
		return nil
	}
	return wallet
}

func (wallet *Wallet) loadPublickKeys() error {
	for i := 0; i < WalletPublickeyNumber; i++ {
		_, pub, err := wallet.getKeyPair(wallet.extKey,
			44+hdkeychain.HardenedKeyStart,
			1+hdkeychain.HardenedKeyStart,
			0+hdkeychain.HardenedKeyStart,
			0,
			i)
		if err != nil {
			log.Printf("hdkeychain.NewMaster error : %v", err)
			return err
		}
		hash := btcutil.Hash160(pub.SerializeCompressed())
		pkh := &Pkh{}
		pkh.hash = hash
		pkh.path = i
		wallet.pkhs = append(wallet.pkhs, pkh)
	}
	return nil
}

// GetNewPkh returns new publickey hash
func (wallet *Wallet) GetNewPkh() (*Pkh, error) {
	path := rand.Intn(WalletPublickeyNumber)
	_, pub, err := wallet.getKeyPair(wallet.extKey,
		44+hdkeychain.HardenedKeyStart,
		1+hdkeychain.HardenedKeyStart,
		0+hdkeychain.HardenedKeyStart,
		0,
		path)
	if err != nil {
		log.Printf("hdkeychain.NewMaster error : %v", err)
		return nil, err
	}
	hash := btcutil.Hash160(pub.SerializeCompressed())
	pkh := &Pkh{}
	pkh.hash = hash
	pkh.path = path
	return pkh, nil
}

// CheckTxIn check txin
func (wallet *Wallet) CheckTxIn(txin *wire.TxIn) {
	utxo, ok := wallet.utxom[txin.PreviousOutPoint]
	if !ok {
		return
	}
	if utxo.status == WalletUtxoStatusUsed {
		return
	}
	if txin.PreviousOutPoint.Index == utxo.outpoint.Index &&
		txin.PreviousOutPoint.Hash.IsEqual(&(utxo.outpoint.Hash)) {
		utxo.status = WalletUtxoStatusUsed
		return
	}
}

// CheckTxOut check txout
func (wallet *Wallet) CheckTxOut(height int, txid chainhash.Hash, index int, txout *wire.TxOut) {
	exist := false
	pkScript := txout.PkScript
	size := len(pkScript)
	var hash []byte
	kind := WalletUtxoKindUnknown
	if size == 22 {
		if pkScript[0] != 0x00 || pkScript[1] != 0x14 {
			return
		}
		kind = WalletUtxoKindP2WPKH
		hash = pkScript[2:]
	} else if size == 25 {
		if pkScript[0] != 0x76 || pkScript[1] != 0xa9 || pkScript[2] != 0x14 ||
			pkScript[23] != 0x88 || pkScript[24] != 0xac {
			return
		}
		kind = WalletUtxoKindP2PKH
		hash = pkScript[3:23]
	} else {
		return
	}
	//log.Printf("CheckTxout Hash %x", hash)
	path := -1
	for _, pkh := range wallet.pkhs {
		//if reflect.DeepEqual(pkh.hash, hash) {
		if wallet.beq(pkh.hash, hash) {
			exist = true
			path = pkh.path
			log.Printf("CheckTxout Match Hash %x", hash)
			break
		}
	}
	if !exist {
		return
	}
	outpoint := wire.NewOutPoint(&txid, uint32(index))
	_, ok := wallet.utxom[*outpoint]
	if ok {
		return
	}
	utxo := &Utxo{}
	utxo.height = height
	utxo.outpoint = outpoint
	utxo.value = txout.Value
	utxo.path = path
	utxo.status = WalletUtxoStatusCanUse
	utxo.kind = kind
	wallet.utxom[*outpoint] = utxo
}

func (wallet *Wallet) beq(bs1, bs2 []byte) bool {
	result := false
	if len(bs1) == len(bs2) {
		result = true
		for i, b := range bs1 {
			if b != bs2[i] {
				result = false
				break
			}
		}
	}
	return result
}

// DumpPkScript dump pkScript
func (wallet *Wallet) DumpPkScript() {
	for _, pkh := range wallet.pkhs {
		log.Printf("hash path %x %v", pkh.hash, pkh.path)
	}
}

// DumpUtxo dump utxo
func (wallet *Wallet) DumpUtxo() {
	for _, utxo := range wallet.utxom {
		log.Printf("utxo %+v", utxo)
	}
}

func (wallet *Wallet) getKeyPair(extKey *hdkeychain.ExtendedKey, path ...int) (*btcec.PrivateKey, *btcec.PublicKey, error) {
	var key *hdkeychain.ExtendedKey
	key = extKey
	var err error
	for _, i := range path {
		key, err = key.Child(uint32(i))
		if err != nil {
			return nil, nil, err
		}
	}
	if err != nil {
		return nil, nil, err
	}
	prvKey, err := key.ECPrivKey()
	if err != nil {
		return nil, nil, err
	}
	pubKey, err := key.ECPubKey()
	if err != nil {
		return nil, nil, err
	}
	return prvKey, pubKey, nil
}
