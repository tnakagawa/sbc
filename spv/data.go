// Package spv project data.go
package spv

import (
	"bytes"
	"database/sql"
	"fmt"
	"log"
	"math/big"
	"sync"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"

	// sqlite3 driver
	_ "github.com/mattn/go-sqlite3"
)

// key names for kvs
const (
	KeyCheckHeight = "checkHeight"
)

// Data is data type
type Data struct {
	name    string
	datadir string
	mutex   *sync.Mutex
}

// NewData returns a new Data
func NewData(name, datadir string) (*Data, error) {
	data := &Data{}
	data.name = name
	data.datadir = datadir
	data.mutex = new(sync.Mutex)
	err := data.init()
	if err != nil {
		log.Printf("data.init Error : %+v", err)
		return nil, err
	}
	return data, nil
}

func (data *Data) init() error {
	db, err := data.openDb()
	defer data.closeDb(db)
	if err != nil {
		log.Printf("data.openDb Error : %+v", err)
		return err
	}
	tables := [][]string{
		{"headers", "CREATE TABLE headers (hash BLOB, height INTEGER, data BLOB, PRIMARY KEY(hash), UNIQUE(height))"},
		{"kvs", "CREATE TABLE kvs (key TEXT, val BLOB, PRIMARY KEY(key))"},
		{"tx", "CREATE TABLE tx (hash BLOB, data BLOB, PRIMARY KEY(hash))"},
	}
	for _, table := range tables {
		rows, err := db.Query("SELECT name FROM sqlite_master WHERE name = ?", table[0])
		if err != nil {
			log.Printf("db.Query Error : %+v", err)
			return err
		}
		exist := rows.Next()
		rows.Close()
		if exist {
			continue
		}
		tx, err := db.Begin()
		if err != nil {
			log.Printf("db.Begin Error : %+v", err)
			return err
		}
		_, err = tx.Exec(table[1])
		if err != nil {
			tx.Rollback()
			log.Printf("tx.Exec : %+v", err)
			return err
		}
		err = tx.Commit()
		if err != nil {
			log.Printf("tx.Commit Error : %+v", err)
			return err
		}
	}
	return nil
}

func (data *Data) openDb() (*sql.DB, error) {
	data.mutex.Lock()
	dataSourceName := fmt.Sprintf("file:%sheaders-%s.db?cache=shared&mode=rwc", data.datadir, data.name)
	log.Printf("%s", dataSourceName)
	db, err := sql.Open("sqlite3", dataSourceName)
	if err != nil {
		log.Printf("sql.Open Error : %+v", err)
		return nil, err
	}
	return db, nil
}

func (data *Data) closeDb(db *sql.DB) {
	if db != nil {
		err := db.Close()
		if err != nil {
			log.Printf("db.Close Error : %+v", err)
		}
	}
	data.mutex.Unlock()
}

// KVS

// PutInt puts key and number
func (data *Data) PutInt(key string, i int) error {
	if i == 0 {
		return data.Put(key, []byte{0x00})
	}
	bi := big.NewInt(int64(i))
	return data.Put(key, bi.Bytes())
}

// GetInt gets number by key
func (data *Data) GetInt(key string, defaultInt int) (int, error) {
	bs, err := data.get(nil, key)
	if err != nil {
		log.Printf("data.get Error : %+v", err)
		return defaultInt, err
	}
	if bs == nil {
		return defaultInt, nil
	}
	bi := new(big.Int)
	bi.SetBytes(bs)
	return int(bi.Int64()), nil
}

// Del delete key
func (data *Data) Del(key string) error {
	db, err := data.openDb()
	defer data.closeDb(db)
	if err != nil {
		log.Printf("data.openDb Error : %+v", err)
		return err
	}
	bs, err := data.get(db, key)
	if err != nil {
		log.Printf("data.get Error : %+v", err)
		return err
	}
	if bs == nil {
		return nil
	}
	tx, err := db.Begin()
	if err != nil {
		log.Printf("db.Begin Error : %+v", err)
		return err
	}
	_, err = tx.Exec("DELETE FROM kvs WHERE key=?", key)
	if err != nil {
		tx.Rollback()
		log.Printf("tx.Exec : %+v", err)
		return err
	}
	err = tx.Commit()
	if err != nil {
		log.Printf("tx.Commit Error : %+v", err)
		return err
	}
	return nil
}

func (data *Data) Put(key string, val []byte) error {
	db, err := data.openDb()
	defer data.closeDb(db)
	if err != nil {
		log.Printf("data.openDb Error : %+v", err)
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		log.Printf("db.Begin Error : %+v", err)
		return err
	}
	_, err = tx.Exec("INSERT OR REPLACE INTO kvs (key,val) VALUES (?,?)", key, val)
	if err != nil {
		tx.Rollback()
		log.Printf("tx.Exec : %+v", err)
		return err
	}
	err = tx.Commit()
	if err != nil {
		log.Printf("tx.Commit Error : %+v", err)
		return err
	}
	return nil
}

func (data *Data) Get(key string) ([]byte, error) {
	return data.get(nil, key)
}

func (data *Data) get(db *sql.DB, key string) ([]byte, error) {
	var err error
	if db == nil {
		db, err = data.openDb()
		defer data.closeDb(db)
		if err != nil {
			log.Printf("data.openDb Error : %+v", err)
			return nil, err
		}
	}
	var val []byte
	err = db.QueryRow("SELECT val FROM kvs WHERE key=?", key).Scan(&val)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		log.Printf("db.QueryRow Error : %+v", err)
		return nil, err
	}
	return val, nil
}

// Header

// GetHeaderByHash gets header by hash
func (data *Data) GetHeaderByHash(hash chainhash.Hash) (*wire.BlockHeader, int, error) {
	return data.getHeader(hash.CloneBytes(), -1)
}

// GetHeaderByHeight gets header by height
func (data *Data) GetHeaderByHeight(height int) (*wire.BlockHeader, int, error) {
	return data.getHeader(nil, height)
}

func (data *Data) getHeader(hash []byte, height int) (*wire.BlockHeader, int, error) {
	db, err := data.openDb()
	defer data.closeDb(db)
	if err != nil {
		log.Printf("data.openDb Error : %+v", err)
		return nil, -1, err
	}
	var bs []byte
	if hash != nil {
		err = db.QueryRow("SELECT height, data FROM headers WHERE hash=?", hash).Scan(&height, &bs)
	} else {
		err = db.QueryRow("SELECT data FROM headers WHERE height=?", height).Scan(&bs)
	}
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, -1, nil
		}
		log.Printf("db.QueryRow Error : %+v", err)
		return nil, -1, err
	}
	header, err := data.deserialize(bs)
	if err != nil {
		log.Printf("data.deserialize Error : %+v", err)
		return nil, -1, err
	}
	return header, height, nil
}

// PutHeaders puts headers
func (data *Data) PutHeaders(headers []*wire.BlockHeader, startHeight int) error {
	db, err := data.openDb()
	defer data.closeDb(db)
	if err != nil {
		log.Printf("data.openDb Error : %+v", err)
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		log.Printf("db.Begin Error : %+v", err)
		return err
	}
	for i, header := range headers {
		hash := header.BlockHash()
		bs := data.serialize(header)
		_, err := tx.Exec("INSERT INTO headers (hash,height,data) VALUES (?,?,?)", hash.CloneBytes(), startHeight+i, bs)
		if err != nil {
			tx.Rollback()
			log.Printf("tx.Exec : %+v", err)
			return err
		}
		log.Print(startHeight+i, hash)
	}
	err = tx.Commit()
	if err != nil {
		log.Printf("tx.Commit Error : %+v", err)
		return err
	}
	return nil
}

// GetMinMaxHeight gets count, max and min height
// if count is zero, max and min is -1
func (data *Data) GetCntMinMaxHeight() (int, int, int, error) {
	db, err := data.openDb()
	defer data.closeDb(db)
	if err != nil {
		log.Printf("data.openDb Error : %+v", err)
		return -1, -1, -1, err
	}
	var cnt int
	err = db.QueryRow("SELECT COUNT(hash) FROM headers").Scan(&cnt)
	if err != nil {
		log.Printf("db.QueryRow Error : %+v", err)
		return -1, -1, -1, err
	}
	if cnt == 0 {
		return cnt, -1, -1, nil
	}
	var max int
	var min int
	err = db.QueryRow("SELECT MIN(height), MAX(height) FROM headers").Scan(&min, &max)
	if err != nil {
		log.Printf("db.QueryRow Error : %+v", err)
		return -1, -1, -1, err
	}
	return cnt, min, max, nil
}

func (data *Data) serialize(header *wire.BlockHeader) []byte {
	buf := &bytes.Buffer{}
	header.Serialize(buf)
	return buf.Bytes()
}

func (data *Data) deserialize(bs []byte) (*wire.BlockHeader, error) {
	if bs == nil {
		return nil, nil
	}
	buf := &bytes.Buffer{}
	_, err := buf.Write(bs)
	if err != nil {
		log.Printf("buf.Write Error : %+v", err)
		return nil, err
	}
	header := new(wire.BlockHeader)
	err = header.Deserialize(buf)
	if err != nil {
		log.Printf("header.Deserialize Error : %+v", err)
		return nil, err
	}
	return header, nil
}

// Tx

// PutTx puts MsgTx
func (data *Data) PutTx(msgTx *wire.MsgTx) error {
	db, err := data.openDb()
	defer data.closeDb(db)
	if err != nil {
		log.Printf("data.openDb Error : %+v", err)
		return err
	}
	hash := msgTx.TxHash()
	bs, err := data.msgTxToBs(msgTx)
	if err != nil {
		log.Printf("data.msgTxToBs Error : %+v", err)
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		log.Printf("db.Begin Error : %+v", err)
		return err
	}
	_, err = tx.Exec("INSERT INTO tx (hash,data) VALUES (?,?)", hash.CloneBytes(), bs)
	if err != nil {
		tx.Rollback()
		log.Printf("tx.Exec : %+v", err)
		return err
	}
	err = tx.Commit()
	if err != nil {
		log.Printf("tx.Commit Error : %+v", err)
		return err
	}
	return nil
}

// GetTx gets MsgTx by hash
func (data *Data) GetTx(hash chainhash.Hash) (*wire.MsgTx, error) {
	db, err := data.openDb()
	defer data.closeDb(db)
	if err != nil {
		log.Printf("data.openDb Error : %+v", err)
		return nil, err
	}
	var bs []byte
	err = db.QueryRow("SELECT data FROM tx WHERE hash=?", hash.CloneBytes()).Scan(&bs)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		log.Printf("db.QueryRow Error : %+v", err)
		return nil, err
	}
	tx, err := data.bsToMsgTx(bs)
	if err != nil {
		log.Printf("data.bsToMsgTx Error : %+v", err)
		return nil, err
	}
	return tx, nil
}

// ListTxHash gets transaction hashes
func (data *Data) ListTxHash() ([]chainhash.Hash, error) {
	db, err := data.openDb()
	defer data.closeDb(db)
	if err != nil {
		log.Printf("data.openDb Error : %+v", err)
		return nil, err
	}
	rows, err := db.Query("SELECT hash FROM tx")
	if err != nil {
		log.Printf("db.Query Error : %+v", err)
		return nil, err
	}
	var list []chainhash.Hash
	for rows.Next() {
		var hash chainhash.Hash
		rows.Scan(&hash)
		list = append(list, hash)
	}
	return list, nil
}

// DelTx delete MsgTx by hash
func (data *Data) DelTx(hash chainhash.Hash) error {
	db, err := data.openDb()
	defer data.closeDb(db)
	if err != nil {
		log.Printf("data.openDb Error : %+v", err)
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		log.Printf("db.Begin Error : %+v", err)
		return err
	}
	_, err = tx.Exec("DELETE FROM tx WHERE hash=?", hash.CloneBytes())
	if err != nil {
		tx.Rollback()
		log.Printf("tx.Exec : %+v", err)
		return err
	}
	err = tx.Commit()
	if err != nil {
		log.Printf("tx.Commit Error : %+v", err)
		return err
	}
	return nil
}

func (data *Data) msgTxToBs(tx *wire.MsgTx) ([]byte, error) {
	buf := &bytes.Buffer{}
	err := tx.Serialize(buf)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (data *Data) bsToMsgTx(bs []byte) (*wire.MsgTx, error) {
	tx := &wire.MsgTx{}
	buf := &bytes.Buffer{}
	_, err := buf.Write(bs)
	if err != nil {
		return nil, err
	}
	err = tx.Deserialize(buf)
	if err != nil {
		tx = &wire.MsgTx{}
		buf := &bytes.Buffer{}
		_, err := buf.Write(bs)
		if err != nil {
			return nil, err
		}
		err = tx.DeserializeNoWitness(buf)
		if err != nil {
			return nil, err
		}
	}
	return tx, nil
}
