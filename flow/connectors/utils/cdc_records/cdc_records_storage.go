//nolint:stylecheck
package cdc_records

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"os"
	"time"

	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
	"github.com/PeerDB-io/peer-flow/shared"
	"github.com/cockroachdb/pebble"
)

func encVal(val any) ([]byte, error) {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	err := enc.Encode(val)
	if err != nil {
		return []byte{}, fmt.Errorf("unable to encode value %v: %w", val, err)
	}
	return buf.Bytes(), nil
}

type cdcRecordsStore struct {
	inMemoryRecords           map[model.TableWithPkey]model.Record
	pebbleDB                  *pebble.DB
	numRecords                int
	flowJobName               string
	dbFolderName              string
	numRecordsSwitchThreshold int
}

func NewCDCRecordsStore(flowJobName string) *cdcRecordsStore {
	return &cdcRecordsStore{
		inMemoryRecords:           make(map[model.TableWithPkey]model.Record),
		pebbleDB:                  nil,
		numRecords:                0,
		flowJobName:               flowJobName,
		dbFolderName:              fmt.Sprintf("%s/%s_%s", os.TempDir(), flowJobName, shared.RandomString(8)),
		numRecordsSwitchThreshold: peerdbenv.PeerDBCDCDiskSpillThreshold(),
	}
}

func (c *cdcRecordsStore) initPebbleDB() error {
	if c.pebbleDB != nil {
		return nil
	}

	// register future record classes here as well, if they are passed/stored as interfaces
	gob.Register(&model.InsertRecord{})
	gob.Register(&model.UpdateRecord{})
	gob.Register(&model.DeleteRecord{})
	gob.Register(&time.Time{})
	gob.Register(&big.Rat{})

	var err error
	// we don't want a WAL since cache, we don't want to overwrite another DB either
	c.pebbleDB, err = pebble.Open(c.dbFolderName, &pebble.Options{
		DisableWAL:         true,
		ErrorIfExists:      true,
		FormatMajorVersion: pebble.FormatNewest,
	})
	if err != nil {
		return fmt.Errorf("failed to initialize Pebble database: %w", err)
	}
	return nil
}

func (c *cdcRecordsStore) Set(key model.TableWithPkey, rec model.Record) error {
	_, ok := c.inMemoryRecords[key]
	if ok || len(c.inMemoryRecords) < c.numRecordsSwitchThreshold {
		c.inMemoryRecords[key] = rec
	} else {
		if c.pebbleDB == nil {
			slog.Info(fmt.Sprintf("more than %d primary keys read, spilling to disk",
				c.numRecordsSwitchThreshold),
				slog.String("flowName", c.flowJobName))
			err := c.initPebbleDB()
			if err != nil {
				return err
			}
		}

		encodedKey, err := encVal(key)
		if err != nil {
			return err
		}
		// necessary to point pointer to interface so the interface is exposed
		// instead of the underlying type
		encodedRec, err := encVal(&rec)
		if err != nil {
			return err
		}
		// we're using Pebble as a cache, no need for durability here.
		err = c.pebbleDB.Set(encodedKey, encodedRec, &pebble.WriteOptions{
			Sync: false,
		})
		if err != nil {
			return fmt.Errorf("unable to store value in Pebble: %w", err)
		}
	}
	c.numRecords++
	return nil
}

// bool is to indicate if a record is found or not [similar to ok]
func (c *cdcRecordsStore) Get(key model.TableWithPkey) (model.Record, bool, error) {
	rec, ok := c.inMemoryRecords[key]
	if ok {
		return rec, true, nil
	} else if c.pebbleDB != nil {
		encodedKey, err := encVal(key)
		if err != nil {
			return nil, false, err
		}
		encodedRec, closer, err := c.pebbleDB.Get(encodedKey)
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				return nil, false, nil
			} else {
				return nil, false, fmt.Errorf("error while retrieving value with key %v: %w", key, err)
			}
		}
		defer func() {
			err := closer.Close()
			if err != nil {
				slog.Warn("failed to close database",
					slog.Any("error", err),
					slog.String("flowName", c.flowJobName))
			}
		}()

		dec := gob.NewDecoder(bytes.NewReader(encodedRec))
		var rec model.Record
		err = dec.Decode(&rec)
		if err != nil {
			return nil, false, fmt.Errorf("failed to decode record: %w", err)
		}

		return rec, true, nil
	}
	return nil, false, nil
}

func (c *cdcRecordsStore) IsEmpty() bool {
	return c.numRecords == 0
}

func (c *cdcRecordsStore) Len() int {
	return c.numRecords
}

func (c *cdcRecordsStore) Close() error {
	c.inMemoryRecords = nil
	if c.pebbleDB != nil {
		err := c.pebbleDB.Close()
		if err != nil {
			return fmt.Errorf("failed to close database: %w", err)
		}
	}
	err := os.RemoveAll(c.dbFolderName)
	if err != nil {
		return fmt.Errorf("failed to delete database file: %w", err)
	}
	return nil
}
