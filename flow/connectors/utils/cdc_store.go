package utils

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"runtime/metrics"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/shopspring/decimal"
	"go.temporal.io/sdk/log"

	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
	"github.com/PeerDB-io/peer-flow/shared"
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

type cdcStore[Items model.Items] struct {
	inMemoryRecords           map[model.TableWithPkey]model.Record[Items]
	pebbleDB                  *pebble.DB
	flowJobName               string
	dbFolderName              string
	thresholdReason           string
	memStats                  []metrics.Sample
	memThresholdBytes         uint64
	numRecords                atomic.Int32
	numRecordsSwitchThreshold int
}

func NewCDCStore[Items model.Items](ctx context.Context, env map[string]string, flowJobName string) (*cdcStore[Items], error) {
	numRecordsSwitchThreshold, err := peerdbenv.PeerDBCDCDiskSpillRecordsThreshold(ctx, env)
	if err != nil {
		return nil, fmt.Errorf("failed to get CDC disk spill records threshold: %w", err)
	}
	memPercent, err := peerdbenv.PeerDBCDCDiskSpillMemPercentThreshold(ctx, env)
	if err != nil {
		return nil, fmt.Errorf("failed to get CDC disk spill memory percent threshold: %w", err)
	}

	return &cdcStore[Items]{
		inMemoryRecords:           make(map[model.TableWithPkey]model.Record[Items]),
		pebbleDB:                  nil,
		numRecords:                atomic.Int32{},
		flowJobName:               flowJobName,
		dbFolderName:              fmt.Sprintf("%s/%s_%s", os.TempDir(), flowJobName, shared.RandomString(8)),
		numRecordsSwitchThreshold: int(numRecordsSwitchThreshold),
		memThresholdBytes: func() uint64 {
			maxMemBytes := peerdbenv.PeerDBFlowWorkerMaxMemBytes()
			if memPercent > 0 && maxMemBytes > 0 {
				return maxMemBytes * uint64(memPercent) / 100
			}
			return 0
		}(),
		thresholdReason: "",
		memStats:        []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}},
	}, nil
}

func init() {
	// register future record classes here as well, if they are passed/stored as interfaces
	gob.Register(time.Time{})
	gob.Register(decimal.Decimal{})
	gob.Register(qvalue.QValueNull(""))
	gob.Register(qvalue.QValueInvalid{})
	gob.Register(qvalue.QValueFloat32{})
	gob.Register(qvalue.QValueFloat64{})
	gob.Register(qvalue.QValueInt16{})
	gob.Register(qvalue.QValueInt32{})
	gob.Register(qvalue.QValueInt64{})
	gob.Register(qvalue.QValueBoolean{})
	gob.Register(qvalue.QValueStruct{})
	gob.Register(qvalue.QValueQChar{})
	gob.Register(qvalue.QValueString{})
	gob.Register(qvalue.QValueTimestamp{})
	gob.Register(qvalue.QValueTimestampTZ{})
	gob.Register(qvalue.QValueDate{})
	gob.Register(qvalue.QValueTime{})
	gob.Register(qvalue.QValueTimeTZ{})
	gob.Register(qvalue.QValueInterval{})
	gob.Register(qvalue.QValueNumeric{})
	gob.Register(qvalue.QValueBytes{})
	gob.Register(qvalue.QValueUUID{})
	gob.Register(qvalue.QValueJSON{})
	gob.Register(qvalue.QValueHStore{})
	gob.Register(qvalue.QValueGeography{})
	gob.Register(qvalue.QValueGeometry{})
	gob.Register(qvalue.QValuePoint{})
	gob.Register(qvalue.QValueCIDR{})
	gob.Register(qvalue.QValueINET{})
	gob.Register(qvalue.QValueMacaddr{})
	gob.Register(qvalue.QValueArrayFloat32{})
	gob.Register(qvalue.QValueArrayFloat64{})
	gob.Register(qvalue.QValueArrayInt16{})
	gob.Register(qvalue.QValueArrayInt32{})
	gob.Register(qvalue.QValueArrayInt64{})
	gob.Register(qvalue.QValueArrayString{})
	gob.Register(qvalue.QValueArrayDate{})
	gob.Register(qvalue.QValueArrayTimestamp{})
	gob.Register(qvalue.QValueArrayTimestampTZ{})
	gob.Register(qvalue.QValueArrayBoolean{})
	gob.Register(qvalue.QValueTSTZRange{})
}

func (c *cdcStore[T]) initPebbleDB() error {
	if c.pebbleDB != nil {
		return nil
	}

	gob.Register(&model.InsertRecord[T]{})
	gob.Register(&model.UpdateRecord[T]{})
	gob.Register(&model.DeleteRecord[T]{})
	gob.Register(&model.RelationRecord[T]{})
	gob.Register(&model.MessageRecord[T]{})

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

func (c *cdcStore[T]) diskSpillThresholdsExceeded() bool {
	if len(c.inMemoryRecords) >= c.numRecordsSwitchThreshold {
		c.thresholdReason = fmt.Sprintf("more than %d primary keys read, spilling to disk",
			c.numRecordsSwitchThreshold)
		return true
	}
	if c.memThresholdBytes > 0 {
		metrics.Read(c.memStats)

		if c.memStats[0].Value.Uint64() >= c.memThresholdBytes {
			c.thresholdReason = fmt.Sprintf("memalloc greater than %d bytes, spilling to disk",
				c.memThresholdBytes)
			return true
		}
	}
	return false
}

func (c *cdcStore[T]) Set(logger log.Logger, key model.TableWithPkey, rec model.Record[T]) error {
	if key.TableName != "" {
		_, ok := c.inMemoryRecords[key]
		if ok || !c.diskSpillThresholdsExceeded() {
			c.inMemoryRecords[key] = rec
		} else {
			if c.pebbleDB == nil {
				logger.Info(c.thresholdReason,
					slog.String(string(shared.FlowNameKey), c.flowJobName))
				if err := c.initPebbleDB(); err != nil {
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
	}

	c.numRecords.Add(1)
	return nil
}

// bool is to indicate if a record is found or not [similar to ok]
func (c *cdcStore[T]) Get(key model.TableWithPkey) (model.Record[T], bool, error) {
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
		var rec model.Record[T]
		err = dec.Decode(&rec)
		if err != nil {
			return nil, false, fmt.Errorf("failed to decode record: %w", err)
		}

		return rec, true, nil
	}
	return nil, false, nil
}

func (c *cdcStore[T]) Len() int {
	return int(c.numRecords.Load())
}

func (c *cdcStore[T]) IsEmpty() bool {
	return c.Len() == 0
}

func (c *cdcStore[T]) Close() error {
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
