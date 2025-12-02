package connmysql

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	_ "github.com/pingcap/tidb/pkg/types/parser_driver"
	"google.golang.org/protobuf/proto"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/connectors/utils/monitoring"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	mysql_validation "github.com/PeerDB-io/peerdb/flow/pkg/mysql"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/datatypes"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

func (c *MySqlConnector) GetTableSchema(
	ctx context.Context,
	env map[string]string,
	version uint32,
	system protos.TypeSystem,
	tableMappings []*protos.TableMapping,
) (map[string]*protos.TableSchema, error) {
	res := make(map[string]*protos.TableSchema, len(tableMappings))
	for _, tm := range tableMappings {
		tableSchema, err := c.getTableSchemaForTable(ctx, env, tm, system)
		if err != nil {
			c.logger.Info("error fetching schema", slog.String("table", tm.SourceTableIdentifier), slog.Any("error", err))
			return nil, err
		}
		res[tm.SourceTableIdentifier] = tableSchema
		c.logger.Info("fetched schema", slog.String("table", tm.SourceTableIdentifier))
	}

	return res, nil
}

func (c *MySqlConnector) getTableSchemaForTable(
	ctx context.Context,
	env map[string]string,
	tm *protos.TableMapping,
	system protos.TypeSystem,
) (*protos.TableSchema, error) {
	schemaTable, err := utils.ParseSchemaTable(tm.SourceTableIdentifier)
	if err != nil {
		return nil, err
	}

	nullableEnabled, err := internal.PeerDBNullable(ctx, env)
	if err != nil {
		return nil, err
	}

	rs, err := c.Execute(ctx, fmt.Sprintf(`select column_name, column_type, column_key, is_nullable, numeric_precision, numeric_scale
		from information_schema.columns where table_schema = '%s' and table_name = '%s' order by ordinal_position`,
		mysql.Escape(schemaTable.Schema), mysql.Escape(schemaTable.Table)))
	if err != nil {
		return nil, err
	}
	columns := make([]*protos.FieldDescription, 0, rs.RowNumber())
	primary := make([]string, 0)

	for idx := range rs.RowNumber() {
		columnName, err := rs.GetString(idx, 0)
		if err != nil {
			return nil, err
		}
		if slices.Contains(tm.Exclude, columnName) {
			continue
		}

		dataType, err := rs.GetString(idx, 1)
		if err != nil {
			return nil, err
		}
		columnKey, err := rs.GetString(idx, 2)
		if err != nil {
			return nil, err
		}
		isNullable, err := rs.GetString(idx, 3)
		if err != nil {
			return nil, err
		}
		numericPrecision, err := rs.GetInt(idx, 4)
		if err != nil {
			return nil, err
		}
		numericScale, err := rs.GetInt(idx, 5)
		if err != nil {
			return nil, err
		}
		qkind, err := QkindFromMysqlColumnType(dataType)
		if err != nil {
			return nil, err
		}

		column := &protos.FieldDescription{
			Name:         columnName,
			Type:         string(qkind),
			TypeModifier: datatypes.MakeNumericTypmod(int32(numericPrecision), int32(numericScale)),
			Nullable:     isNullable == "YES",
		}
		if columnKey == "PRI" {
			primary = append(primary, columnName)
		}
		columns = append(columns, column)
	}

	return &protos.TableSchema{
		TableIdentifier:       tm.SourceTableIdentifier,
		PrimaryKeyColumns:     primary,
		IsReplicaIdentityFull: false,
		System:                system,
		NullableEnabled:       nullableEnabled,
		Columns:               columns,
	}, nil
}

func (c *MySqlConnector) EnsurePullability(
	ctx context.Context, req *protos.EnsurePullabilityBatchInput,
) (*protos.EnsurePullabilityBatchOutput, error) {
	return nil, nil
}

func (c *MySqlConnector) ExportTxSnapshot(context.Context, string, map[string]string) (*protos.ExportTxSnapshotOutput, any, error) {
	// https://dev.mysql.com/doc/refman/8.4/en/replication-howto-masterstatus.html
	return nil, nil, nil
}

func (c *MySqlConnector) FinishExport(any) error {
	return nil
}

func (c *MySqlConnector) SetupReplication(
	ctx context.Context,
	req *protos.SetupReplicationInput,
) (model.SetupReplicationResult, error) {
	var gtidModeOn bool
	if c.config.ReplicationMechanism == protos.MySqlReplicationMechanism_MYSQL_AUTO {
		var err error
		gtidModeOn, err = c.GetGtidModeOn(ctx)
		if err != nil {
			return model.SetupReplicationResult{}, fmt.Errorf("[mysql] SetupReplication failed to get gtid_mode: %w", err)
		}
	} else {
		gtidModeOn = c.config.ReplicationMechanism == protos.MySqlReplicationMechanism_MYSQL_GTID
	}
	var lastOffsetText string
	if gtidModeOn {
		set, err := c.GetMasterGTIDSet(ctx)
		if err != nil {
			return model.SetupReplicationResult{}, fmt.Errorf("[mysql] SetupReplication failed to GetMasterGTIDSet: %w", err)
		}
		lastOffsetText = set.String()
	} else {
		pos, err := c.GetMasterPos(ctx)
		if err != nil {
			return model.SetupReplicationResult{}, fmt.Errorf("[mysql] SetupReplication failed to GetMasterPos: %w", err)
		}
		lastOffsetText = posToOffsetText(pos)
	}
	if err := c.SetLastOffset(
		ctx, req.FlowJobName, model.CdcCheckpoint{Text: lastOffsetText},
	); err != nil {
		return model.SetupReplicationResult{}, fmt.Errorf("[mysql] SetupReplication failed to SetLastOffset: %w", err)
	}

	return model.SetupReplicationResult{}, nil
}

func (c *MySqlConnector) SetupReplConn(context.Context, map[string]string) error {
	// mysql code will spin up new connection for each normalize for now
	return nil
}

func (c *MySqlConnector) startSyncer(ctx context.Context) (*replication.BinlogSyncer, error) {
	var tlsConfig *tls.Config
	if !c.config.DisableTls {
		var err error
		tlsConfig, err = shared.CreateTlsConfig(
			tls.VersionTLS12, c.config.RootCa, c.config.Host, c.config.TlsHost, c.config.SkipCertVerification,
		)
		if err != nil {
			return nil, err
		}
	}
	config := c.config
	if c.rdsAuth != nil {
		c.logger.Info("Setting up IAM auth for MySQL replication")
		host := c.config.Host
		if c.config.TlsHost != "" {
			host = c.config.TlsHost
		}
		token, err := utils.GetRDSToken(ctx, utils.RDSConnectionConfig{
			Host: host,
			Port: config.Port,
			User: config.User,
		}, c.rdsAuth, "MYSQL")
		if err != nil {
			return nil, err
		}
		config = proto.CloneOf(config)
		config.Password = token
	}

	//nolint:gosec
	return replication.NewBinlogSyncer(replication.BinlogSyncerConfig{
		ServerID:   rand.Uint32(),
		Flavor:     c.Flavor(),
		Host:       config.Host,
		Port:       uint16(config.Port),
		User:       config.User,
		Password:   config.Password,
		Logger:     internal.SlogLoggerFromCtx(ctx),
		Dialer:     c.Dialer(),
		UseDecimal: true,
		ParseTime:  true,
		TLSConfig:  tlsConfig,
	}), nil
}

func (c *MySqlConnector) startStreaming(
	ctx context.Context,
	pos string,
) (*replication.BinlogSyncer, *replication.BinlogStreamer, mysql.GTIDSet, mysql.Position, error) {
	if rest, isFile := strings.CutPrefix(pos, "!f:"); isFile {
		comma := strings.LastIndexByte(rest, ',')
		if comma == -1 {
			return nil, nil, nil, mysql.Position{}, fmt.Errorf("no comma in file/pos offset %s", pos)
		}
		offset, err := strconv.ParseUint(rest[comma+1:], 16, 32)
		if err != nil {
			return nil, nil, nil, mysql.Position{}, fmt.Errorf("invalid offset in file/pos offset %s: %w", pos, err)
		}
		return c.startCdcStreamingFilePos(ctx, mysql.Position{Name: rest[:comma], Pos: uint32(offset)})
	} else {
		gset, err := mysql.ParseGTIDSet(c.Flavor(), pos)
		if err != nil {
			return nil, nil, nil, mysql.Position{}, err
		}
		return c.startCdcStreamingGtid(ctx, gset)
	}
}

func (c *MySqlConnector) startCdcStreamingFilePos(
	ctx context.Context,
	pos mysql.Position,
) (*replication.BinlogSyncer, *replication.BinlogStreamer, mysql.GTIDSet, mysql.Position, error) {
	syncer, err := c.startSyncer(ctx)
	if err != nil {
		return nil, nil, nil, mysql.Position{}, err
	}
	stream, err := syncer.StartSync(pos)
	if err != nil {
		syncer.Close()
	}
	return syncer, stream, nil, pos, err
}

func (c *MySqlConnector) startCdcStreamingGtid(
	ctx context.Context,
	gset mysql.GTIDSet,
) (*replication.BinlogSyncer, *replication.BinlogStreamer, mysql.GTIDSet, mysql.Position, error) {
	syncer, err := c.startSyncer(ctx)
	if err != nil {
		return nil, nil, nil, mysql.Position{}, err
	}
	stream, err := syncer.StartSyncGTID(gset)
	if err != nil {
		syncer.Close()
	}
	return syncer, stream, gset, mysql.Position{}, err
}

func (c *MySqlConnector) ReplPing(context.Context) error {
	return nil
}

func (c *MySqlConnector) UpdateReplStateLastOffset(ctx context.Context, lastOffset model.CdcCheckpoint) error {
	flowName := ctx.Value(shared.FlowNameKey).(string)
	return c.SetLastOffset(ctx, flowName, lastOffset)
}

func (c *MySqlConnector) PullFlowCleanup(ctx context.Context, jobName string) error {
	return nil
}

func (c *MySqlConnector) PullRecords(
	ctx context.Context,
	catalogPool shared.CatalogPool,
	otelManager *otel_metrics.OtelManager,
	req *model.PullRecordsRequest[model.RecordItems],
) error {
	defer req.RecordStream.Close()

	sourceSchemaAsDestinationColumn, err := internal.PeerDBSourceSchemaAsDestinationColumn(ctx, req.Env)
	if err != nil {
		return err
	}

	versionToCmp := mysql_validation.MySQLMinVersionForBinlogRowMetadata
	if c.config.Flavor == protos.MySqlFlavor_MYSQL_MARIA {
		versionToCmp = mysql_validation.MariaDBMinVersionForBinlogRowMetadata
	}
	cmp, err := c.CompareServerVersion(ctx, versionToCmp)
	if err != nil {
		return fmt.Errorf("failed to get server version: %w", err)
	}
	binlogRowMetadataSupported := cmp >= 0

	syncer, mystream, gset, pos, err := c.startStreaming(ctx, req.LastOffset.Text)
	if err != nil {
		return err
	}
	defer syncer.Close()
	c.logger.Info("[mysql] PullRecords started streaming")

	var skewLossReported bool
	var coercionReported bool
	var updatedOffset string
	var inTx bool
	var recordCount uint32

	// set when a tx is preventing us from respecting the timeout, immediately exit after we see inTx false
	var overtime bool
	var fetchedBytes, totalFetchedBytes, allFetchedBytes atomic.Int64
	pullStart := time.Now()
	defer func() {
		if recordCount == 0 {
			req.RecordStream.SignalAsEmpty()
		}
		c.logger.Info("[mysql] PullRecords finished streaming",
			slog.Uint64("records", uint64(recordCount)),
			slog.Int64("bytes", totalFetchedBytes.Load()),
			slog.Int("channelLen", req.RecordStream.ChannelLen()),
			slog.Float64("elapsedMinutes", time.Since(pullStart).Minutes()))
	}()

	defer func() {
		otelManager.Metrics.FetchedBytesCounter.Add(ctx, fetchedBytes.Swap(0))
		otelManager.Metrics.AllFetchedBytesCounter.Add(ctx, allFetchedBytes.Swap(0))
	}()
	shutdown := shared.Interval(ctx, time.Minute, func() {
		otelManager.Metrics.FetchedBytesCounter.Add(ctx, fetchedBytes.Swap(0))
		otelManager.Metrics.AllFetchedBytesCounter.Add(ctx, allFetchedBytes.Swap(0))
		c.logger.Info("[mysql] pulling records",
			slog.Uint64("records", uint64(recordCount)),
			slog.Int64("bytes", totalFetchedBytes.Load()),
			slog.Int("channelLen", req.RecordStream.ChannelLen()),
			slog.Float64("elapsedMinutes", time.Since(pullStart).Minutes()))
	})
	defer shutdown()

	timeoutCtx, cancelTimeout := context.WithTimeout(ctx, time.Hour)
	//nolint:gocritic // cancelTimeout is rebound, do not defer cancelTimeout()
	defer func() {
		cancelTimeout()
	}()

	addRecord := func(ctx context.Context, record model.Record[model.RecordItems]) error {
		recordCount += 1
		if err := req.RecordStream.AddRecord(ctx, record); err != nil {
			return err
		}
		if recordCount == 1 {
			req.RecordStream.SignalAsNotEmpty()
			cancelTimeout()
			timeoutCtx, cancelTimeout = context.WithTimeout(ctx, req.IdleTimeout)
		}
		if recordCount%50000 == 0 {
			c.logger.Info("[mysql] PullRecords streaming",
				slog.Uint64("records", uint64(recordCount)),
				slog.Int64("bytes", totalFetchedBytes.Load()),
				slog.Int("channelLen", req.RecordStream.ChannelLen()),
				slog.Float64("elapsedMinutes", time.Since(pullStart).Minutes()),
				slog.Bool("inTx", inTx),
				slog.Bool("overtime", overtime))
		}
		return nil
	}

	var mysqlParser *parser.Parser
	for inTx || (!overtime && recordCount < req.MaxBatchSize) {
		var event *replication.BinlogEvent
		// don't gamble on closed timeoutCtx.Done() being prioritized over event backlog channel
		err := timeoutCtx.Err()
		if err == nil {
			event, err = mystream.GetEvent(timeoutCtx)
		}
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				c.logger.Info("[mysql] PullRecords context canceled, stopping streaming", slog.Any("error", err))
				//nolint:govet // cancelTimeout called by defer, spurious lint
				return ctxErr
			} else if errors.Is(err, context.DeadlineExceeded) {
				if recordCount == 0 {
					// progress offset while no records read to avoid falling behind when all tables inactive
					if updatedOffset != "" {
						c.logger.Info("[mysql] updating inactive offset", slog.Any("offset", updatedOffset))
						if err := c.SetLastOffset(ctx, req.FlowJobName, model.CdcCheckpoint{Text: updatedOffset}); err != nil {
							c.logger.Error("[mysql] failed to update offset, ignoring", slog.Any("error", err))
						} else {
							updatedOffset = ""
						}
					}

					// reset timer for next offset update
					cancelTimeout()
					timeoutCtx, cancelTimeout = context.WithTimeout(ctx, time.Hour)
				} else if inTx {
					c.logger.Info("[mysql] timeout reached, but still in transaction, waiting for inTx false",
						slog.Uint64("records", uint64(recordCount)),
						slog.Int64("bytes", totalFetchedBytes.Load()),
						slog.Int("channelLen", req.RecordStream.ChannelLen()),
						slog.Float64("elapsedMinutes", time.Since(pullStart).Minutes()))
					// reset timeoutCtx to a low value and wait for inTx to become false
					cancelTimeout()
					//nolint:govet // cancelTimeout called by defer, spurious lint
					timeoutCtx, cancelTimeout = context.WithTimeout(ctx, time.Minute)
					overtime = true
				} else {
					return nil
				}

				continue
			} else {
				c.logger.Error("[mysql] PullRecords failed to get event", slog.Any("error", err))
			}
			return err
		}

		allFetchedBytes.Add(int64(len(event.RawData)))

		switch ev := event.Event.(type) {
		case *replication.GTIDEvent:
			if ev.ImmediateCommitTimestamp > 0 {
				otelManager.Metrics.CommitLagGauge.Record(ctx,
					time.Now().UTC().Sub(time.UnixMicro(int64(ev.ImmediateCommitTimestamp))).Microseconds())
			}
		case *replication.XIDEvent:
			if gset != nil {
				gset = ev.GSet
				updatedOffset = gset.String()
				req.RecordStream.UpdateLatestCheckpointText(updatedOffset)
			} else if event.Header.LogPos > pos.Pos {
				pos.Pos = event.Header.LogPos
				updatedOffset = posToOffsetText(pos)
				req.RecordStream.UpdateLatestCheckpointText(updatedOffset)
			}
			inTx = false
		case *replication.RotateEvent:
			if gset == nil && (event.Header.Timestamp != 0 || string(ev.NextLogName) != pos.Name) {
				pos.Name = string(ev.NextLogName)
				pos.Pos = uint32(ev.Position)
				updatedOffset = posToOffsetText(pos)
				req.RecordStream.UpdateLatestCheckpointText(updatedOffset)
				c.logger.Info("rotate", slog.String("name", pos.Name), slog.Uint64("pos", uint64(pos.Pos)))
			}
		case *replication.QueryEvent:
			if !inTx && gset == nil && event.Header.LogPos > pos.Pos {
				pos.Pos = event.Header.LogPos
				updatedOffset = posToOffsetText(pos)
				req.RecordStream.UpdateLatestCheckpointText(updatedOffset)
			}
			if mysqlParser == nil {
				mysqlParser = parser.New()
			}
			stmts, warns, err := mysqlParser.ParseSQL(shared.UnsafeFastReadOnlyBytesToString(ev.Query))
			if err != nil {
				c.logger.Warn("failed to parse QueryEvent", slog.String("query", string(ev.Query)), slog.Any("error", err))
				break
			}
			if len(warns) > 0 {
				c.logger.Warn("processing QueryEvent with logged warnings", slog.Any("warns", warns))
			}
			for _, stmt := range stmts {
				if alterTableStmt, ok := stmt.(*ast.AlterTableStmt); ok {
					if err := c.processAlterTableQuery(ctx, catalogPool, req, alterTableStmt, string(ev.Schema)); err != nil {
						return fmt.Errorf("failed to process ALTER TABLE query: %w", err)
					}
				}
			}
		case *replication.RowsEvent:
			sourceTableName := string(ev.Table.Schema) + "." + string(ev.Table.Table) // TODO this is fragile
			destinationTableName := req.TableNameMapping[sourceTableName].Name
			exclusion := req.TableNameMapping[sourceTableName].Exclude
			schema := req.TableNameSchemaMapping[destinationTableName]
			if schema != nil {
				// The issue is global, but only error if we see a table in the pipe
				// Otherwise users could be confused
				if binlogRowMetadataSupported && ev.Table.ColumnName == nil {
					e := exceptions.NewMySQLUnsupportedBinlogRowMetadataError(string(ev.Table.Schema), string(ev.Table.Table))
					c.logger.Error(e.Error())
					return e
				}
				otelManager.Metrics.FetchedBytesCounter.Add(ctx, int64(len(event.RawData)))
				fetchedBytes.Add(int64(len(event.RawData)))
				totalFetchedBytes.Add(int64(len(event.RawData)))
				inTx = true
				enumMap := ev.Table.EnumStrValueMap()
				setMap := ev.Table.SetStrValueMap()

				// Process TABLE_MAP_EVENT schema to detect new columns
				// and build efficient column index mapping
				var colIndexToFd map[int]*protos.FieldDescription
				if ev.Table.ColumnName != nil {
					var err error
					colIndexToFd, err = c.processTableMapEventSchema(
						ctx, catalogPool, req, ev.Table,
						sourceTableName, destinationTableName, schema, exclusion,
					)
					if err != nil {
						return err
					}
				}

				getFd := func(idx int) *protos.FieldDescription {
					if colIndexToFd != nil {
						return colIndexToFd[idx]
					}
					if idx < len(schema.Columns) {
						return schema.Columns[idx]
					}
					if !skewLossReported {
						skewLossReported = true
						c.logger.Warn("Column ordinal position out of range, ignoring", slog.Int("position", idx))
					}
					return nil
				}
				switch event.Header.EventType {
				case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2, replication.MARIADB_WRITE_ROWS_COMPRESSED_EVENT_V1:
					for _, row := range ev.Rows {
						items := model.NewRecordItems(len(row))
						for idx, val := range row {
							fd := getFd(idx)
							if fd == nil {
								continue
							}
							val, err := QValueFromMysqlRowEvent(ev.Table, idx, enumMap[idx], setMap[idx],
								types.QValueKind(fd.Type), val, c.logger, &coercionReported)
							if err != nil {
								return err
							}
							items.AddColumn(fd.Name, val)
						}
						if sourceSchemaAsDestinationColumn {
							items.AddColumn("_peerdb_source_schema", types.QValueString{Val: string(ev.Table.Schema)})
						}

						if err := addRecord(ctx, &model.InsertRecord[model.RecordItems]{
							BaseRecord:           model.BaseRecord{CommitTimeNano: int64(event.Header.Timestamp) * 1e9},
							Items:                items,
							SourceTableName:      sourceTableName,
							DestinationTableName: destinationTableName,
						}); err != nil {
							return err
						}
					}
				case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2, replication.MARIADB_UPDATE_ROWS_COMPRESSED_EVENT_V1:
					for idx := 0; idx < len(ev.Rows); idx += 2 {
						var unchangedToastColumns map[string]struct{}
						if len(ev.SkippedColumns) > idx+1 {
							unchangedToastColumns = make(map[string]struct{}, len(ev.SkippedColumns[idx+1]))
							for _, skipped := range ev.SkippedColumns[idx+1] {
								unchangedToastColumns[schema.Columns[skipped].Name] = struct{}{}
							}
						}

						oldRow := ev.Rows[idx]
						oldItems := model.NewRecordItems(len(oldRow))
						for idx, val := range oldRow {
							fd := getFd(idx)
							if fd == nil {
								continue
							}
							val, err := QValueFromMysqlRowEvent(ev.Table, idx, enumMap[idx], setMap[idx],
								types.QValueKind(fd.Type), val, c.logger, &coercionReported)
							if err != nil {
								return err
							}
							oldItems.AddColumn(fd.Name, val)
						}
						newRow := ev.Rows[idx+1]
						newItems := model.NewRecordItems(len(newRow))
						for idx, val := range ev.Rows[idx+1] {
							fd := getFd(idx)
							if fd == nil {
								continue
							}
							val, err := QValueFromMysqlRowEvent(ev.Table, idx, enumMap[idx], setMap[idx],
								types.QValueKind(fd.Type), val, c.logger, &coercionReported)
							if err != nil {
								return err
							}
							newItems.AddColumn(fd.Name, val)
						}
						if sourceSchemaAsDestinationColumn {
							newItems.AddColumn("_peerdb_source_schema", types.QValueString{Val: string(ev.Table.Schema)})
						}

						if err := addRecord(ctx, &model.UpdateRecord[model.RecordItems]{
							BaseRecord:            model.BaseRecord{CommitTimeNano: int64(event.Header.Timestamp) * 1e9},
							OldItems:              oldItems,
							NewItems:              newItems,
							SourceTableName:       sourceTableName,
							DestinationTableName:  destinationTableName,
							UnchangedToastColumns: unchangedToastColumns,
						}); err != nil {
							return err
						}
					}
				case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2, replication.MARIADB_DELETE_ROWS_COMPRESSED_EVENT_V1:
					for idx, row := range ev.Rows {
						var unchangedToastColumns map[string]struct{}
						if len(ev.SkippedColumns) > idx {
							unchangedToastColumns = make(map[string]struct{}, len(ev.SkippedColumns[idx]))
							for _, skipped := range ev.SkippedColumns[idx] {
								unchangedToastColumns[schema.Columns[skipped].Name] = struct{}{}
							}
						}

						items := model.NewRecordItems(len(row))
						for idx, val := range row {
							fd := getFd(idx)
							if fd == nil {
								continue
							}
							val, err := QValueFromMysqlRowEvent(ev.Table, idx, enumMap[idx], setMap[idx],
								types.QValueKind(fd.Type), val, c.logger, &coercionReported)
							if err != nil {
								return err
							}
							items.AddColumn(fd.Name, val)
						}
						if sourceSchemaAsDestinationColumn {
							items.AddColumn("_peerdb_source_schema", types.QValueString{Val: string(ev.Table.Schema)})
						}

						if err := addRecord(ctx, &model.DeleteRecord[model.RecordItems]{
							BaseRecord:            model.BaseRecord{CommitTimeNano: int64(event.Header.Timestamp) * 1e9},
							Items:                 items,
							SourceTableName:       sourceTableName,
							DestinationTableName:  destinationTableName,
							UnchangedToastColumns: unchangedToastColumns,
						}); err != nil {
							return err
						}
					}
				case replication.WRITE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv0:
					return errors.New("mysql v0 replication protocol not supported")
				}
			}
			if event.Header.Timestamp > 0 {
				otelManager.Metrics.LatestConsumedLogEventGauge.Record(
					ctx,
					int64(event.Header.Timestamp),
				)
			}
		}
	}
	return nil
}

func (c *MySqlConnector) processAlterTableQuery(ctx context.Context, catalogPool shared.CatalogPool,
	req *model.PullRecordsRequest[model.RecordItems], stmt *ast.AlterTableStmt, stmtSchema string,
) error {
	// if ALTER TABLE doesn't have database/schema name, use one attached to event
	var sourceSchemaName string
	if stmt.Table.Schema.String() != "" {
		sourceSchemaName = stmt.Table.Schema.String()
	} else {
		sourceSchemaName = stmtSchema
	}
	sourceTableName := sourceSchemaName + "." + stmt.Table.Name.String()

	destinationTableName := req.TableNameMapping[sourceTableName].Name
	if destinationTableName == "" {
		c.logger.Warn("table not found in mapping", slog.String("table", sourceTableName))
		return nil
	}
	currentSchema := req.TableNameSchemaMapping[destinationTableName]

	tableSchemaDelta := &protos.TableSchemaDelta{
		SrcTableName:    sourceTableName,
		DstTableName:    destinationTableName,
		AddedColumns:    nil,
		System:          protos.TypeSystem_Q,
		NullableEnabled: currentSchema != nil && currentSchema.NullableEnabled,
	}

	for _, spec := range stmt.Specs {
		if spec.NewColumns != nil {
			// these are added columns
			for _, col := range spec.NewColumns {
				if col.Tp == nil {
					// ignore, can be plain ALTER TABLE ... ALTER COLUMN ... DEFAULT ...
					c.logger.Warn("ALTER TABLE with no column type detected, ignoring",
						slog.String("columnName", col.Name.String()),
						slog.String("tableName", sourceTableName))
					continue
				}
				qkind, err := QkindFromMysqlColumnType(col.Tp.InfoSchemaStr())
				if err != nil {
					return err
				}

				nullable := true
				for _, option := range col.Options {
					if option.Tp == ast.ColumnOptionNotNull {
						nullable = false
					}
				}

				precision := col.Tp.GetFlen()
				scale := col.Tp.GetDecimal()
				typmod := int32(-1)
				if scale >= 0 || precision >= 0 {
					typmod = datatypes.MakeNumericTypmod(int32(precision), int32(scale))
				}

				fd := &protos.FieldDescription{
					Name:         col.Name.OrigColName(),
					Type:         string(qkind),
					TypeModifier: typmod,
					Nullable:     nullable,
				}
				tableSchemaDelta.AddedColumns = append(tableSchemaDelta.AddedColumns, fd)
				// current assumption is the columns will be ordered like this
				currentSchema.Columns = append(currentSchema.Columns, fd)
			}
		} else if spec.OldColumnName != nil {
			// this could be dropped or renamed column
			if spec.NewColumnName != nil {
				c.logger.Warn("renamed column detected but not propagating",
					slog.String("columnOldName", spec.OldColumnName.String()), slog.String("columnNewName", spec.NewColumnName.String()))
			} else {
				c.logger.Warn("dropped column detected but not propagating", slog.String("columnName", spec.OldColumnName.String()))
			}
		}
	}
	if tableSchemaDelta.AddedColumns != nil {
		c.logger.Info("Column added detected",
			slog.String("table", destinationTableName), slog.Any("columns", tableSchemaDelta.AddedColumns))
		req.RecordStream.AddSchemaDelta(req.TableNameMapping, tableSchemaDelta)
		return monitoring.AuditSchemaDelta(ctx, catalogPool.Pool, req.FlowJobName, tableSchemaDelta)
	}
	return nil
}

func posToOffsetText(pos mysql.Position) string {
	return fmt.Sprintf("!f:%s,%x", pos.Name, pos.Pos)
}

// processTableMapEventSchema compares the TABLE_MAP_EVENT schema against the cached schema
// and returns a TableSchemaDelta if new columns are detected (e.g., after gh-ost migration).
// It also returns a map from binlog column index to FieldDescription for efficient row processing.
func (c *MySqlConnector) processTableMapEventSchema(
	ctx context.Context,
	catalogPool shared.CatalogPool,
	req *model.PullRecordsRequest[model.RecordItems],
	tableMap *replication.TableMapEvent,
	sourceTableName string,
	destinationTableName string,
	schema *protos.TableSchema,
	exclusion map[string]struct{},
) (colIndexToFd map[int]*protos.FieldDescription, err error) {
	colIndexToFd = make(map[int]*protos.FieldDescription, len(tableMap.ColumnName))

	// Build a set of existing column names for quick lookup
	existingCols := make(map[string]*protos.FieldDescription, len(schema.Columns))
	for _, col := range schema.Columns {
		existingCols[col.Name] = col
	}

	// Get metadata maps for type conversion
	unsignedMap := tableMap.UnsignedMap()
	collationMap := tableMap.CollationMap()

	var addedColumns []*protos.FieldDescription

	for idx, colNameBytes := range tableMap.ColumnName {
		colName := shared.UnsafeFastReadOnlyBytesToString(colNameBytes)
		if _, excluded := exclusion[colName]; excluded {
			continue
		}

		if fd, exists := existingCols[colName]; exists {
			colIndexToFd[idx] = fd
		} else {
			// New column detected - get type from TABLE_MAP_EVENT
			var charset uint16
			if collation, ok := collationMap[idx]; ok {
				charset = uint16(collation)
			}
			mytype := tableMap.ColumnType[idx]
			qkind, err := qkindFromMysqlType(mytype, unsignedMap[idx], charset)
			if err != nil {
				c.logger.Warn("Unknown MySQL type for new column, skipping",
					slog.String("table", sourceTableName),
					slog.String("column", colName),
					slog.Any("error", err))
				continue
			}

			// Get nullable info
			_, nullable := tableMap.Nullable(idx)

			// Extract precision/scale for DECIMAL types from ColumnMeta
			// ColumnMeta stores: high byte = precision, low byte = scale
			typmod := int32(-1)
			if (mytype == mysql.MYSQL_TYPE_DECIMAL || mytype == mysql.MYSQL_TYPE_NEWDECIMAL) &&
				idx < len(tableMap.ColumnMeta) {
				meta := tableMap.ColumnMeta[idx]
				precision := int32(meta >> 8)
				scale := int32(meta & 0xFF)
				typmod = datatypes.MakeNumericTypmod(precision, scale)
			}

			newFd := &protos.FieldDescription{
				Name:         colName,
				Type:         string(qkind),
				TypeModifier: typmod,
				Nullable:     nullable,
			}

			addedColumns = append(addedColumns, newFd)
			colIndexToFd[idx] = newFd

			c.logger.Info("Detected new column from TABLE_MAP_EVENT",
				slog.String("table", sourceTableName),
				slog.String("column", colName),
				slog.String("type", string(qkind)))
		}
	}

	// If new columns were detected, emit schema delta and update cached schema
	if len(addedColumns) > 0 {
		tableSchemaDelta := &protos.TableSchemaDelta{
			SrcTableName:    sourceTableName,
			DstTableName:    destinationTableName,
			AddedColumns:    addedColumns,
			System:          protos.TypeSystem_Q,
			NullableEnabled: schema.NullableEnabled,
		}

		c.logger.Info("Schema change detected from TABLE_MAP_EVENT",
			slog.String("table", destinationTableName),
			slog.Any("addedColumns", addedColumns))

		// Update cached schema
		schema.Columns = append(schema.Columns, addedColumns...)

		// Emit schema delta
		req.RecordStream.AddSchemaDelta(req.TableNameMapping, tableSchemaDelta)
		if err := monitoring.AuditSchemaDelta(ctx, catalogPool.Pool, req.FlowJobName, tableSchemaDelta); err != nil {
			return nil, err
		}
	}

	return colIndexToFd, nil
}
