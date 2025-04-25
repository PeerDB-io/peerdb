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

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	_ "github.com/pingcap/tidb/pkg/types/parser_driver"
	"google.golang.org/protobuf/proto"

	"github.com/PeerDB-io/peerdb/flow/alerting"
	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/connectors/utils/monitoring"
	"github.com/PeerDB-io/peerdb/flow/datatypes"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/model/qvalue"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

func (c *MySqlConnector) GetTableSchema(
	ctx context.Context,
	env map[string]string,
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

	rs, err := c.Execute(ctx, `select column_name, column_type, column_key, is_nullable, numeric_precision, numeric_scale
	from information_schema.columns
	where table_schema = ? and table_name = ? order by ordinal_position`, schemaTable.Schema, schemaTable.Table)
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
		qkind, err := qkindFromMysqlColumnType(dataType)
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

func (c *MySqlConnector) ExportTxSnapshot(context.Context, map[string]string) (*protos.ExportTxSnapshotOutput, any, error) {
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
		lastOffsetText = fmt.Sprintf("!f:%s,%x", pos.Name, pos.Pos)
	}
	if err := c.SetLastOffset(
		ctx, req.FlowJobName, model.CdcCheckpoint{Text: lastOffsetText},
	); err != nil {
		return model.SetupReplicationResult{}, fmt.Errorf("[mysql] SetupReplication failed to SetLastOffset: %w", err)
	}

	return model.SetupReplicationResult{}, nil
}

func (c *MySqlConnector) SetupReplConn(ctx context.Context) error {
	// mysql code will spin up new connection for each normalize for now
	return nil
}

func (c *MySqlConnector) startSyncer(ctx context.Context) (*replication.BinlogSyncer, error) {
	var tlsConfig *tls.Config
	if !c.config.DisableTls {
		var err error
		tlsConfig, err = shared.CreateTlsConfig(tls.VersionTLS12, c.config.RootCa, c.config.Host, c.config.TlsHost)
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
	logger, ok := c.logger.(*slog.Logger)
	if !ok {
		logger = slog.Default()
	}
	//nolint:gosec
	return replication.NewBinlogSyncer(replication.BinlogSyncerConfig{
		ServerID:   rand.Uint32(),
		Flavor:     c.Flavor(),
		Host:       config.Host,
		Port:       uint16(config.Port),
		User:       config.User,
		Password:   config.Password,
		Logger:     logger,
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

func (c *MySqlConnector) HandleSlotInfo(
	ctx context.Context,
	alerter *alerting.Alerter,
	catalogPool shared.CatalogPool,
	alertKeys *alerting.AlertKeys,
	slotMetricGauges otel_metrics.SlotMetricGauges,
) error {
	return nil
}

func (c *MySqlConnector) GetSlotInfo(ctx context.Context, slotName string) ([]*protos.SlotInfo, error) {
	return nil, nil
}

func (c *MySqlConnector) AddTablesToPublication(ctx context.Context, req *protos.AddTablesToPublicationInput) error {
	return nil
}

func (c *MySqlConnector) RemoveTablesFromPublication(ctx context.Context, req *protos.RemoveTablesFromPublicationInput) error {
	return nil
}

func (c *MySqlConnector) PullRecords(
	ctx context.Context,
	catalogPool shared.CatalogPool,
	otelManager *otel_metrics.OtelManager,
	req *model.PullRecordsRequest[model.RecordItems],
) error {
	defer req.RecordStream.Close()

	syncer, mystream, gset, pos, err := c.startStreaming(ctx, req.LastOffset.Text)
	if err != nil {
		return err
	}
	defer syncer.Close()

	var skewLossReported bool
	var inTx bool
	var recordCount uint32
	defer func() {
		if recordCount == 0 {
			req.RecordStream.SignalAsEmpty()
		}
		c.logger.Info(fmt.Sprintf("[finished] PullRecords streamed %d records", recordCount))
	}()

	timeoutCtx := ctx
	var cancelTimeout context.CancelFunc
	defer func() {
		if cancelTimeout != nil {
			cancelTimeout()
		}
	}()

	addRecord := func(ctx context.Context, record model.Record[model.RecordItems]) error {
		recordCount += 1
		if err := req.RecordStream.AddRecord(ctx, record); err != nil {
			return err
		}
		if recordCount == 1 {
			req.RecordStream.SignalAsNotEmpty()
			timeoutCtx, cancelTimeout = context.WithTimeout(ctx, req.IdleTimeout)
		}
		return nil
	}

	var mysqlParser *parser.Parser
	for inTx || recordCount < req.MaxBatchSize {
		getCtx := ctx
		if !inTx {
			// don't gamble on closed timeoutCtx.Done() being prioritized over event backlog channel
			if err := timeoutCtx.Err(); err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					return nil
				}
				return err
			}
			getCtx = timeoutCtx
		}
		event, err := mystream.GetEvent(getCtx)
		if err != nil {
			if !inTx && errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			return err
		}

		otelManager.Metrics.FetchedBytesCounter.Add(ctx, int64(len(event.RawData)))

		switch ev := event.Event.(type) {
		case *replication.XIDEvent:
			if gset != nil {
				gset = ev.GSet
				req.RecordStream.UpdateLatestCheckpointText(gset.String())
			} else if event.Header.LogPos > pos.Pos {
				pos.Pos = event.Header.LogPos
				req.RecordStream.UpdateLatestCheckpointText(fmt.Sprintf("!f:%s,%x", pos.Name, pos.Pos))
			}
			inTx = false
		case *replication.RotateEvent:
			if gset == nil && (event.Header.Timestamp != 0 || string(ev.NextLogName) != pos.Name) {
				pos.Name = string(ev.NextLogName)
				pos.Pos = uint32(ev.Position)
				req.RecordStream.UpdateLatestCheckpointText(fmt.Sprintf("!f:%s,%x", pos.Name, pos.Pos))
				c.logger.Info("rotate", slog.String("name", pos.Name), slog.Uint64("pos", uint64(pos.Pos)))
			}
		case *replication.QueryEvent:
			if !inTx {
				if gset != nil {
					gset = ev.GSet
					req.RecordStream.UpdateLatestCheckpointText(gset.String())
				} else if event.Header.LogPos > pos.Pos {
					pos.Pos = event.Header.LogPos
					req.RecordStream.UpdateLatestCheckpointText(fmt.Sprintf("!f:%s,%x", pos.Name, pos.Pos))
				}
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
				inTx = true
				enumMap := ev.Table.EnumStrValueMap()
				setMap := ev.Table.SetStrValueMap()
				getFd := func(idx int) *protos.FieldDescription {
					if ev.Table.ColumnName != nil {
						unsafeName := shared.UnsafeFastReadOnlyBytesToString(ev.Table.ColumnName[idx])
						if _, excluded := exclusion[unsafeName]; !excluded {
							for _, col := range schema.Columns {
								if col.Name == unsafeName {
									return col
								}
							}
						}
						if !skewLossReported {
							skewLossReported = true
							c.logger.Warn("Unknown column name received, ignoring", slog.String("name", string(ev.Table.ColumnName[idx])))
						}
						return nil
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
							val, err := QValueFromMysqlRowEvent(ev.Table.ColumnType[idx], enumMap[idx], setMap[idx],
								qvalue.QValueKind(fd.Type), val)
							if err != nil {
								return err
							}
							items.AddColumn(fd.Name, val)
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
							val, err := QValueFromMysqlRowEvent(ev.Table.ColumnType[idx], enumMap[idx], setMap[idx],
								qvalue.QValueKind(fd.Type), val)
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
							val, err := QValueFromMysqlRowEvent(ev.Table.ColumnType[idx], enumMap[idx], setMap[idx],
								qvalue.QValueKind(fd.Type), val)
							if err != nil {
								return err
							}
							newItems.AddColumn(fd.Name, val)
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
							val, err := QValueFromMysqlRowEvent(ev.Table.ColumnType[idx], enumMap[idx], setMap[idx],
								qvalue.QValueKind(fd.Type), val)
							if err != nil {
								return err
							}
							items.AddColumn(fd.Name, val)
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
				qkind, err := qkindFromMysqlColumnType(col.Tp.InfoSchemaStr())
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
					Name:         col.Name.String(),
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
