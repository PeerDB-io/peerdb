package connmysql

import (
	"bytes"
	"cmp"
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"slices"
	"sync/atomic"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	tidbmysql "github.com/pingcap/tidb/pkg/parser/mysql"
	_ "github.com/pingcap/tidb/pkg/types/parser_driver"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/text/encoding"
	"google.golang.org/protobuf/proto"

	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/connectors/utils/monitoring"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/datatypes"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

const (
	defaultBinlogHeartbeatPeriod = time.Minute
)

const (
	queryStatusVarFlags2  = 0
	queryStatusVarSQLMode = 1
)

const (
	OnlineSchemaMigrationToolGhOst = "gh-ost"
	OnlineSchemaMigrationToolPtOsc = "pt-online-schema-change"
	OnlineSchemaMigrationToolOther = "other"
)

// sqlModeFromStatusVars extracts the sql_mode bitmask from a QueryEvent's status vars.
// Both MySQL and MariaDB guarantee status vars are written in increasing order,
// so we only need to parse 0 and 1 to get to 1
// https://dev.mysql.com/doc/dev/mysql-server/latest/classmysql_1_1binlog_1_1event_1_1Query__event.html#Query_event_binary_format
// https://github.com/MariaDB/server/blob/c3ec2dc368a8c7165cdbea58208eb828e76ebc57/sql/log_event_server.cc#L1083-L1087
func sqlModeFromStatusVars(statusVars []byte) (uint64, bool) {
	for pos := 0; pos < len(statusVars); {
		code := statusVars[pos]
		pos++

		switch code {
		case queryStatusVarFlags2:
			pos += 4
		case queryStatusVarSQLMode:
			if pos+8 > len(statusVars) {
				return 0, false
			}
			return binary.LittleEndian.Uint64(statusVars[pos : pos+8]), true
		default:
			return 0, false
		}

		if pos > len(statusVars) {
			return 0, false
		}
	}
	return 0, false
}

func setParserSQLModeFromStatusVars(mysqlParser *parser.Parser, statusVars []byte) {
	var sqlMode tidbmysql.SQLMode
	if mode, ok := sqlModeFromStatusVars(statusVars); ok && mode&uint64(tidbmysql.ModeANSIQuotes) != 0 {
		sqlMode = tidbmysql.ModeANSIQuotes
	}
	mysqlParser.SetSQLMode(sqlMode)
}

func parseSQL(parser *parser.Parser, query []byte) ([]ast.StmtNode, []error, error) {
	// TIDB parser errors on null-terminated strings
	trimmedQuery := shared.UnsafeFastReadOnlyBytesToString(bytes.TrimRight(query, "\x00"))
	return parser.ParseSQL(trimmedQuery)
}

func (c *MySqlConnector) GetTableSchema(
	ctx context.Context,
	env map[string]string,
	version uint32,
	system protos.TypeSystem,
	tableMappings []*protos.TableMapping,
) (map[string]*protos.TableSchema, error) {
	res := make(map[string]*protos.TableSchema, len(tableMappings))
	for _, tm := range tableMappings {
		tableSchema, err := c.getTableSchemaForTable(ctx, env, tm, system, version)
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
	mirrorVersion uint32,
) (*protos.TableSchema, error) {
	qualifiedTable, err := common.ParseTableIdentifier(tm.SourceTableIdentifier)
	if err != nil {
		return nil, err
	}

	nullableEnabled, err := internal.PeerDBNullable(ctx, env)
	if err != nil {
		return nil, err
	}

	// CAST(... AS BINARY) forces a case-sensitive, collation-independent comparison on the join keys so it works with
	// lower_case_table_names=0. The LEFT JOIN leaves seq_in_index NULL for non-primary-key columns.
	rs, err := c.Execute(ctx, fmt.Sprintf(`
		select c.column_name, c.column_type, c.is_nullable, c.numeric_precision, c.numeric_scale, s.seq_in_index
		from information_schema.columns c
		left join information_schema.statistics s
			on cast(s.table_schema as binary) = cast(c.table_schema as binary)
			and cast(s.table_name as binary) = cast(c.table_name as binary)
			and cast(s.column_name as binary) = cast(c.column_name as binary)
			and s.index_name = 'PRIMARY'
		where c.table_schema = '%s' and c.table_name = '%s'
		order by c.ordinal_position`,
		mysql.Escape(qualifiedTable.Namespace), mysql.Escape(qualifiedTable.Table)))
	if err != nil {
		return nil, err
	}
	columns := make([]*protos.FieldDescription, 0, rs.RowNumber())
	type pkEntry struct {
		name       string
		seqInIndex int64
	}

	binlogRowMetadataSupported, err := c.IsBinlogRowMetadataSupported(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to determine if binlog row metadata is supported: %w", err)
	}

	var primaryEntries []pkEntry
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
		isNullable, err := rs.GetString(idx, 2)
		if err != nil {
			return nil, err
		}
		numericPrecision, err := rs.GetInt(idx, 3)
		if err != nil {
			return nil, err
		}
		numericScale, err := rs.GetInt(idx, 4)
		if err != nil {
			return nil, err
		}
		qkind, err := QkindFromMysqlColumnType(dataType, binlogRowMetadataSupported, mirrorVersion)
		if err != nil {
			return nil, err
		}

		column := &protos.FieldDescription{
			Name:         columnName,
			Type:         string(qkind),
			TypeModifier: datatypes.MakeNumericTypmod(int32(numericPrecision), int32(numericScale)),
			Nullable:     isNullable == "YES",
		}
		columns = append(columns, column)

		seqIsNull, err := rs.IsNull(idx, 5)
		if err != nil {
			return nil, err
		}
		if !seqIsNull {
			seq, err := rs.GetInt(idx, 5)
			if err != nil {
				return nil, err
			}
			primaryEntries = append(primaryEntries, pkEntry{name: columnName, seqInIndex: seq})
		}
	}

	slices.SortFunc(primaryEntries, func(a, b pkEntry) int {
		return cmp.Compare(a.seqInIndex, b.seqInIndex)
	})
	primary := make([]string, len(primaryEntries))
	for i, e := range primaryEntries {
		primary[i] = e.name
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
	catalogPool shared.CatalogPool,
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

func (c *MySqlConnector) startSyncer(ctx context.Context, env map[string]string) (*replication.BinlogSyncer, error) {
	var tlsConfig *tls.Config
	if !c.config.DisableTls {
		var err error
		tlsConfig, err = common.CreateTlsConfig(
			tls.VersionTLS12, c.config.RootCa, c.config.Host, c.config.TlsHost, c.config.SkipCertVerification,
			nil,
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

	eventCacheCount, err := internal.PeerDBMySQLEventCacheCount(ctx, env)
	if err != nil {
		return nil, fmt.Errorf("failed to get event cache count: %w", err)
	}

	var serverId uint32
	if c.config.ServerId != nil {
		serverId = *c.config.ServerId
	} else {
		// If the configuration doesn't specify a server_id value, fallback to
		// default behavior of generating a random server_id in the range [1000, MaxUint32).
		// Range reference: https://dev.mysql.com/doc/refman/9.7/en/replication-options.html#sysvar_server_id
		serverId = 1000 + rand.Uint32()%(math.MaxUint32-1000) //nolint:gosec // G404: server_id does not require cryptographic randomness
	}

	return replication.NewBinlogSyncer(replication.BinlogSyncerConfig{
		ServerID:         serverId,
		Flavor:           c.Flavor(),
		Host:             config.Host,
		Port:             uint16(config.Port),
		User:             config.User,
		Password:         config.Password,
		Logger:           internal.SlogLoggerFromCtx(ctx),
		Dialer:           c.Dialer(),
		DisableRetrySync: true,
		UseDecimal:       true,
		ParseTime:        true,
		TLSConfig:        tlsConfig,
		HeartbeatPeriod:  c.binlogHeartbeatPeriod,
		EventCacheCount:  eventCacheCount,
	}), nil
}

func (c *MySqlConnector) startStreaming(
	ctx context.Context,
	pos string,
	env map[string]string,
) (*replication.BinlogSyncer, *replication.BinlogStreamer, mysql.GTIDSet, mysql.Position, error) {
	parsedOffset, err := parseReplicationOffsetText(c.Flavor(), pos)
	if err != nil {
		return nil, nil, nil, mysql.Position{}, err
	}

	switch parsedOffset.mechanism {
	case protos.MySqlReplicationMechanism_MYSQL_FILEPOS.String():
		return c.startCdcStreamingFilePos(ctx, parsedOffset.pos, env)
	case protos.MySqlReplicationMechanism_MYSQL_GTID.String():
		return c.startCdcStreamingGtid(ctx, parsedOffset.gset, env)
	default:
		return nil, nil, nil, mysql.Position{}, fmt.Errorf("empty mysql replication offset")
	}
}

func (c *MySqlConnector) startCdcStreamingFilePos(
	ctx context.Context,
	pos mysql.Position,
	env map[string]string,
) (*replication.BinlogSyncer, *replication.BinlogStreamer, mysql.GTIDSet, mysql.Position, error) {
	syncer, err := c.startSyncer(ctx, env)
	if err != nil {
		return nil, nil, nil, mysql.Position{}, err
	}
	stream, err := syncer.StartSync(pos)
	if err != nil {
		syncer.Close()
		return nil, nil, nil, mysql.Position{}, exceptions.NewMySQLExecuteError(err)
	}
	return syncer, stream, nil, pos, nil
}

func (c *MySqlConnector) startCdcStreamingGtid(
	ctx context.Context,
	gset mysql.GTIDSet,
	env map[string]string,
) (*replication.BinlogSyncer, *replication.BinlogStreamer, mysql.GTIDSet, mysql.Position, error) {
	syncer, err := c.startSyncer(ctx, env)
	if err != nil {
		return nil, nil, nil, mysql.Position{}, err
	}
	stream, err := syncer.StartSyncGTID(gset)
	if err != nil {
		syncer.Close()
		return nil, nil, nil, mysql.Position{}, exceptions.NewMySQLExecuteError(err)
	}
	return syncer, stream, gset, mysql.Position{}, nil
}

// closeSyncerWithTimeout is a safety net around syncer.Close(). go-mysql v1.15.0
// (https://github.com/go-mysql-org/go-mysql/commit/069f15d92122ca74c563d94cfc8de77a3799bbf6)
// fixed the bug that led to BinlogSyncer.Close hang, so this timeout should no
// longer fire. Keeping it around a bit longer before removing to ensure no regression.
func (c *MySqlConnector) closeSyncerWithTimeout(syncer *replication.BinlogSyncer, timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		syncer.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		c.logger.Error("[mysql] syncer.Close hung, force-closing SSH tunnel to unblock")
		_ = c.ssh.Close()
	}
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

	binlogStalenessThreshold, err := internal.PeerDBMySQLBinlogStalenessSeconds(ctx, req.Env)
	if err != nil {
		return err
	}

	isMariaDb := c.Flavor() == mysql.MariaDBFlavor
	binlogRowMetadataSupported, err := c.IsBinlogRowMetadataSupported(ctx)
	if err != nil {
		return fmt.Errorf("failed to determine if binlog row metadata is supported: %w", err)
	}

	syncer, mystream, gset, pos, err := c.startStreaming(ctx, req.LastOffset.Text, req.Env)
	if err != nil {
		return err
	}
	defer c.closeSyncerWithTimeout(syncer, 10*time.Second)

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
		span := trace.SpanFromContext(ctx)
		span.SetAttributes(
			attribute.Int64(otel_metrics.RowsInBatchKey, int64(recordCount)),
			attribute.Int64(otel_metrics.BytesPulledKey, totalFetchedBytes.Load()),
		)
		if updatedOffset != "" {
			span.SetAttributes(attribute.String(otel_metrics.GtidKey, updatedOffset))
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
	shutdown := common.Interval(ctx, time.Minute, func() {
		otelManager.Metrics.FetchedBytesCounter.Add(ctx, fetchedBytes.Swap(0))
		otelManager.Metrics.AllFetchedBytesCounter.Add(ctx, allFetchedBytes.Swap(0))
		c.logger.Info("[mysql] pulling records",
			slog.Uint64("records", uint64(recordCount)),
			slog.Int64("bytes", totalFetchedBytes.Load()),
			slog.Int("channelLen", req.RecordStream.ChannelLen()),
			slog.Float64("elapsedMinutes", time.Since(pullStart).Minutes()))
	})
	defer shutdown()

	timeoutCtx, cancelTimeout := context.WithTimeout(ctx, binlogStalenessThreshold)
	//nolint:gocritic // cancelTimeout is rebound, do not defer cancelTimeout()
	defer func() {
		cancelTimeout()
	}()
	resetTimeout := func(d time.Duration) {
		cancelTimeout()
		timeoutCtx, cancelTimeout = context.WithTimeout(ctx, d)
	}

	addRecord := func(ctx context.Context, record model.Record[model.RecordItems]) error {
		recordCount += 1
		if err := req.RecordStream.AddRecord(ctx, record); err != nil {
			return err
		}
		if recordCount == 1 {
			req.RecordStream.SignalAsNotEmpty()
			resetTimeout(req.IdleTimeout)
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

	recordCommitLagMetric := func(ctx context.Context, commitTs uint64) {
		if commitTs > 0 {
			otelManager.Metrics.CommitLagGauge.Record(ctx,
				time.Now().UTC().Sub(time.UnixMicro(int64(commitTs))).Microseconds())
		}
	}

	advanceCheckpoint := func(evGSet mysql.GTIDSet, logPos uint32) {
		if gset != nil {
			gset = evGSet
			updatedOffset = gset.String()
			req.RecordStream.UpdateLatestCheckpointText(updatedOffset)
		} else if logPos > pos.Pos {
			pos.Pos = logPos
			updatedOffset = posToOffsetText(pos)
			req.RecordStream.UpdateLatestCheckpointText(updatedOffset)
		}
	}

	lastEventAt := time.Now()
	var mysqlParser *parser.Parser
	processEvent := func(event *replication.BinlogEvent) error {
		switch ev := event.Event.(type) {
		case *replication.GTIDEvent:
			recordCommitLagMetric(ctx, ev.ImmediateCommitTimestamp)
		case *replication.GtidTaggedLogEvent: // MySQL 8.4+ tagged GTIDs
			recordCommitLagMetric(ctx, ev.ImmediateCommitTimestamp)
		case *replication.XIDEvent:
			advanceCheckpoint(ev.GSet, event.Header.LogPos)
			inTx = false
		case *replication.RotateEvent:
			if gset == nil && (event.Header.Timestamp != 0 || string(ev.NextLogName) != pos.Name) {
				pos.Name = string(ev.NextLogName)
				pos.Pos = uint32(ev.Position)
				updatedOffset = posToOffsetText(pos)
				req.RecordStream.UpdateLatestCheckpointText(updatedOffset)
				c.logger.Info("rotate", slog.String("name", pos.Name), slog.Uint64("pos", uint64(pos.Pos)))
			}
		case *replication.GenericEvent:
			// INCIDENT_EVENT (LOST_EVENTS) - fail and require resync
			if event.Header.EventType == replication.INCIDENT_EVENT {
				incident, message := parseIncidentEvent(ev.Data)
				c.logger.Error("[mysql] received binlog incident event, resync required",
					slog.Uint64("incident", uint64(incident)), slog.String("message", message))
				return exceptions.NewMySQLBinlogIncidentError(incident, message)
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
			setParserSQLModeFromStatusVars(mysqlParser, ev.StatusVars)
			stmts, warns, err := parseSQL(mysqlParser, ev.Query)
			if err != nil {
				if classifyUnparsedStatement(string(ev.Query), isMariaDb) == ddlKindIgnored {
					c.logger.Warn("skipping parse failure for non-replicated statement",
						slog.String("query", string(ev.Query)), slog.Any("error", err))
				} else {
					c.logger.Error("failed to parse QueryEvent", slog.String("query", string(ev.Query)), slog.Any("error", err))
					otelManager.Metrics.ParseSQLErrorsCounter.Add(ctx, 1)
				}
				break
			}
			if len(warns) > 0 {
				warnStrs := make([]string, len(warns))
				for i, w := range warns {
					warnStrs[i] = w.Error()
				}
				c.logger.Warn("processing QueryEvent with logged warnings", slog.Any("warns", warnStrs))
			}
			for _, stmt := range stmts {
				kind, alterStmt, renameStmt := classifyParsedStatement(stmt)
				switch kind {
				case ddlKindAlterTable:
					if err := c.processAlterTableQuery(
						ctx, catalogPool, otelManager, req, alterStmt,
						string(ev.Schema), binlogRowMetadataSupported, req.InternalVersion); err != nil {
						return fmt.Errorf("failed to process ALTER TABLE query: %w", err)
					}
				case ddlKindRenameTable:
					c.processRenameTableQuery(ctx, otelManager, req, renameStmt, string(ev.Schema))
				case ddlKindCommit, ddlKindRollback:
					// Non-transactional engines (e.g. MyISAM) end a binlog group with a COMMIT/ROLLBACK
					advanceCheckpoint(ev.GSet, event.Header.LogPos)
					inTx = false
				case ddlKindIgnored:
				default:
					return fmt.Errorf("unknown stmt kind: %v", kind)
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

				// Build colIdx -> encoding directly from TABLE_MAP charset metadata.
				// This mirrors go-mysql's collation traversal without allocating its maps;
				// when all character columns are utf8/ascii/binary, the slice stays nil.
				colEncodings, err := c.tableMapColumnEncodings(ctx, ev.Table, enumMap, setMap, otelManager)
				if err != nil {
					return err
				}
				encFor := func(idx int) encoding.Encoding {
					if idx >= 0 && idx < len(colEncodings) {
						return colEncodings[idx]
					}
					return nil
				}

				// Process TABLE_MAP_EVENT schema to detect new columns
				var fields []*protos.FieldDescription
				if ev.Table.ColumnName != nil {
					var err error
					fields, err = c.processTableMapEventSchema(
						ctx, catalogPool, otelManager, req, ev.Table,
						sourceTableName, destinationTableName, schema, exclusion,
					)
					if err != nil {
						return err
					}
				}

				getFd := func(idx int) *protos.FieldDescription {
					if fields != nil {
						if idx < len(fields) {
							return fields[idx]
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
							val, err := QValueFromMysqlRowEvent(ev.Table, idx, enumMap[idx], setMap[idx],
								types.QValueKind(fd.Type), val, encFor(idx), c.logger, &coercionReported)
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
								types.QValueKind(fd.Type), val, encFor(idx), c.logger, &coercionReported)
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
								types.QValueKind(fd.Type), val, encFor(idx), c.logger, &coercionReported)
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
								types.QValueKind(fd.Type), val, encFor(idx), c.logger, &coercionReported)
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
					return fmt.Errorf("mysql v0 replication protocol not supported")
				}
			}
			if event.Header.Timestamp > 0 {
				otelManager.Metrics.LatestConsumedLogEventGauge.Record(
					ctx,
					int64(event.Header.Timestamp),
				)
			}
		}

		return nil
	}

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
				return ctxErr
			}

			if errors.Is(err, context.DeadlineExceeded) {
				if recordCount == 0 || inTx {
					if since := time.Since(lastEventAt); since > binlogStalenessThreshold {
						return exceptions.NewMySQLStaleConnectionError(since, c.binlogHeartbeatPeriod)
					}

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
						resetTimeout(binlogStalenessThreshold)
					} else {
						c.logger.Info("[mysql] timeout reached, but still in transaction, waiting for inTx false",
							slog.Uint64("records", uint64(recordCount)),
							slog.Int64("bytes", totalFetchedBytes.Load()),
							slog.Int("channelLen", req.RecordStream.ChannelLen()),
							slog.Float64("elapsedMinutes", time.Since(pullStart).Minutes()))
						resetTimeout(time.Minute)
						overtime = true
					}

					continue
				}

				return nil
			}

			c.logger.Error("[mysql] PullRecords failed to get event", slog.Any("error", err))
			return exceptions.NewMySQLExecuteError(err)
		}

		lastEventAt = time.Now()

		allFetchedBytes.Add(int64(len(event.RawData)))

		switch ev := event.Event.(type) {
		case *replication.TransactionPayloadEvent:
			for _, inner := range ev.Events {
				err = processEvent(inner)
				if err != nil {
					return err
				}
			}
		default:
			err = processEvent(event)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func fieldDescriptionFromMysqlColumn(
	col *ast.ColumnDef, binlogRowMetadataSupported bool, mirrorVersion uint32,
) (*protos.FieldDescription, error) {
	if col.Tp == nil {
		return nil, fmt.Errorf("mysql column %s has no type", col.Name.OrigColName())
	}

	qkind, err := QkindFromMysqlColumnType(col.Tp.InfoSchemaStr(), binlogRowMetadataSupported, mirrorVersion)
	if err != nil {
		return nil, err
	}

	nullable := true
	for _, option := range col.Options {
		if option.Tp == ast.ColumnOptionNotNull {
			nullable = false
			break
		}
	}

	typmod := int32(-1)
	if qkind == types.QValueKindNumeric {
		precision := col.Tp.GetFlen()
		scale := col.Tp.GetDecimal()
		// TiDB leaves bare DECIMAL aliases without flen/decimal; MySQL defaults them to DECIMAL(10,0).
		if precision < 0 {
			precision = 10
		}
		if scale < 0 {
			scale = 0
		}
		typmod = datatypes.MakeNumericTypmod(int32(precision), int32(scale))
	}

	return &protos.FieldDescription{
		Name:         col.Name.OrigColName(),
		Type:         string(qkind),
		TypeModifier: typmod,
		Nullable:     nullable,
	}, nil
}

func (c *MySqlConnector) processAlterTableQuery(ctx context.Context, catalogPool shared.CatalogPool,
	otelManager *otel_metrics.OtelManager,
	req *model.PullRecordsRequest[model.RecordItems], stmt *ast.AlterTableStmt, stmtSchema string,
	binlogRowMetadataSupported bool, mirrorVersion uint32,
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

	existingColTypes := make(map[string]string)
	if currentSchema != nil {
		for _, col := range currentSchema.Columns {
			existingColTypes[col.Name] = col.Type
		}
	}

	hasPositionShiftingDdlChanges := false

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

				fd, err := fieldDescriptionFromMysqlColumn(col, binlogRowMetadataSupported, mirrorVersion)
				if err != nil {
					return err
				}
				qkind := types.QValueKind(fd.Type)

				if oldType, exists := existingColTypes[col.Name.OrigColName()]; exists && oldType != string(qkind) {
					c.logger.Warn("column type change detected via ALTER TABLE, not propagating",
						slog.String("table", sourceTableName),
						slog.String("column", col.Name.OrigColName()),
						slog.String("from", oldType),
						slog.String("to", string(qkind)))
					c.recordColumnTypeChange(ctx, otelManager, types.QValueKind(oldType), qkind,
						otel_metrics.SourceEventTypeDDL)
				}

				if spec.Position != nil && spec.Position.Tp != ast.ColumnPositionNone {
					hasPositionShiftingDdlChanges = true
					c.logger.Warn("column added with position specifier (FIRST/AFTER)",
						slog.String("columnName", col.Name.String()),
						slog.String("tableName", sourceTableName))
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
				hasPositionShiftingDdlChanges = true
				c.logger.Warn("dropped column detected but not propagating", slog.String("columnName", spec.OldColumnName.String()))
			}
		}
	}

	// When a column is dropped, or added with a position specifier, columns in future
	// change events may have a different ordinal position, so we cannot reliably map
	// columns by ordinal position if binlog_row_metadata is not supported.
	if hasPositionShiftingDdlChanges && !binlogRowMetadataSupported {
		c.logger.Error("Position-shifting DDL detected on table without binlog_row_metadata support",
			slog.String("table", sourceTableName),
			slog.Bool("binlogRowMetadataSupported", binlogRowMetadataSupported))
		return exceptions.NewMySQLUnsupportedDDLError(sourceTableName)
	}

	if tableSchemaDelta.AddedColumns != nil {
		c.logger.Info("Column added detected",
			slog.String("table", destinationTableName), slog.Any("columns", tableSchemaDelta.AddedColumns))
		req.RecordStream.AddSchemaDelta(req.TableNameMapping, tableSchemaDelta)
		return monitoring.AuditSchemaDelta(ctx, catalogPool.Pool, req.FlowJobName, tableSchemaDelta)
	}
	return nil
}

// processRenameTableQuery detects online schema-migration tools (gh-ost,
// pt-online-schema-change, ...). These never issue an ALTER on the tracked
// table; instead they build a shadow/ghost copy with the new schema and
// atomically rename it into place, e.g.:
//
//	RENAME TABLE users TO _users_del, _users_gho TO users
//
// The resulting schema change surfaces to us only later, as row-event metadata
// (see processTableMapEventSchema). Here we just meter how often a tracked table
// is renamed-into, so we can gauge how prevalent these tools are
func (c *MySqlConnector) processRenameTableQuery(
	ctx context.Context,
	otelManager *otel_metrics.OtelManager,
	req *model.PullRecordsRequest[model.RecordItems],
	stmt *ast.RenameTableStmt,
	stmtSchema string,
) {
	for _, t2t := range stmt.TableToTables {
		if t2t.NewTable == nil || t2t.OldTable == nil {
			continue
		}

		// if the rename target has no schema, fall back to the one on the event
		newSchemaName := t2t.NewTable.Schema.String()
		if newSchemaName == "" {
			newSchemaName = stmtSchema
		}
		newTableName := newSchemaName + "." + t2t.NewTable.Name.String()

		// only care about renames that land on a table we are replicating
		if _, tracked := req.TableNameMapping[newTableName]; !tracked {
			continue
		}

		oldTableName := t2t.OldTable.Name.String()
		tool := classifyOnlineSchemaMigrationTool(oldTableName, t2t.NewTable.Name.String())

		c.logger.Info("table atomically renamed into a tracked table, likely an online schema migration",
			slog.String("table", newTableName),
			slog.String("renamedFrom", oldTableName),
			slog.String("tool", tool))

		otelManager.Metrics.OnlineSchemaMigrationsCounter.Add(ctx, 1, metric.WithAttributeSet(attribute.NewSet(
			attribute.String(otel_metrics.SourcePeerType, "mysql"),
			attribute.String(otel_metrics.OnlineSchemaMigrationTool, tool),
		)))
	}
}

// classifyOnlineSchemaMigrationTool guesses which online schema-change tool
// performed a rename, based on the shadow table's naming convention:
//   - gh-ost ghost table:                _<table>_gho
//   - pt-online-schema-change new table: _<table>_new
//
// Anything else that renames into a tracked table is bucketed as "other".
func classifyOnlineSchemaMigrationTool(oldTable, newTable string) string {
	switch oldTable {
	case "_" + newTable + "_gho":
		return OnlineSchemaMigrationToolGhOst
	case "_" + newTable + "_new":
		return OnlineSchemaMigrationToolPtOsc
	default:
		return OnlineSchemaMigrationToolOther
	}
}

func posToOffsetText(pos mysql.Position) string {
	return fmt.Sprintf("!f:%s,%x", pos.Name, pos.Pos)
}

// parseIncidentEvent extracts the incident number and human-readable message.
// Best-effort: returns a diagnostic message if the body is malformed.
func parseIncidentEvent(data []byte) (uint16, string) {
	if len(data) < 2 {
		return 0, fmt.Sprintf("(payload too short: len=%d, raw=0x%s)",
			len(data), hex.EncodeToString(data))
	}
	incident := binary.LittleEndian.Uint16(data[:2])
	if len(data) < 3 {
		return incident, fmt.Sprintf("(payload too short: len=%d, raw=0x%s)",
			len(data), hex.EncodeToString(data))
	}
	end := min(3+int(data[2]), len(data))
	return incident, string(data[3:end])
}

func (c *MySqlConnector) recordColumnTypeChange(
	ctx context.Context, otelManager *otel_metrics.OtelManager, from types.QValueKind, to types.QValueKind,
	eventType string,
) {
	otelManager.Metrics.ColumnTypeChangesCounter.Add(ctx, 1, metric.WithAttributeSet(attribute.NewSet(
		attribute.String(otel_metrics.TypeChangeFromKey, string(from)),
		attribute.String(otel_metrics.TypeChangeToKey, string(to)),
		attribute.String(otel_metrics.SourceEventTypeKey, eventType),
	)))
}

// processTableMapEventSchema compares the TABLE_MAP_EVENT schema against the cached schema
// and returns a TableSchemaDelta if new columns are detected (e.g., after gh-ost migration).
// It also returns a slice mapping binlog column index to FieldDescription for efficient row processing.
func (c *MySqlConnector) processTableMapEventSchema(
	ctx context.Context,
	catalogPool shared.CatalogPool,
	otelManager *otel_metrics.OtelManager,
	req *model.PullRecordsRequest[model.RecordItems],
	tableMap *replication.TableMapEvent,
	sourceTableName string,
	destinationTableName string,
	schema *protos.TableSchema,
	exclusion map[string]struct{},
) ([]*protos.FieldDescription, error) {
	newFds := make([]*protos.FieldDescription, len(tableMap.ColumnName))

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

		var charset uint16
		if collation, ok := collationMap[idx]; ok {
			charset = uint16(collation)
		}

		if fd, exists := existingCols[colName]; exists {
			newFds[idx] = fd

			if qkind, err := qkindFromMysqlType(
				tableMap.ColumnType[idx], unsignedMap[idx], charset, req.InternalVersion,
			); err == nil && shouldReportColumnTypeChange(types.QValueKind(fd.Type), qkind, c.config.Flavor) {
				c.logger.Warn("column type change detected from TABLE_MAP_EVENT, not propagating",
					slog.String("table", sourceTableName),
					slog.String("column", colName),
					slog.String("from", fd.Type),
					slog.String("to", string(qkind)))
				c.recordColumnTypeChange(ctx, otelManager, types.QValueKind(fd.Type), qkind, otel_metrics.SourceEventTypeEventMetadata)
			}
		} else {
			// New column detected - get type from TABLE_MAP_EVENT
			mytype := tableMap.ColumnType[idx]
			qkind, err := qkindFromMysqlType(mytype, unsignedMap[idx], charset, req.InternalVersion)
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
			newFds[idx] = newFd

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

	return newFds, nil
}
