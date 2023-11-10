package e2e_s3

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/PeerDB-io/peer-flow/e2e"
	peerflow "github.com/PeerDB-io/peer-flow/workflows"
	"github.com/stretchr/testify/require"
)

func (s *PeerFlowE2ETestSuiteS3) attachSchemaSuffix(tableName string) string {
	return fmt.Sprintf("e2e_test_%s.%s", s3Suffix, tableName)
}

func (s *PeerFlowE2ETestSuiteS3) attachSuffix(input string) string {
	return fmt.Sprintf("%s_%s", input, s3Suffix)
}

func (s *PeerFlowE2ETestSuiteS3) Test_Complete_Simple_Flow_S3() {
	env := s.NewTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(env)

	srcTableName := s.attachSchemaSuffix("test_simple_flow_s3")
	dstTableName := fmt.Sprintf("%s.%s", "peerdb_test_s3", "test_simple_flow_s3")
	flowJobName := s.attachSuffix("test_simple_flow_s3")
	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE %s (
			id SERIAL PRIMARY KEY,
			key TEXT NOT NULL,
			value TEXT NOT NULL
		);
	`, srcTableName))
	s.NoError(err)
	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      flowJobName,
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.s3Helper.GetPeer(),
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	s.NoError(err)

	limits := peerflow.CDCFlowLimits{
		TotalSyncFlows:   4,
		ExitAfterRecords: 20,
		MaxBatchSize:     5,
	}

	go func() {
		e2e.SetupCDCFlowStatusQuery(env, connectionGen)
		s.NoError(err)
		//insert 20 rows
		for i := 1; i <= 20; i++ {
			testKey := fmt.Sprintf("test_key_%d", i)
			testValue := fmt.Sprintf("test_value_%d", i)
			_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s (key, value) VALUES ($1, $2)
		`, srcTableName), testKey, testValue)
			s.NoError(err)
		}
		s.NoError(err)
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	s.Error(err)
	s.Contains(err.Error(), "continue as new")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	fmt.Println("JobName: ", flowJobName)
	files, err := s.s3Helper.ListAllFiles(ctx, flowJobName)
	fmt.Println("Files in Test_Complete_Simple_Flow_S3: ", len(files))
	require.NoError(s.T(), err)

	require.Equal(s.T(), 4, len(files))

	env.AssertExpectations(s.T())
}

func (s *PeerFlowE2ETestSuiteS3) Test_Complete_Simple_Flow_GCS_Interop() {
	env := s.NewTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(env)
	setupErr := s.setupS3("gcs")
	if setupErr != nil {
		s.Fail("failed to setup S3", setupErr)
	}

	srcTableName := s.attachSchemaSuffix("test_simple_flow_gcs_interop")
	dstTableName := fmt.Sprintf("%s.%s", "peerdb_test_gcs_interop", "test_simple_flow_gcs_interop")
	flowJobName := s.attachSuffix("test_simple_flow_gcs_interop")
	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
		CREATE TABLE %s (
			id SERIAL PRIMARY KEY,
			key TEXT NOT NULL,
			value TEXT NOT NULL
		);
	`, srcTableName))
	s.NoError(err)
	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      flowJobName,
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.s3Helper.GetPeer(),
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	s.NoError(err)

	limits := peerflow.CDCFlowLimits{
		TotalSyncFlows:   4,
		ExitAfterRecords: 20,
		MaxBatchSize:     5,
	}

	go func() {
		e2e.SetupCDCFlowStatusQuery(env, connectionGen)
		s.NoError(err)
		//insert 20 rows
		for i := 1; i <= 20; i++ {
			testKey := fmt.Sprintf("test_key_%d", i)
			testValue := fmt.Sprintf("test_value_%d", i)
			_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
			INSERT INTO %s (key, value) VALUES ($1, $2)
		`, srcTableName), testKey, testValue)
			s.NoError(err)
		}
		fmt.Println("Inserted 20 rows into the source table")
		s.NoError(err)
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	s.Error(err)
	s.Contains(err.Error(), "continue as new")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	files, err := s.s3Helper.ListAllFiles(ctx, flowJobName)
	fmt.Println("Files in Test_Complete_Simple_Flow_GCS: ", len(files))
	require.NoError(s.T(), err)

	require.Equal(s.T(), 4, len(files))

	env.AssertExpectations(s.T())
}

func (s *PeerFlowE2ETestSuiteS3) Test_Types_GCS_Interop() {
	env := s.NewTestWorkflowEnvironment()
	e2e.RegisterWorkflowsAndActivities(env)
	setupErr := s.setupS3("gcs")
	if setupErr != nil {
		s.Fail("failed to setup S3", setupErr)
	}

	srcTableName := s.attachSchemaSuffix("test_types_interop")
	dstTableName := fmt.Sprintf("%s.%s_%d", "peerdb_test_gcs_interop", "test_types_interop", time.Now().Unix())
	flowJobName := s.attachSuffix("test_simple_flow")

	createMoodEnum := "CREATE TYPE mood AS ENUM ('happy', 'sad', 'angry');"
	_, enumErr := s.pool.Exec(context.Background(), createMoodEnum)
	if !strings.Contains(enumErr.Error(), "already exists") {
		s.NoError(enumErr)
	}

	_, err := s.pool.Exec(context.Background(), fmt.Sprintf(`
	CREATE TABLE %s (id serial PRIMARY KEY,c1 BIGINT,c2 BIT,c3 VARBIT,c4 BOOLEAN,
		c6 BYTEA,c7 CHARACTER,c8 varchar,c9 CIDR,c11 DATE,c12 FLOAT,c13 DOUBLE PRECISION,
		c14 INET,c15 INTEGER,c16 INTERVAL,c17 JSON,c18 JSONB,c21 MACADDR,c22 MONEY,
		c23 NUMERIC,c24 OID,c28 REAL,c29 SMALLINT,c30 SMALLSERIAL,c31 SERIAL,c32 TEXT,
		c33 TIMESTAMP,c34 TIMESTAMPTZ,c35 TIME, c36 TIMETZ,c37 TSQUERY,c38 TSVECTOR,
		c39 TXID_SNAPSHOT,c40 UUID,c41 XML, c42 GEOMETRY(POINT), c43 GEOGRAPHY(POINT),
		c44 GEOGRAPHY(POLYGON), c45 GEOGRAPHY(LINESTRING), c46 GEOMETRY(LINESTRING), c47 GEOMETRY(POLYGON),
		c48 mood, c49 hstore, c51 cidr, c52 citext, c53 ltree);
	CREATE OR REPLACE FUNCTION random_bytea(bytea_length integer)
		RETURNS bytea AS $body$
			SELECT decode(string_agg(lpad(to_hex(width_bucket(random(), 0, 1, 256)-1),2,'0') ,''), 'hex')
			FROM generate_series(1, $1);
		$body$
		LANGUAGE 'sql'
		VOLATILE
		SET search_path = 'pg_catalog';
	`, srcTableName))
	s.NoError(err)
	connectionGen := e2e.FlowConnectionGenerationConfig{
		FlowJobName:      flowJobName,
		TableNameMapping: map[string]string{srcTableName: dstTableName},
		PostgresPort:     e2e.PostgresPort,
		Destination:      s.s3Helper.GetPeer(),
	}

	flowConnConfig, err := connectionGen.GenerateFlowConnectionConfigs()
	s.NoError(err)

	limits := peerflow.CDCFlowLimits{
		TotalSyncFlows: 2,
		MaxBatchSize:   5,
	}

	go func() {
		e2e.SetupCDCFlowStatusQuery(env, connectionGen)
		s.NoError(err)
		_, err = s.pool.Exec(context.Background(), fmt.Sprintf(`
		INSERT INTO %s SELECT 2,2,b'1',b'101',
		true,random_bytea(32),'s','test','1.1.10.2'::cidr,
		CURRENT_DATE,1.23,1.234,'192.168.1.5'::inet,1,
		'5 years 2 months 29 days 1 minute 2 seconds 200 milliseconds 20000 microseconds'::interval,
		'{"sai":1}'::json,'{"sai":1}'::jsonb,'08:00:2b:01:02:03'::macaddr,
		1.2,1.23,4::oid,1.23,1,1,1,'test',now(),now(),now()::time,now()::timetz,
		'fat & rat'::tsquery,'a fat cat sat on a mat and ate a fat rat'::tsvector,
		txid_current_snapshot(),
		'66073c38-b8df-4bdb-bbca-1c97596b8940'::uuid,xmlcomment('hello'),
		'POINT(1 2)','POINT(40.7128 -74.0060)','POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))',
		'LINESTRING(-74.0060 40.7128, -73.9352 40.7306, -73.9123 40.7831)','LINESTRING(0 0, 1 1, 2 2)',
		'POLYGON((-74.0060 40.7128, -73.9352 40.7306, -73.9123 40.7831, -74.0060 40.7128))',
		'sad', 'a=>1,b=>2'::hstore,'192.168.0.0/16','abc','Top.Top1.Top2'::ltree;
		`, srcTableName))
		s.NoError(err)
		fmt.Println("Executed an insert with all types")
	}()

	env.ExecuteWorkflow(peerflow.CDCFlowWorkflowWithConfig, flowConnConfig, &limits, nil)

	// Verify workflow completes without error
	s.True(env.IsWorkflowCompleted())
	err = env.GetWorkflowError()

	// allow only continue as new error
	s.Error(err)
	s.Contains(err.Error(), "continue as new")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	files, err := s.s3Helper.ListAllFiles(ctx, flowJobName)
	fmt.Println("Files in Test_Types_GCS: ", len(files))
	require.NoError(s.T(), err)

	require.Equal(s.T(), 1, len(files))

	env.AssertExpectations(s.T())
}
