package connpostgres

import (
	"fmt"
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/types"
)

// TestInheritedTableWithExtraColumns verifies that CDC correctly handles child tables
// that inherit from a parent but have additional columns beyond the parent's schema.
//
// Without the fix, the child's RelationMessage would be stored under the parent's relation ID,
// causing cross-child contamination and positional column mismatches during tuple decoding.
func TestInheritedTableWithExtraColumns(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	connector, schemaName := setupDB(t, "cdc_inherit_extra_cols")
	defer connector.Close()
	defer teardownDB(t, connector.conn, schemaName)

	parentTable := common.QuoteIdentifier(schemaName) + ".payment"
	childTable := common.QuoteIdentifier(schemaName) + ".stripe_payment"

	_, err := connector.conn.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			id_payment UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			amount NUMERIC(12,2) NOT NULL,
			status TEXT NOT NULL,
			fk_customer UUID NOT NULL
		);
		CREATE TABLE %s (
			fk_stripe_payment TEXT NOT NULL,
			stripe_account_id TEXT NOT NULL
		) INHERITS (%s);
	`, parentTable, childTable, parentTable))
	require.NoError(t, err)

	var parentOID, childOID uint32
	err = connector.conn.QueryRow(ctx,
		`SELECT c.oid FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid
		 WHERE n.nspname = $1 AND c.relname = 'payment'`, schemaName).Scan(&parentOID)
	require.NoError(t, err)
	err = connector.conn.QueryRow(ctx,
		`SELECT c.oid FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid
		 WHERE n.nspname = $1 AND c.relname = 'stripe_payment'`, schemaName).Scan(&childOID)
	require.NoError(t, err)

	tableName := schemaName + ".payment"
	srcTableIDNameMapping := map[uint32]string{parentOID: tableName}
	tableNameMapping := map[string]model.NameAndExclude{
		tableName: {Name: "dest_payment", Exclude: nil},
	}

	cdc, err := connector.NewPostgresCDCSource(ctx, &PostgresCDCConfig{
		SrcTableIDNameMapping:                    srcTableIDNameMapping,
		TableNameMapping:                         tableNameMapping,
		TableNameSchemaMapping:                   nil,
		RelationMessageMapping:                   connector.relationMessageMapping,
		FlowJobName:                              "test_flow",
		Slot:                                     "test_slot",
		Publication:                              "test_pub",
		HandleInheritanceForNonPartitionedTables: true,
		InternalVersion:                          shared.InternalVersion_Latest,
	})
	require.NoError(t, err)

	// Parent has 4 columns: id_payment (UUID), amount (NUMERIC), status (TEXT), fk_customer (UUID)
	parentRelMsg := &pglogrepl.RelationMessage{
		RelationID: parentOID,
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id_payment", DataType: pgtype.UUIDOID, TypeModifier: -1},
			{Name: "amount", DataType: pgtype.NumericOID, TypeModifier: -1},
			{Name: "status", DataType: pgtype.TextOID, TypeModifier: -1},
			{Name: "fk_customer", DataType: pgtype.UUIDOID, TypeModifier: -1},
		},
	}
	// Child has 6 columns: the 4 inherited + fk_stripe_payment (TEXT) + stripe_account_id (TEXT)
	childRelMsg := &pglogrepl.RelationMessage{
		RelationID: childOID,
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id_payment", DataType: pgtype.UUIDOID, TypeModifier: -1},
			{Name: "amount", DataType: pgtype.NumericOID, TypeModifier: -1},
			{Name: "status", DataType: pgtype.TextOID, TypeModifier: -1},
			{Name: "fk_customer", DataType: pgtype.UUIDOID, TypeModifier: -1},
			{Name: "fk_stripe_payment", DataType: pgtype.TextOID, TypeModifier: -1},
			{Name: "stripe_account_id", DataType: pgtype.TextOID, TypeModifier: -1},
		},
	}
	connector.relationMessageMapping[parentOID] = parentRelMsg
	connector.relationMessageMapping[childOID] = childRelMsg

	// Insert from child: 6 columns including the Stripe-specific ones.
	// fk_stripe_payment contains a Stripe charge ID that would fail UUID parsing
	// if decoded with the parent's 4-column schema (positional mismatch).
	testUUID := "550e8400-e29b-41d4-a716-446655440000"
	customerUUID := "660e8400-e29b-41d4-a716-446655440000"
	insertMsg := &pglogrepl.InsertMessage{
		RelationID: childOID,
		Tuple: &pglogrepl.TupleData{
			Columns: []*pglogrepl.TupleDataColumn{
				{DataType: 't', Data: []byte(testUUID)},
				{DataType: 't', Data: []byte("99.95")},
				{DataType: 't', Data: []byte("completed")},
				{DataType: 't', Data: []byte(customerUUID)},
				{DataType: 't', Data: []byte("ch_3T1KxCFUtwYrZPVC0M6OeTpy")},
				{DataType: 't', Data: []byte("acct_1234567890")},
			},
		},
	}

	record, err := processInsertMessage(cdc, pglogrepl.LSN(0), insertMsg, qProcessor{}, nil)
	require.NoError(t, err, "child table insert with extra columns must not fail")
	require.NotNil(t, record)

	insertRec, ok := record.(*model.InsertRecord[model.RecordItems])
	require.True(t, ok)
	require.Equal(t, tableName, insertRec.SourceTableName, "should be routed to parent table name")
	require.Equal(t, "dest_payment", insertRec.DestinationTableName)

	// Verify inherited columns are decoded correctly
	idVal := insertRec.Items.GetColumnValue("id_payment")
	require.NotNil(t, idVal)
	require.Equal(t, types.QValueKindUUID, idVal.Kind())

	statusVal := insertRec.Items.GetColumnValue("status")
	require.NotNil(t, statusVal)
	require.Equal(t, "completed", statusVal.Value())

	// Verify child-specific columns are decoded as text, not misinterpreted
	stripeVal := insertRec.Items.GetColumnValue("fk_stripe_payment")
	require.NotNil(t, stripeVal, "child-specific column fk_stripe_payment must be present")
	require.Equal(t, types.QValueKindString, stripeVal.Kind(),
		"fk_stripe_payment must be decoded as string, not UUID")
	require.Equal(t, "ch_3T1KxCFUtwYrZPVC0M6OeTpy", stripeVal.Value())

	acctVal := insertRec.Items.GetColumnValue("stripe_account_id")
	require.NotNil(t, acctVal, "child-specific column stripe_account_id must be present")
	require.Equal(t, "acct_1234567890", acctVal.Value())
}

// TestMultipleInheritedChildrenNoContamination verifies that two child tables with
// different extra columns don't contaminate each other's tuple decoding.
// This catches the pre-fix bug where all children's RelationMessages were stored under
// the parent's relation ID, so whichever child sent last would overwrite the others.
func TestMultipleInheritedChildrenNoContamination(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	connector, schemaName := setupDB(t, "cdc_inherit_multi_child")
	defer connector.Close()
	defer teardownDB(t, connector.conn, schemaName)

	parentTable := common.QuoteIdentifier(schemaName) + ".payment"
	stripeChild := common.QuoteIdentifier(schemaName) + ".stripe_payment"
	paypalChild := common.QuoteIdentifier(schemaName) + ".paypal_payment"

	_, err := connector.conn.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			id_payment UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			amount NUMERIC(12,2) NOT NULL,
			status TEXT NOT NULL
		);
		CREATE TABLE %s (
			fk_stripe_payment TEXT NOT NULL
		) INHERITS (%s);
		CREATE TABLE %s (
			paypal_order_id TEXT NOT NULL,
			paypal_payer_id TEXT NOT NULL
		) INHERITS (%s);
	`, parentTable, stripeChild, parentTable, paypalChild, parentTable))
	require.NoError(t, err)

	var parentOID, stripeOID, paypalOID uint32
	for _, tbl := range []struct {
		oid  *uint32
		name string
	}{
		{&parentOID, "payment"},
		{&stripeOID, "stripe_payment"},
		{&paypalOID, "paypal_payment"},
	} {
		err = connector.conn.QueryRow(ctx,
			`SELECT c.oid FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid
			 WHERE n.nspname = $1 AND c.relname = $2`, schemaName, tbl.name).Scan(tbl.oid)
		require.NoError(t, err)
	}

	tableName := schemaName + ".payment"
	srcTableIDNameMapping := map[uint32]string{parentOID: tableName}
	tableNameMapping := map[string]model.NameAndExclude{
		tableName: {Name: "dest_payment", Exclude: nil},
	}

	cdc, err := connector.NewPostgresCDCSource(ctx, &PostgresCDCConfig{
		SrcTableIDNameMapping:                    srcTableIDNameMapping,
		TableNameMapping:                         tableNameMapping,
		TableNameSchemaMapping:                   nil,
		RelationMessageMapping:                   connector.relationMessageMapping,
		FlowJobName:                              "test_flow",
		Slot:                                     "test_slot",
		Publication:                              "test_pub",
		HandleInheritanceForNonPartitionedTables: true,
		InternalVersion:                          shared.InternalVersion_Latest,
	})
	require.NoError(t, err)

	// Store RelationMessages under their own (child) relation IDs
	connector.relationMessageMapping[parentOID] = &pglogrepl.RelationMessage{
		RelationID: parentOID,
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id_payment", DataType: pgtype.UUIDOID, TypeModifier: -1},
			{Name: "amount", DataType: pgtype.NumericOID, TypeModifier: -1},
			{Name: "status", DataType: pgtype.TextOID, TypeModifier: -1},
		},
	}
	// stripe_payment: 3 inherited + 1 extra
	connector.relationMessageMapping[stripeOID] = &pglogrepl.RelationMessage{
		RelationID: stripeOID,
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id_payment", DataType: pgtype.UUIDOID, TypeModifier: -1},
			{Name: "amount", DataType: pgtype.NumericOID, TypeModifier: -1},
			{Name: "status", DataType: pgtype.TextOID, TypeModifier: -1},
			{Name: "fk_stripe_payment", DataType: pgtype.TextOID, TypeModifier: -1},
		},
	}
	// paypal_payment: 3 inherited + 2 different extras
	connector.relationMessageMapping[paypalOID] = &pglogrepl.RelationMessage{
		RelationID: paypalOID,
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id_payment", DataType: pgtype.UUIDOID, TypeModifier: -1},
			{Name: "amount", DataType: pgtype.NumericOID, TypeModifier: -1},
			{Name: "status", DataType: pgtype.TextOID, TypeModifier: -1},
			{Name: "paypal_order_id", DataType: pgtype.TextOID, TypeModifier: -1},
			{Name: "paypal_payer_id", DataType: pgtype.TextOID, TypeModifier: -1},
		},
	}

	testUUID := "550e8400-e29b-41d4-a716-446655440000"

	// Process a stripe insert
	stripeInsert := &pglogrepl.InsertMessage{
		RelationID: stripeOID,
		Tuple: &pglogrepl.TupleData{
			Columns: []*pglogrepl.TupleDataColumn{
				{DataType: 't', Data: []byte(testUUID)},
				{DataType: 't', Data: []byte("49.99")},
				{DataType: 't', Data: []byte("completed")},
				{DataType: 't', Data: []byte("ch_3T1KxCFUtwYrZPVC0M6OeTpy")},
			},
		},
	}
	rec, err := processInsertMessage(cdc, pglogrepl.LSN(1), stripeInsert, qProcessor{}, nil)
	require.NoError(t, err, "stripe child insert must succeed")
	stripeRec := rec.(*model.InsertRecord[model.RecordItems])
	require.Equal(t, "ch_3T1KxCFUtwYrZPVC0M6OeTpy",
		stripeRec.Items.GetColumnValue("fk_stripe_payment").Value())

	// Now process a paypal insert — must not be contaminated by stripe's RelationMessage
	paypalInsert := &pglogrepl.InsertMessage{
		RelationID: paypalOID,
		Tuple: &pglogrepl.TupleData{
			Columns: []*pglogrepl.TupleDataColumn{
				{DataType: 't', Data: []byte(testUUID)},
				{DataType: 't', Data: []byte("25.00")},
				{DataType: 't', Data: []byte("pending")},
				{DataType: 't', Data: []byte("PAYID-ORDER-123")},
				{DataType: 't', Data: []byte("PAYER-456")},
			},
		},
	}
	rec, err = processInsertMessage(cdc, pglogrepl.LSN(2), paypalInsert, qProcessor{}, nil)
	require.NoError(t, err, "paypal child insert must succeed")
	paypalRec := rec.(*model.InsertRecord[model.RecordItems])
	require.Equal(t, "PAYID-ORDER-123",
		paypalRec.Items.GetColumnValue("paypal_order_id").Value(),
		"paypal_order_id must be decoded correctly, not using stripe's schema")
	require.Equal(t, "PAYER-456",
		paypalRec.Items.GetColumnValue("paypal_payer_id").Value())

	// Verify both are routed to the parent table
	require.Equal(t, tableName, stripeRec.SourceTableName)
	require.Equal(t, tableName, paypalRec.SourceTableName)
}

// TestChildTableSchemaDeltaNotFalsePositive verifies that child-specific columns
// are NOT treated as schema changes on the parent table.
// Without the fix, a child's extra columns would be falsely detected as "added columns"
// on the parent, triggering spurious ALTER TABLE statements on the destination.
func TestChildTableSchemaDeltaNotFalsePositive(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	connector, schemaName := setupDB(t, "cdc_inherit_schema_delta")
	defer connector.Close()
	defer teardownDB(t, connector.conn, schemaName)

	parentTable := common.QuoteIdentifier(schemaName) + ".payment"
	stripeChild := common.QuoteIdentifier(schemaName) + ".stripe_payment"
	promoChild := common.QuoteIdentifier(schemaName) + ".promo_payment"

	_, err := connector.conn.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			id_payment UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			amount NUMERIC(12,2) NOT NULL,
			status TEXT NOT NULL
		);
		CREATE TABLE %s (
			fk_stripe_payment TEXT NOT NULL,
			stripe_account_id TEXT NOT NULL
		) INHERITS (%s);
		CREATE TABLE %s (
			fk_promo_payment TEXT NOT NULL
		) INHERITS (%s);
	`, parentTable, stripeChild, parentTable, promoChild, parentTable))
	require.NoError(t, err)

	var parentOID, stripeOID, promoOID uint32
	for _, tbl := range []struct {
		oid  *uint32
		name string
	}{
		{&parentOID, "payment"},
		{&stripeOID, "stripe_payment"},
		{&promoOID, "promo_payment"},
	} {
		err = connector.conn.QueryRow(ctx,
			`SELECT c.oid FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid
			 WHERE n.nspname = $1 AND c.relname = $2`, schemaName, tbl.name).Scan(tbl.oid)
		require.NoError(t, err)
	}

	tableName := schemaName + ".payment"
	srcTableIDNameMapping := map[uint32]string{parentOID: tableName}
	tableNameMapping := map[string]model.NameAndExclude{
		tableName: {Name: "dest_payment", Exclude: nil},
	}
	tableNameSchemaMapping := map[string]*protos.TableSchema{
		"dest_payment": {
			Columns: []*protos.FieldDescription{
				{Name: "id_payment", Type: string(types.QValueKindUUID)},
				{Name: "amount", Type: string(types.QValueKindNumeric)},
				{Name: "status", Type: string(types.QValueKindString)},
			},
			System: protos.TypeSystem_Q,
		},
	}

	cdc, err := connector.NewPostgresCDCSource(ctx, &PostgresCDCConfig{
		SrcTableIDNameMapping:                    srcTableIDNameMapping,
		TableNameMapping:                         tableNameMapping,
		TableNameSchemaMapping:                   tableNameSchemaMapping,
		RelationMessageMapping:                   connector.relationMessageMapping,
		FlowJobName:                              "test_flow",
		Slot:                                     "test_slot",
		Publication:                              "test_pub",
		HandleInheritanceForNonPartitionedTables: true,
		InternalVersion:                          shared.InternalVersion_Latest,
	})
	require.NoError(t, err)

	// --- Case 1: Child table with extra columns must NOT produce a schema delta ---
	// stripe_payment has fk_stripe_payment and stripe_account_id beyond the parent's columns.
	// These exist on the child but NOT in pg_attribute for the parent, so they must be filtered out.
	stripeRelMsg := &pglogrepl.RelationMessage{
		RelationID: stripeOID,
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id_payment", DataType: pgtype.UUIDOID, TypeModifier: -1},
			{Name: "amount", DataType: pgtype.NumericOID, TypeModifier: -1},
			{Name: "status", DataType: pgtype.TextOID, TypeModifier: -1},
			{Name: "fk_stripe_payment", DataType: pgtype.TextOID, TypeModifier: -1},
			{Name: "stripe_account_id", DataType: pgtype.TextOID, TypeModifier: -1},
		},
	}

	rec, err := processRelationMessage[model.RecordItems](ctx, cdc, pglogrepl.LSN(1), stripeRelMsg)
	require.NoError(t, err)
	require.Nil(t, rec, "child-specific columns must not produce a schema delta")

	// The child's relation should be stored for tuple decoding
	_, stored := connector.relationMessageMapping[stripeOID]
	require.True(t, stored, "child relation must be stored after processing")

	// --- Case 2: Different child with different extra columns also must NOT produce a delta ---
	promoRelMsg := &pglogrepl.RelationMessage{
		RelationID: promoOID,
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id_payment", DataType: pgtype.UUIDOID, TypeModifier: -1},
			{Name: "amount", DataType: pgtype.NumericOID, TypeModifier: -1},
			{Name: "status", DataType: pgtype.TextOID, TypeModifier: -1},
			{Name: "fk_promo_payment", DataType: pgtype.TextOID, TypeModifier: -1},
		},
	}

	rec, err = processRelationMessage[model.RecordItems](ctx, cdc, pglogrepl.LSN(2), promoRelMsg)
	require.NoError(t, err)
	require.Nil(t, rec, "promo child-specific columns must not produce a schema delta")

	// --- Case 3: Repeated relation with same child columns must NOT produce a delta ---
	rec, err = processRelationMessage[model.RecordItems](ctx, cdc, pglogrepl.LSN(3), stripeRelMsg)
	require.NoError(t, err)
	require.Nil(t, rec, "repeated child relation must not produce a schema delta")

	// --- Case 4: Genuine ALTER TABLE on the parent must still be detected via child ---
	// Actually add the column to the parent table in Postgres so pg_attribute reflects it.
	_, err = connector.conn.Exec(ctx, fmt.Sprintf(
		`ALTER TABLE %s ADD COLUMN metadata TEXT`, parentTable))
	require.NoError(t, err)

	// The child's RELATION now includes "metadata" (inherited from parent ALTER).
	stripeRelMsgWithNew := &pglogrepl.RelationMessage{
		RelationID: stripeOID,
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id_payment", DataType: pgtype.UUIDOID, TypeModifier: -1},
			{Name: "amount", DataType: pgtype.NumericOID, TypeModifier: -1},
			{Name: "status", DataType: pgtype.TextOID, TypeModifier: -1},
			{Name: "fk_stripe_payment", DataType: pgtype.TextOID, TypeModifier: -1},
			{Name: "stripe_account_id", DataType: pgtype.TextOID, TypeModifier: -1},
			{Name: "metadata", DataType: pgtype.TextOID, TypeModifier: -1},
		},
	}

	rec, err = processRelationMessage[model.RecordItems](ctx, cdc, pglogrepl.LSN(4), stripeRelMsgWithNew)
	require.NoError(t, err)
	require.NotNil(t, rec, "genuinely new parent column must produce a schema delta")

	relRec, ok := rec.(*model.RelationRecord[model.RecordItems])
	require.True(t, ok)
	require.Len(t, relRec.TableSchemaDelta.AddedColumns, 1,
		"only the genuinely new column should appear, not child-specific columns")
	require.Equal(t, "metadata", relRec.TableSchemaDelta.AddedColumns[0].Name)
}
