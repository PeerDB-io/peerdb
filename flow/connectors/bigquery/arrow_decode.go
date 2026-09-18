// Copyright 2023 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// This is adapted from cloud.google.com/go/bigquery/arrow.go for Arrow v18.
package connbigquery

import (
	"bytes"
	"fmt"
	"math/big"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

type bqArrowDecoder struct {
	tableSchema bigquery.Schema
	arrowSchema *arrow.Schema
}

func newBQArrowDecoder(serializedSchema []byte, schema bigquery.Schema) (*bqArrowDecoder, error) {
	r, err := ipc.NewReader(bytes.NewReader(serializedSchema))
	if err != nil {
		return nil, err
	}
	defer r.Release()
	arrowSchema := r.Schema()
	if len(schema) != len(arrowSchema.Fields()) {
		return nil, fmt.Errorf("BigQuery schema has %d fields but Arrow schema has %d", len(schema), len(arrowSchema.Fields()))
	}
	return &bqArrowDecoder{tableSchema: schema, arrowSchema: arrowSchema}, nil
}

func (d *bqArrowDecoder) decode(serializedBatch []byte) ([][]bigquery.Value, error) {
	r, err := ipc.NewReader(bytes.NewReader(serializedBatch), ipc.WithSchema(d.arrowSchema))
	if err != nil {
		return nil, err
	}
	defer r.Release()
	var rows [][]bigquery.Value
	for r.Next() {
		record := r.RecordBatch()
		batchRows := make([][]bigquery.Value, record.NumRows())
		for i := range batchRows {
			batchRows[i] = make([]bigquery.Value, record.NumCols())
		}
		for colIdx, col := range record.Columns() {
			for rowIdx := 0; rowIdx < col.Len(); rowIdx++ {
				value, err := bqArrowValue(col, rowIdx, d.arrowSchema.Field(colIdx).Type, d.tableSchema[colIdx])
				if err != nil {
					return nil, fmt.Errorf("column %s: %w", d.tableSchema[colIdx].Name, err)
				}
				batchRows[rowIdx][colIdx] = value
			}
		}
		rows = append(rows, batchRows...)
	}
	if err := r.Err(); err != nil {
		return nil, err
	}
	return rows, nil
}

func bqArrowValue(col arrow.Array, i int, dataType arrow.DataType, field *bigquery.FieldSchema) (bigquery.Value, error) {
	if !col.IsValid(i) {
		return nil, nil
	}
	switch typ := dataType.(type) {
	case *arrow.BooleanType:
		return col.(*array.Boolean).Value(i), nil
	case *arrow.Int8Type:
		return int64(col.(*array.Int8).Value(i)), nil
	case *arrow.Int16Type:
		return int64(col.(*array.Int16).Value(i)), nil
	case *arrow.Int32Type:
		return int64(col.(*array.Int32).Value(i)), nil
	case *arrow.Int64Type:
		return col.(*array.Int64).Value(i), nil
	case *arrow.Float16Type:
		return float64(col.(*array.Float16).Value(i).Float32()), nil
	case *arrow.Float32Type:
		return float64(col.(*array.Float32).Value(i)), nil
	case *arrow.Float64Type:
		return col.(*array.Float64).Value(i), nil
	case *arrow.BinaryType:
		return bytes.Clone(col.(*array.Binary).Value(i)), nil
	case *arrow.StringType:
		v := col.(*array.String).Value(i)
		if field.Type == bigquery.IntervalFieldType {
			return bigquery.ParseInterval(v)
		}
		return v, nil
	case *arrow.Date32Type:
		return civil.ParseDate(col.(*array.Date32).Value(i).FormattedString())
	case *arrow.Date64Type:
		return civil.ParseDate(col.(*array.Date64).Value(i).FormattedString())
	case *arrow.TimestampType:
		t := col.(*array.Timestamp).Value(i).ToTime(typ.Unit)
		if typ.TimeZone == "" {
			return civil.DateTimeOf(t), nil
		}
		return t.UTC(), nil
	case *arrow.Time32Type:
		return civil.ParseTime(col.(*array.Time32).Value(i).FormattedString(arrow.Microsecond))
	case *arrow.Time64Type:
		return civil.ParseTime(col.(*array.Time64).Value(i).FormattedString(arrow.Microsecond))
	case *arrow.Decimal128Type:
		v, ok := new(big.Rat).SetString(col.(*array.Decimal128).Value(i).ToString(typ.Scale))
		if !ok {
			return nil, fmt.Errorf("invalid decimal128")
		}
		return v, nil
	case *arrow.Decimal256Type:
		v, ok := new(big.Rat).SetString(col.(*array.Decimal256).Value(i).ToString(typ.Scale))
		if !ok {
			return nil, fmt.Errorf("invalid decimal256")
		}
		return v, nil
	case *arrow.ListType:
		arr := col.(*array.List)
		start, end := arr.ValueOffsets(i)
		values := make([]bigquery.Value, 0, end-start)
		for j := start; j < end; j++ {
			value, err := bqArrowValue(arr.ListValues(), int(j), typ.Elem(), field)
			if err != nil {
				return nil, err
			}
			values = append(values, value)
		}
		return values, nil
	case *arrow.StructType:
		arr := col.(*array.Struct)
		fields := typ.Fields()
		if field.Type == bigquery.RangeFieldType {
			if field.RangeElementType == nil {
				return nil, fmt.Errorf("range field has no element type")
			}
			element := &bigquery.FieldSchema{Type: field.RangeElementType.Type}
			start, err := bqArrowValue(arr.Field(0), i, fields[0].Type, element)
			if err != nil {
				return nil, err
			}
			end, err := bqArrowValue(arr.Field(1), i, fields[1].Type, element)
			if err != nil {
				return nil, err
			}
			return &bigquery.RangeValue{Start: start, End: end}, nil
		}
		if len(field.Schema) != len(fields) {
			return nil, fmt.Errorf("record schema has %d fields but Arrow struct has %d", len(field.Schema), len(fields))
		}
		values := make([]bigquery.Value, len(fields))
		for idx, nested := range fields {
			value, err := bqArrowValue(arr.Field(idx), i, nested.Type, field.Schema[idx])
			if err != nil {
				return nil, err
			}
			values[idx] = value
		}
		return values, nil
	default:
		return nil, fmt.Errorf("unsupported Arrow type %s", dataType)
	}
}
