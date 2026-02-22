package storageengine

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"strings"

	"DaemonDB/types"
)

// SerializeRow converts column definitions and values (as a slice) into binary.
// This is the version called by InsertRow when values come from the VM stack.
//
// values must be in the same order as cols.
func (se *StorageEngine) SerializeRow(cols []types.ColumnDef, values []any) ([]byte, error) {
	if len(cols) != len(values) {
		return nil, fmt.Errorf("column count (%d) != value count (%d)", len(cols), len(values))
	}

	buf := new(bytes.Buffer)

	for i, col := range cols {
		b, err := ValueToBytes(values[i], col.Type)
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", col.Name, err)
		}
		buf.Write(b)
	}

	return buf.Bytes(), nil
}

// SerializeRowFromMap is your existing function — rename it to avoid collision.
// This is used when you have a Row struct (map-based) instead of a slice.
func (se *StorageEngine) SerializeRowFromMap(cols []types.ColumnDef, row types.Row) ([]byte, error) {
	buf := new(bytes.Buffer)

	for _, col := range cols {
		val, ok := row.Values[strings.ToLower(col.Name)]
		if !ok {
			return nil, fmt.Errorf("missing value for column %s", col.Name)
		}

		b, err := ValueToBytes(val, col.Type)
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", col.Name, err)
		}
		buf.Write(b)
	}

	return buf.Bytes(), nil
}

func ValueToBytes(val any, typ string) ([]byte, error) {
	buf := new(bytes.Buffer)

	switch strings.ToUpper(typ) {
	case "INT":
		i32, err := types.ToInt(val)
		if err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, i32); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil

	case "FLOAT":
		f32, err := types.ToFloat(val)
		if err != nil {
			return nil, err
		}
		bits := math.Float32bits(f32)
		if err := binary.Write(buf, binary.LittleEndian, bits); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil

	case "VARCHAR":
		s, err := types.ToString(val)
		if err != nil {
			return nil, err
		}
		if len(s) > 65535 {
			return nil, fmt.Errorf("varchar too long")
		}
		if err := binary.Write(buf, binary.LittleEndian, uint16(len(s))); err != nil {
			return nil, err
		}
		buf.Write([]byte(s))
		return buf.Bytes(), nil
	}

	return nil, fmt.Errorf("unsupported type %s", typ)
}

func BytesToValue(b []byte, typ string) (any, int, error) {
	switch strings.ToUpper(typ) {
	case "INT":
		if len(b) < 4 {
			return nil, 0, fmt.Errorf("not enough bytes for int")
		}
		i := int32(binary.LittleEndian.Uint32(b[:4]))
		return int(i), 4, nil

	case "FLOAT":
		if len(b) < 4 {
			return nil, 0, fmt.Errorf("not enough bytes for float")
		}
		bits := binary.LittleEndian.Uint32(b[:4])
		f := math.Float32frombits(bits)
		return float64(f), 4, nil

	case "VARCHAR":
		if len(b) < 2 {
			return nil, 0, fmt.Errorf("not enough bytes for varchar length")
		}
		strlen := binary.LittleEndian.Uint16(b[:2])
		if len(b) < int(2+strlen) {
			return nil, 0, fmt.Errorf("varchar length exceeds row size")
		}
		s := string(b[2 : 2+strlen])
		return s, int(2 + strlen), nil
	}

	return nil, 0, fmt.Errorf("unknown type %s", typ)
}

func (se *StorageEngine) DeserializeRow(row []byte, cols []types.ColumnDef) ([]any, error) {
	out := make([]any, len(cols))
	offset := 0

	for i, col := range cols {
		if offset >= len(row) {
			return nil, fmt.Errorf("not enough data for column %s (offset %d >= row length %d)",
				col.Name, offset, len(row))
		}

		val, read, err := BytesToValue(row[offset:], col.Type)
		if err != nil {
			return nil, fmt.Errorf("column %s at offset %d: %w", col.Name, offset, err)
		}
		out[i] = val
		offset += read
	}

	if offset != len(row) {
		return nil, fmt.Errorf("extra bytes at end of row: expected total %d bytes, got %d bytes (unused: %d bytes)",
			offset, len(row), len(row)-offset)
	}
	return out, nil
}

func (se *StorageEngine) SerializeRowPointer(ptr types.RowPointer) []byte {
	buf := make([]byte, 10) // FileID(4) + PageNumber(4) + SlotIndex(2)
	binary.LittleEndian.PutUint32(buf[0:4], ptr.FileID)
	binary.LittleEndian.PutUint32(buf[4:8], ptr.PageNumber)
	binary.LittleEndian.PutUint16(buf[8:10], ptr.SlotIndex)
	return buf
}

func (se *StorageEngine) DeserializeRowPointer(b []byte) (types.RowPointer, error) {
	if len(b) < 10 {
		return types.RowPointer{}, fmt.Errorf("row pointer buffer too short: %d", len(b))
	}
	return types.RowPointer{
		FileID:     binary.LittleEndian.Uint32(b[0:4]),
		PageNumber: binary.LittleEndian.Uint32(b[4:8]),
		SlotIndex:  binary.LittleEndian.Uint16(b[8:10]),
	}, nil
}
