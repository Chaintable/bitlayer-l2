package util

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"

	"github.com/ethereum/go-ethereum/rlp"
)

// EncodeToJsonGzip encodes data to JSON and compresses with gzip
func EncodeToJsonGzip(v any) ([]byte, error) {
	jsonData, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write(jsonData); err != nil {
		return nil, err
	}
	if err := gz.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// DecodeFromGzipJson decompresses gzip data and decodes from JSON
func DecodeFromGzipJson(data []byte, v any) error {
	reader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return err
	}
	defer reader.Close()

	jsonData, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	return json.Unmarshal(jsonData, v)
}

// EncodeToRlp encodes data to RLP
func EncodeToRlp(v any) ([]byte, error) {
	return rlp.EncodeToBytes(v)
}

// DecodeFromRlp decodes data from RLP
func DecodeFromRlp(data []byte, v any) error {
	return rlp.DecodeBytes(data, v)
}
