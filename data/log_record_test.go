package data

import (
  "github.com/stretchr/testify/assert"
  "hash/crc32"
  "testing"
)

func TestEncodeLogRecord(t *testing.T) {
  // 正常情况
  rec1 := &LogRecord{
    Key:   []byte("name"),
    Value: []byte("Yra"),
    Type:  LogRecordNormal,
  }
  res1, n1 := EncodeLogRecord(rec1)
  t.Log(res1)
  assert.NotNil(t, res1)
  assert.Greater(t, n1, int64(5))

  // value 为空的情况
  rec2 := &LogRecord{
    Key:  []byte("name"),
    Type: LogRecordNormal,
  }
  res2, n2 := EncodeLogRecord(rec2)
  assert.NotNil(t, res2)
  assert.Greater(t, n2, int64(5))

  // type 为 deleted 的情况
  rec3 := &LogRecord{
    Key:   []byte("name"),
    Value: []byte("Yra"),
    Type:  LogRecordDeleted,
  }
  res3, n3 := EncodeLogRecord(rec3)
  t.Log(res3)
  assert.NotNil(t, res3)
  assert.Greater(t, n3, int64(5))
}

func TestDecodeLogRecordHeader(t *testing.T) {
  // header length : 7, crc : 2945958886
  // log_record_test.go:16: [230 195 151 175 0 8 6 110 97 109 101 89 114 97]
  headerBuf1 := []byte{230, 195, 151, 175, 0, 8, 6}
  h1, size1 := decodeLogRecordHeader(headerBuf1)
  assert.NotNil(t, h1)
  assert.Equal(t, int64(7), size1)
  assert.Equal(t, uint32(2945958886), h1.crc)
  assert.Equal(t, LogRecordNormal, h1.recordType)
  assert.Equal(t, uint32(4), h1.keySize)   // "name"
  assert.Equal(t, uint32(3), h1.valueSize) // "Yra"

  //header length : 7, crc : 240712713
  //log_record_test.go:27: [9 252 88 14 0 8 0 110 97 109 101]
  headerBuf2 := []byte{9, 252, 88, 14, 0, 8, 0}
  h2, size2 := decodeLogRecordHeader(headerBuf2)
  assert.NotNil(t, h2)
  assert.Equal(t, int64(7), size2)
  assert.Equal(t, uint32(240712713), h2.crc)
  assert.Equal(t, LogRecordNormal, h2.recordType)
  assert.Equal(t, uint32(4), h2.keySize)   // "name"
  assert.Equal(t, uint32(0), h2.valueSize) // ""

  //header length : 7, crc : 1079355608
  //log_record_test.go:38: [216 168 85 64 1 8 6 110 97 109 101 89 114 97]
  headerBuf3 := []byte{216, 168, 85, 64, 1, 8, 6}
  h3, size3 := decodeLogRecordHeader(headerBuf3)
  assert.NotNil(t, h3)
  assert.Equal(t, int64(7), size3)
  assert.Equal(t, uint32(1079355608), h3.crc)
  assert.Equal(t, LogRecordDeleted, h3.recordType)
  assert.Equal(t, uint32(4), h3.keySize)   // "name"
  assert.Equal(t, uint32(3), h3.valueSize) // "Yra"
}

func TestGetlogRecordCRC(t *testing.T) {
  rec1 := &LogRecord{
    Key:   []byte("name"),
    Value: []byte("Yra"),
    Type:  LogRecordNormal,
  }
  headerBuf1 := []byte{230, 195, 151, 175, 0, 8, 6}
  crc := getLogRecordCRC(rec1, headerBuf1[crc32.Size:])
  assert.Equal(t, crc, uint32(2945958886))

  rec2 := &LogRecord{
    Key:   []byte("name"),
    Value: []byte(""),
    Type:  LogRecordNormal,
  }
  headerBuf2 := []byte{9, 252, 88, 14, 0, 8, 0}
  crc = getLogRecordCRC(rec2, headerBuf2[crc32.Size:])
  assert.Equal(t, crc, uint32(240712713))

  rec3 := &LogRecord{
    Key:   []byte("name"),
    Value: []byte("Yra"),
    Type:  LogRecordDeleted,
  }
  headerBuf3 := []byte{216, 168, 85, 64, 1, 8, 6}
  crc = getLogRecordCRC(rec3, headerBuf3[crc32.Size:])
  assert.Equal(t, crc, uint32(1079355608))
}
