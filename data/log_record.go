package data

import "encoding/binary"

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)

// LogRecordPos 数据内存索引，描述了数据在磁盘上的位置
type LogRecordPos struct {
	Fid    uint32 // 文件 id，表示数据存在了磁盘上哪个文件中
	Offset int64  // 偏移量，表示数据在文件中的位置
}

// Header【crc type keySize valueSize】
//
//	【4B + 1B + 5B    + 5B      】
//
// crc 和 type 也都是定长，keySize、valueSize 是变长的
// 变长 int32 的最大值为 5B
const maxLogRecordHeaderSize = 4 + 1 + binary.MaxVarintLen32*2

// LogRecord 追加写到磁盘数据文件的日志记录
// 先写磁盘数据文件，再更新内存索引
type LogRecord struct {
	Key   []byte
	Value []byte

	// 数据墓碑值，根据 bitcask 论文描述，数据删除后会进行标记，此处会标记为 LogRecordDeleted
	Type LogRecordType
}

type LogRecordHeader struct {
	crc        uint32        // crc 校验值
	recordType LogRecordType // LogRecord 的类型
	keySize    uint32        // key 的长度
	valueSize  uint32        // value 的长度
}

// EncodeLogRecord  TODO: 将 LogRecord 编码成字节数组
// 返回字节数组 和 长度
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	return nil, 0
}

// decodeLogRecordHeader 解码 Header
func decodeLogRecordHeader(buf []byte) (*LogRecordHeader, int64) {
	return nil, 0
}

// getLogRecordCRC TODO：根据 LogRecord 中的 key value 和 header 计算 CRC
func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	return 0
}
