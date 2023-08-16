package data

import (
	"encoding/binary"
	"hash/crc32"
)

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
	LogRecordTxnFinished
)

// LogRecordPos 数据内存索引，描述了数据在磁盘上的位置
type LogRecordPos struct {
	Fid    uint32 // 文件 id，表示数据存在了磁盘上哪个文件中
	Offset int64  // 偏移量，表示数据在文件中的位置
}

// TransactionRecord 暂存事务相关的数据
type TransactionRecord struct {
	Record *LogRecord
	Pos    *LogRecordPos
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

// EncodeLogRecord 将 LogRecord 编码成字节数组
// 返回字节数组 和 长度
// +-------------+-------------+-------------+-------------+-------------+-------------+
// | crc 校验值   +   type 类型  +  key size   + value size  +     key     +    value    +
// +-------------+-------------+-------------+-------------+-------------+-------------+
//
//	4 bytes       1 byte      变长（最大 5）  变长（最大 5）      变长           变长
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	// 初始化一个 header 部分的字节数组
	header := make([]byte, maxLogRecordHeaderSize)

	// 第五个字节存储 Type
	header[4] = logRecord.Type
	var index = 5

	// 接下来写入 key size 和 value size
	// 使用变长类型，可以节省空间
	index += binary.PutVarint(header[index:], int64(len(logRecord.Key)))
	index += binary.PutVarint(header[index:], int64(len(logRecord.Value)))

	// 得到实际的 logRecord 编码成字节数组的长度
	size := index + len(logRecord.Key) + len(logRecord.Value)
	encBytes := make([]byte, size)

	// 将 header 拷贝到 encBytes
	copy(encBytes[:index], header[:index])

	// 将 key 和 value 数据直接拷贝到字节数组，key 和 value 本身就是 []byte
	copy(encBytes[index:], logRecord.Key)
	copy(encBytes[index+len(logRecord.Key):], logRecord.Value)

	// 对字节数组计算 crc，并存入前四个字节
	crc := crc32.ChecksumIEEE(encBytes[4:])
	binary.LittleEndian.PutUint32(encBytes[:4], crc)

	// for test
	//fmt.Printf("header length : %d, crc : %d\n", index, crc)

	return encBytes, int64(size)
}

// decodeLogRecordHeader 解码 Header
func decodeLogRecordHeader(buf []byte) (*LogRecordHeader, int64) {
	if len(buf) <= 4 {
		return nil, 0
	}

	header := &LogRecordHeader{
		crc:        binary.LittleEndian.Uint32(buf[:4]),
		recordType: buf[4],
	}

	var index = 5

	// 取 key size
	keySize, n := binary.Varint(buf[index:])
	header.keySize = uint32(keySize)
	index += n

	// 取 value size
	valueSize, n := binary.Varint(buf[index:])
	header.valueSize = uint32(valueSize)
	index += n

	return header, int64(index)
}

// getLogRecordCRC 根据 LogRecord 中的 key value 和 header 计算 CRC
func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	if lr == nil {
		return 0
	}

	crc := crc32.ChecksumIEEE(header[:])
	crc = crc32.Update(crc, crc32.IEEETable, lr.Key)
	crc = crc32.Update(crc, crc32.IEEETable, lr.Value)

	return crc
}
