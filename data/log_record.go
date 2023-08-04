package data

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

// LogRecord 追加写到磁盘数据文件的日志记录
// 先写磁盘数据文件，再更新内存索引
type LogRecord struct {
  Key   []byte
  Value []byte

  // 数据墓碑值，根据 bitcask 论文描述，数据删除后会进行标记，此处会标记为 LogRecordDeleted
  Type LogRecordType
}

// EncodeLogRecord  TODO: 将 LogRecord 编码成字节数组
// 返回字节数组 和 长度
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
  return nil, 0
}
