package data

// LogRecordPos 数据内存索引，描述了数据在磁盘上的位置
type LogRecordPos struct {
  Fid    uint32 // 文件 id，表示数据存在了磁盘上哪个文件中
  Offset int64  // 偏移量，表示数据在文件中的位置
}
