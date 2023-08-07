package bitcask_go

type Options struct {
  // 数据库数据目录
  DirPath string

  // 数据文件大小（活跃文件写阈值）
  DataFileSize int64

  // 每次写入数据后是否进行持久化
  SyncWrites bool

  // 索引类型
  indexerType IndexerType
}

type IndexerType = int8

const (
  // BTreeIndex BTree 索引
  BTreeIndex IndexerType = iota + 1

  // ARTIndex 自适应基数树索引
  ARTIndex
)
