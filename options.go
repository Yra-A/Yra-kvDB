package bitcask_go

type Options struct {
  // 数据库数据目录
  DirPath string

  // 数据文件大小（活跃文件写阈值）
  DataFileSize int64

  // 每次写入数据后是否进行持久化
  SyncWrites bool
}
