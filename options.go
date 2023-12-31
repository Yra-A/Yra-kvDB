package bitcask_go

import "os"

type Options struct {
	// 数据库数据目录
	DirPath string

	// 数据文件大小（活跃文件写阈值）
	DataFileSize int64

	// 每次写入数据后是否进行持久化
	SyncWrites bool

	// 累计写到多少字节后进行持久化
	BytesPerSync uint

	// 索引类型
	IndexerType IndexerType

	// 启动时是否使用 MMap 加载数据
	MMapAtStartup bool

	// 数据文件进行 merge 的阈值
	DataFileMergeRatio float32
}

type IteratorOptions struct {
	// 遍历前缀为指定值的 key，默认为空
	Prefix []byte
	// 是否反向遍历，默认为 false，表示正向遍历
	Reverse bool
}

// WriteBatchOptions 批量写配置项
type WriteBatchOptions struct {
	// 一个批次中最大的数据量
	MaxBatchNum uint

	// 提交时是否 sync 持久化
	SyncWrites bool
}

type IndexerType = int8

const (
	// BTreeIndex BTree 索引
	BTreeIndex IndexerType = iota + 1

	// ARTIndex 自适应基数树索引
	ARTIndex

	// BPlusTreeIndex B+ 树索引，将索引存储到磁盘上
	BPlusTreeIndex
)

var DefaultOptions = Options{
	DirPath:            os.TempDir(),
	DataFileSize:       256 * 1024 * 1024, // 256 MB
	SyncWrites:         false,
	BytesPerSync:       0,
	IndexerType:        BTreeIndex,
	MMapAtStartup:      true,
	DataFileMergeRatio: 0.5,
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchNum: 10000,
	SyncWrites:  true,
}
