package index

import (
  "bitcask-go/data"
  "bytes"
  "github.com/google/btree"
)

// Indexer 内存中的索引接口，key 对应了数据在磁盘上的位置
type Indexer interface {
  // Put 向索引中存储信息，key 对应数据在磁盘上的位置
  Put(key []byte, pos *data.LogRecordPos) bool

  // Get 根据 key 取出对应的位置信息
  Get(key []byte) *data.LogRecordPos

  // Delete 根据 key 删除对应的位置信息
  Delete(key []byte) bool

  // Iterator 返回索引迭代器，根据参数 reverse 选择是否为反向迭代器
  Iterator(reverse bool) Iterator

  // Size 返回索引中存在多少条数据
  Size() int

  // Close 关闭索引
  Close() error
}

type IndexerType = int8

const (
  // BTreeIndex BTree 索引
  BTreeIndex IndexerType = iota + 1

  // ARTIndex 自适应基数树索引
  ARTIndex

  // BPTree B+ 树索引
  BPTree
)

// NewIndexer 初始化Indexer
func NewIndexer(typ IndexerType, dirPath string, sync bool) Indexer {
  switch typ {
  case BTreeIndex:
    return NewBTree()
  case ARTIndex:
    return NewART()
  case BPTree:
    return NewBPlusTree(dirPath, sync)
  default:
    panic("unsupported index type")
  }
}

// Item 实现 BTree 的 Item 接口
// 需要实现 Less 方法，自定义排序
type Item struct {
  key []byte
  pos *data.LogRecordPos
}

func (ai *Item) Less(bi btree.Item) bool {
  // ai.key 小于 bi.(*Item).key 返回 true
  return bytes.Compare(ai.key, bi.(*Item).key) == -1
}

// Iterator 通用索引迭代器
type Iterator interface {
  // Rewind 重新回到迭代器的起点，即第一个数据
  Rewind()

  // Seek 根据传入的 key 查找到第一个大于（或小于）等于的目标 key，根据从这个 key 开始遍历
  Seek(key []byte)

  // Next 跳转到下一个 key
  Next()

  // Valid 是否有效，即是否已经遍历完了所有的 key，用于退出遍历
  Valid() bool

  // Key 当前遍历位置的 Key 数据
  Key() []byte

  // Value 当前遍历位置的 Value 数据
  Value() *data.LogRecordPos

  // Close 关闭迭代器，释放相应资源
  Close()
}
