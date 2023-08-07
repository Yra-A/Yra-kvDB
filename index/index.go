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
}

type IndexerType = int8

const (
  // BTreeIndex BTree 索引
  BTreeIndex IndexerType = iota + 1

  // ARTIndex 自适应基数树索引
  ARTIndex
)

// NewIndexer 初始化Indexer
func NewIndexer(typ IndexerType) Indexer {
  switch typ {
  case BTreeIndex:
    return NewBTree()
  case ARTIndex:
    // TODO: ART 索引
    return nil
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
