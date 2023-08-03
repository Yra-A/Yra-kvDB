package index

import (
  "bitcask-go/data"
  "github.com/google/btree"
  "sync"
)

// BTree 索引数据结构，调用 google 的轮子
// https://github.com/google/btree
// 写操作不是并发安全的，读操作并发安全
type BTree struct {
  tree *btree.BTree
  lock *sync.RWMutex
}

// NewBTree 初始化 BTree
func NewBTree() *BTree {
  return &BTree{
    tree: btree.New(32), // 传入 BTree 叶子结点个数
    lock: new(sync.RWMutex),
  }
}

func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
  item := &Item{key: key, pos: pos}
  bt.lock.Lock()
  defer bt.lock.Unlock()
  bt.tree.ReplaceOrInsert(item)
  return true
}

func (bt *BTree) Get(key []byte) *data.LogRecordPos {
  it := &Item{key: key}
  btreeItem := bt.tree.Get(it)
  if btreeItem == nil {
    return nil
  }
  return btreeItem.(*Item).pos
}

func (bt *BTree) Delete(key []byte) bool {
  item := &Item{key: key}
  bt.lock.Lock()
  defer bt.lock.Unlock()
  deletedItem := bt.tree.Delete(item)
  if deletedItem == nil {
    return false
  }
  return true
}
