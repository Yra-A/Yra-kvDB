package index

import (
	"bitcask-go/data"
	"go.etcd.io/bbolt"
	"path/filepath"
)

const bptreeIndexFileName = "bptree-index"

var indexBucketName = []byte("bitcask-index")

// BPlusTree B+ 树索引
// 主要封装了 go.etcd.io/bbolt 库
type BPlusTree struct {
	tree *bbolt.DB
}

// NewBPlusTree 初始化 B+ 树索引
func NewBPlusTree(dirPath string, syncWrites bool) *BPlusTree {
	opts := bbolt.DefaultOptions
	opts.NoSync = !syncWrites
	bptree, err := bbolt.Open(filepath.Join(dirPath, bptreeIndexFileName), 0644, opts)
	if err != nil {
		panic("failed to open bptree")
	}

	// 创建对应的 bucket
	if err := bptree.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(indexBucketName)
		return err
	}); err != nil {
		panic("failed to create bucket in bptree")
	}

	return &BPlusTree{tree: bptree}
}

func (bpt *BPlusTree) Put(key []byte, pos *data.LogRecordPos) bool {
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		return bucket.Put(key, data.EncodeLogRecordPos(pos))
	}); err != nil {
		panic("failed to put value in bptree")
	}
	return true
}

func (bpt *BPlusTree) Get(key []byte) *data.LogRecordPos {
	var pos *data.LogRecordPos
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		value := bucket.Get(key)
		if len(value) != 0 {
			pos = data.DecodeLogRecordPos(value)
		}
		return nil
	}); err != nil {
		panic("failed to get value in bptree")
	}
	return pos
}

func (bpt *BPlusTree) Delete(key []byte) bool {
	var ok bool
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		if value := bucket.Get(key); len(value) != 0 {
			ok = true
			return bucket.Delete(key)
		}
		return nil
	}); err != nil {
		panic("failed to delete value in bptree")
	}
	return ok
}

func (bpt *BPlusTree) Size() int {
	var size int
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		size = bucket.Stats().KeyN
		return nil
	}); err != nil {
		panic("failed to get value in bptree")
	}
	return size
}

func (bpt *BPlusTree) Iterator(reverse bool) Iterator {
	return newBptreeIterator(bpt.tree, reverse)
}

func (bpt *BPlusTree) Close() error {
	return bpt.tree.Close()
}

// B+ 树迭代器
type bptreeIterator struct {
	tx       *bbolt.Tx
	cursor   *bbolt.Cursor
	reverse  bool
	curKey   []byte
	curValue []byte
}

func newBptreeIterator(tree *bbolt.DB, reverse bool) *bptreeIterator {
	// 手动开启一个事务
	tx, err := tree.Begin(false)
	if err != nil {
		panic("failed to begin a transaction")
	}
	bpi := &bptreeIterator{
		tx:      tx,
		cursor:  tx.Bucket(indexBucketName).Cursor(),
		reverse: reverse,
	}
	// 手动调用一下 rewind，避免 curKey curValue 为空
	bpi.Rewind()
	return bpi
}

func (bpi *bptreeIterator) Rewind() {
	if bpi.reverse {
		// 如果是 reverse，游标回到最后一项
		bpi.curKey, bpi.curValue = bpi.cursor.Last()
	} else {
		bpi.curKey, bpi.curValue = bpi.cursor.First()
	}
}

func (bpi *bptreeIterator) Seek(key []byte) {
	bpi.curKey, bpi.curValue = bpi.cursor.Seek(key)
}

func (bpi *bptreeIterator) Next() {
	if bpi.reverse {
		bpi.curKey, bpi.curValue = bpi.cursor.Prev()
	} else {
		bpi.curKey, bpi.curValue = bpi.cursor.Next()
	}
}

func (bpi *bptreeIterator) Valid() bool {
	return len(bpi.curKey) != 0
}

func (bpi *bptreeIterator) Key() []byte {
	return bpi.curKey
}

func (bpi *bptreeIterator) Value() *data.LogRecordPos {
	return data.DecodeLogRecordPos(bpi.curValue)
}

func (bpi *bptreeIterator) Close() {
	_ = bpi.tx.Rollback()
}
