package index

import (
  "bitcask-go/data"
  "github.com/stretchr/testify/assert"
  "testing"
)

func TestBTree_Put(t *testing.T) {
  bt := NewBTree()

  res1 := bt.Put(nil, &data.LogRecordPos{Fid: 114, Offset: 514})
  assert.True(t, res1)

  res2 := bt.Put([]byte("abc"), &data.LogRecordPos{Fid: 1, Offset: 10})
  assert.True(t, res2)

  res3 := bt.Put([]byte{111}, &data.LogRecordPos{Fid: 12, Offset: 130})
  assert.True(t, res3)
}

func TestBTree_Get(t *testing.T) {
  bt := NewBTree()

  res1 := bt.Put(nil, &data.LogRecordPos{Fid: 114, Offset: 514})
  assert.True(t, res1)
  res2 := bt.Put([]byte("abc"), &data.LogRecordPos{Fid: 1, Offset: 10})
  assert.True(t, res2)
  res3 := bt.Put([]byte{111}, &data.LogRecordPos{Fid: 12, Offset: 130})
  assert.True(t, res3)

  pos1_cmp := &data.LogRecordPos{
    Fid:    114,
    Offset: 514,
  }
  pos2_cmp := &data.LogRecordPos{
    Fid:    1,
    Offset: 10,
  }
  pos3_cmp := &data.LogRecordPos{
    Fid:    12,
    Offset: 130,
  }

  pos1 := bt.Get(nil)
  pos2 := bt.Get([]byte("abc"))
  pos3 := bt.Get([]byte{111})

  assert.Equal(t, pos1, pos1_cmp)
  assert.Equal(t, pos2, pos2_cmp)
  assert.Equal(t, pos3, pos3_cmp)
}

func TestBTree_Delete(t *testing.T) {
  bt := NewBTree()

  res1 := bt.Put(nil, &data.LogRecordPos{Fid: 114, Offset: 514})
  assert.True(t, res1)
  res2 := bt.Put([]byte("abc"), &data.LogRecordPos{Fid: 1, Offset: 10})
  assert.True(t, res2)
  res3 := bt.Put([]byte{111}, &data.LogRecordPos{Fid: 12, Offset: 130})
  assert.True(t, res3)

  res4 := bt.Delete(nil)
  assert.True(t, res4)
  res5 := bt.Delete([]byte("abc"))
  assert.True(t, res5)
  res6 := bt.Delete([]byte{111})
  assert.True(t, res6)

}
