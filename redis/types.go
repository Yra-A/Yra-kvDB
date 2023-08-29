package redis

import (
  bitcask "bitcask-go"
  "encoding/binary"
  "errors"
  "time"
)

var (
  ErrWrongTypeOperation = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
)

type redisDataType = byte

const (
  String redisDataType = iota
  Hash
  Set
  List
  ZSet
)

// RedisDataStructure Redis 数据结构服务
type RedisDataStructure struct {
  db *bitcask.DB
}

// NewRedisDataStructure 初始化 Redis 数据结构服务
func NewRedisDataStructure(options bitcask.Options) (*RedisDataStructure, error) {
  db, err := bitcask.Open(options)
  if err != nil {
    return nil, err
  }
  return &RedisDataStructure{db: db}, nil
}

// ==================== String 数据结构 ====================

func (rds *RedisDataStructure) Set(key []byte, ttl time.Duration, value []byte) error {
  if value == nil {
    return nil
  }

  // 编码 value : type + expire + payload
  buf := make([]byte, binary.MaxVarintLen64+1)
  buf[0] = String
  var index = 1
  var expire int64
  if ttl != 0 {
    expire = time.Now().Add(ttl).UnixNano()
  }
  index += binary.PutVarint(buf[index:], expire)
  encValue := make([]byte, index+len(value))
  copy(encValue[:index], buf[:index])
  copy(encValue[index:], value)

  // 调用存储接口写入数据
  return rds.db.Put(key, encValue)
}

func (rds *RedisDataStructure) Get(key []byte) ([]byte, error) {
  encValue, err := rds.db.Get(key)
  if err != nil {
    return nil, err
  }

  // 解码部分
  dataType := encValue[0]
  if dataType != String {
    return nil, ErrWrongTypeOperation
  }

  var index = 1
  expire, n := binary.Varint(encValue[index:])
  index += n
  // 判断是否过期
  if expire > 0 && expire <= time.Now().UnixNano() {
    return nil, nil
  }
  return encValue[index:], nil
}

// ==================== Hash 数据结构 ====================

// HSet 返回操作结果和错误，只有当数据设置之前不存在才返回 true
func (rds *RedisDataStructure) HSet(key, field, value []byte) (bool, error) {
  // 查找元数据
  meta, err := rds.findMetadata(key, Hash)
  if err != nil {
    return false, err
  }

  // 构造 Hash 数据部分的 key
  hk := &hashInternalKey{
    key:     key,
    version: meta.version,
    field:   field,
  }
  encKey := hk.encode()

  // 根据数据 key 去查找是否存在
  var exist = true
  if _, err := rds.db.Get(encKey); err == bitcask.ErrKeyNotFound {
    exist = false
  }

  wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
  // 不存在则更新元数据
  if !exist {
    meta.size++ // 数据个数 + 1
    _ = wb.Put(key, meta.encode())
  }
  _ = wb.Put(encKey, value)
  if err = wb.Commit(); err != nil {
    return false, err
  }
  return !exist, nil
}

func (rds *RedisDataStructure) HGet(key, field []byte) ([]byte, error) {
  meta, err := rds.findMetadata(key, Hash)
  if err != nil {
    return nil, err
  }
  if meta.size == 0 {
    return nil, nil
  }

  hk := &hashInternalKey{
    key:     key,
    version: meta.version,
    field:   field,
  }

  return rds.db.Get(hk.encode())
}

func (rds *RedisDataStructure) HDel(key, field []byte) (bool, error) {
  meta, err := rds.findMetadata(key, Hash)
  if err != nil {
    return false, err
  }
  if meta.size == 0 {
    return false, nil
  }

  hk := &hashInternalKey{
    key:     key,
    version: meta.version,
    field:   field,
  }
  encKey := hk.encode()

  // 先查看是否存在
  var exist = true
  if _, err = rds.db.Get(encKey); err == bitcask.ErrKeyNotFound {
    exist = false
  }

  if exist {
    wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
    meta.size--
    _ = wb.Put(key, meta.encode()) // 更新元数据
    _ = wb.Delete(encKey)
    if err = wb.Commit(); err != nil {
      return false, err
    }
  }

  return exist, nil
}

// ==================== 通用方法 ====================
func (rds *RedisDataStructure) findMetadata(key []byte, dataType redisDataType) (*metadata, error) {
  metaBuf, err := rds.db.Get(key)
  if err != nil && err != bitcask.ErrKeyNotFound {
    return nil, err
  }

  var meta *metadata
  var exist = true
  if err == bitcask.ErrKeyNotFound {
    exist = false
  } else {
    meta = decodeMetadata(metaBuf)
    // 判断数据类型
    if meta.dataType != dataType {
      return nil, ErrWrongTypeOperation
    }
    // 判断过期时间
    if meta.expire > 0 && meta.expire <= time.Now().UnixNano() {
      exist = false
    }
  }

  if !exist {
    meta = &metadata{
      dataType: dataType,
      expire:   0,
      version:  time.Now().UnixNano(),
      size:     0,
    }
    if dataType == List {
      meta.head = initialListMark
      meta.tail = initialListMark
    }
  }
  return meta, nil
}

// ==================== Set 数据结构 ====================

// SAdd 返回是否添加成功 和 错误
func (rds *RedisDataStructure) SAdd(key, member []byte) (bool, error) {
  // 查找元数据
  meta, err := rds.findMetadata(key, Set)
  if err != nil {
    return false, err
  }

  sk := &setInternalKey{
    key:     key,
    version: meta.version,
    member:  member,
  }

  var ok bool
  if _, err := rds.db.Get(sk.encode()); err == bitcask.ErrKeyNotFound {
    // 不存在的话则更新
    wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
    meta.size++
    _ = wb.Put(key, meta.encode())
    _ = wb.Put(sk.encode(), nil)
    if err = wb.Commit(); err != nil {
      return false, err
    }
    ok = true
  }
  return ok, nil
}

func (rds *RedisDataStructure) SIsMember(key, member []byte) (bool, error) {
  // 查找元数据
  meta, err := rds.findMetadata(key, Set)
  if err != nil {
    return false, err
  }
  if meta.size == 0 {
    // 无数据可以直接返回 false
    return false, nil
  }

  // 构造一个数据部分的 key
  sk := &setInternalKey{
    key:     key,
    version: meta.version,
    member:  member,
  }

  _, err = rds.db.Get(sk.encode())
  if err != nil && err != bitcask.ErrKeyNotFound {
    return false, err
  }
  if err == bitcask.ErrKeyNotFound {
    return false, nil
  }
  return true, nil
}

func (rds *RedisDataStructure) SRem(key, member []byte) (bool, error) {
  meta, err := rds.findMetadata(key, Set)
  if err != nil {
    return false, err
  }
  if meta.size == 0 {
    return false, nil
  }

  // 构造一个数据部分的 key
  sk := &setInternalKey{
    key:     key,
    version: meta.version,
    member:  member,
  }

  if _, err = rds.db.Get(sk.encode()); err == bitcask.ErrKeyNotFound {
    return false, nil
  }

  // 更新
  wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
  meta.size--
  _ = wb.Put(key, meta.encode())
  _ = wb.Delete(sk.encode())
  if err = wb.Commit(); err != nil {
    return false, err
  }
  return true, nil
}
