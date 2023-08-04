package bitcask_go

import (
  "bitcask-go/data"
  "bitcask-go/index"
  "sync"
)

// DB bitcask 存储引擎实例
type DB struct {
  mu         *sync.RWMutex
  options    Options                   // 数据库配置项
  activeFile *data.DataFile            // 当前活跃数据文件，可以写
  olderFile  map[uint32]*data.DataFile // 旧的数据文件，只能读; 文件 id -> 数据文件
  index      index.Indexer             // 内存索引
}

// 根据数据文件日志记录来实际进行往活跃文件中追加写
// 写完后返回索引位置，用于更新索引
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
  db.mu.Lock()
  defer db.mu.Unlock()

  // 判断当前活跃文件是否存在，数据库刚初始化的时候没有任何数据文件存在，因此要新增一个文件
  if db.activeFile == nil {
    if err := db.setAvtiveDataFile(); err != nil {
      return nil, err
    }
  }

  // 已经拥有活跃文件了，将传入的 logRecord 追加写入
  // 写入前还需要编码成字节数组
  encodedLogRecord, size := data.EncodeLogRecord(logRecord)

  // 根据 bitcask 论文描述，如果当前活跃文件写会到达阈值，就要关闭当前活跃文件，重新打开一个

  // 如果写入数据达到了活跃文件写阈值
  if db.activeFile.WriteOff+size > db.options.DataFileSize {
    // TODO: 【Optimization】如果写入数据的大小超过了多个文件的阈值，就需要打开多个新文件

    // 先持久化数据文件，保证数据已持久化到磁盘上
    if err := db.activeFile.Sync(); err != nil {
      return nil, err
    }

    // 将当前活跃文件转换成旧数据文件
    db.olderFile[db.activeFile.FileId] = db.activeFile

    // 打开并设置新的活跃数据文件
    if err := db.setAvtiveDataFile(); err != nil {
      return nil, err
    }
  }

  // 写入操作
  writeOff := db.activeFile.WriteOff
  if err := db.activeFile.Write(encodedLogRecord); err != nil {
    return nil, err
  }

  // TODO: 增加策略，例如 1s 后再持久化
  // 根据用户配置项决定写入后是否进行持久化
  if db.options.SyncWrites {
    if err := db.activeFile.Sync(); err != nil {
      return nil, err
    }
  }

  // 构造索引位置并返回
  pos := &data.LogRecordPos{
    Fid:    db.activeFile.FileId,
    Offset: writeOff,
  }
  return pos, nil
}

// 设置当前活跃文件 需要持有互斥锁
func (db *DB) setAvtiveDataFile() error {
  var initialFileId uint32 = 0

  // 已经有活跃文件了，则新的活跃文件 id 是当前活跃文件 id + 1（递增）
  if db.activeFile != nil {
    initialFileId = db.activeFile.FileId + 1
  }

  // 根据配置项中传递过来的目录，在该目录下打开新的数据文件，并将其设置会新的活跃文件
  dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileId)
  if err != nil {
    return err
  }
  db.activeFile = dataFile
  return nil
}

// Put 数据库写操作，往数据库中写入 K-V 数据，保证 key 非空
// 先写磁盘文件，在更新内存索引
func (db *DB) Put(key []byte, value []byte) error {
  // 判断 key 是否有效
  if len(key) == 0 {
    return ErrKeyIsEmpty
  }

  // 构造 LogReCord 结构体
  logRecord := &data.LogRecord{
    Key:   key,
    Value: value,
    Type:  data.LogRecordNormal,
  }

  // 根据构造出来的日志记录，追加写入数据文件，并得到索引位置
  pos, err := db.appendLogRecord(logRecord)
  if err != nil {
    return err
  }

  // 更新内存索引
  if ok := db.index.Put(key, pos); !ok {
    return ErrIndexUpdateFailed
  }
  return nil
}

// Get 数据库读操作，根据 key，读取 Value。需要获取读锁
// 先根据 key，从内存中获取索引信息，得到数据存放的文件 id 以及偏移量，并根据 id 和 偏移量获取数据
// 返回字节数组
func (db *DB) Get(key []byte) ([]byte, error) {
  db.mu.RLock()
  defer db.mu.RUnlock()

  // 判断 key 是否非空
  if len(key) == 0 {
    return nil, ErrKeyIsEmpty
  }

  // 从内存数据结构中取出 key 对应的索引信息
  logRecordPos := db.index.Get(key)
  // 该 key 不存在
  if logRecordPos == nil {
    return nil, ErrKeyNotFound
  }

  // 根据文件 id 找到对应数据文件
  // 先看是不是活跃文件，否则在旧文件中获取
  var dataFile *data.DataFile
  if db.activeFile.FileId == logRecordPos.Fid {
    dataFile = db.activeFile
  } else {
    dataFile = db.olderFile[logRecordPos.Fid]
  }

  // 根据文件 id，未找到该文件
  if dataFile == nil {
    return nil, ErrDataFileNotFound
  }

  // 找到了数据文件后，根据偏移量读取文件
  logRecord, err := dataFile.ReadLogRecord(logRecordPos.Offset)
  if err != nil {
    return nil, err
  }

  // 该数据已被删除
  if logRecord.Type == data.LogRecordDeleted {
    return nil, ErrKeyNotFound
  }

  return logRecord.Value, nil
}
