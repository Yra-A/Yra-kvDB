package bitcask_go

import (
  "bitcask-go/data"
  "bitcask-go/fio"
  "bitcask-go/index"
  "bitcask-go/utils"
  "errors"
  "fmt"
  "github.com/gofrs/flock"
  "io"
  "os"
  "path/filepath"
  "sort"
  "strconv"
  "strings"
  "sync"
)

const (
  seqNoKey     = "seq.no"
  fileLockName = "flock"
)

// DB bitcask 存储引擎实例
type DB struct {
  fileIds        []int // 文件 id 列表，用于有序遍历，只能在加载索引时使用
  mu             *sync.RWMutex
  options        Options                   // 数据库配置项
  activeFile     *data.DataFile            // 当前活跃数据文件，可以写
  olderFiles     map[uint32]*data.DataFile // 旧的数据文件，只能读; 文件 id -> 数据文件
  index          index.Indexer             // 内存索引
  seqNo          uint64                    // 当前最新的事务序列号，全局递增
  isMerging      bool                      // 标识当前是否正在进行 merge
  seqNoFileExist bool                      // 存储事务序列号的文件是否存在
  isInitial      bool                      // 判断是否是第一次初始化此数据目录
  fileLock       *flock.Flock              // 文件锁保证多进程间的互斥
  bytesWrite     uint                      // 累计写了多少个字节
  reclaimSize    int64                     // 表示有多少数据是无效的
}

// Stat 存储引擎统计数据
type Stat struct {
  KeyNum          uint  // key 的总数量
  DataFileNum     uint  // 磁盘上数据文件的数量
  ReclaimableSize int64 // 可以通过 merge 回收的数据量，以字节为单位
  DiskSize        int64 // 数据目录所占磁盘空间的大小
}

// Stat 返回数据库的相关统计信息
func (db *DB) Stat() *Stat {
  db.mu.RLock()
  defer db.mu.RUnlock()
  var dataFiles = uint(len(db.olderFiles))
  if db.activeFile != nil {
    dataFiles++
  }

  dirSize, err := utils.DirSize(db.options.DirPath)
  if err != nil {
    panic(fmt.Sprintf("failed to get dir size : %v", err))
  }
  return &Stat{
    KeyNum:          uint(db.index.Size()),
    DataFileNum:     dataFiles,
    ReclaimableSize: db.reclaimSize,
    DiskSize:        dirSize,
  }
}

// Open 打开存储引擎实例
func Open(options Options) (*DB, error) {
  // 校验用户传入的配置项
  if err := checkOptions(options); err != nil {
    return nil, err
  }

  var isInitial bool
  // 判断数据目录是否存在，不存在需要创建这个目录
  if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
    isInitial = true
    if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
      return nil, err
    }
  }

  // 判断当前数据目录是否正在使用
  fileLock := flock.New(filepath.Join(options.DirPath, fileLockName))
  hold, err := fileLock.TryLock()
  if err != nil {
    return nil, err
  }
  if !hold {
    return nil, ErrDatabaseIsUsing
  }

  entries, err := os.ReadDir(options.DirPath)
  if err != nil {
    return nil, err
  }
  if len(entries) == 0 {
    // 目录即使存在但目录下为空，也要设置 isInitial 为 true
    isInitial = true
  }

  // 初始化 DB 实例结构体
  db := &DB{
    mu:         new(sync.RWMutex),
    options:    options,
    olderFiles: make(map[uint32]*data.DataFile),
    index:      index.NewIndexer(options.IndexerType, options.DirPath, options.SyncWrites),
    isInitial:  isInitial,
    fileLock:   fileLock,
  }

  // 加载 merge 数据目录
  if err := db.loadMergeFiles(); err != nil {
    return nil, err
  }

  // 加载数据文件
  if err := db.loadDataFiles(); err != nil {
    return nil, err
  }

  // B+ 树索引不需要从数据文件中加载索引
  if options.IndexerType != BPlusTreeIndex {
    // 从 hint 索引文件中加载索引
    if err := db.loadIndexFromHintFile(); err != nil {
      return nil, err
    }
    // 从数据文件中加载 LogRecord 并更新索引
    if err := db.loadIndexFromDataFiles(); err != nil {
      return nil, err
    }

    // 重置 IO 类型为标准文件 IO
    if db.options.MMapAtStartup {
      if err := db.resetIoType(); err != nil {
        return nil, err
      }
    }
  }

  // 取出当前事务序列号
  if options.IndexerType == BPlusTreeIndex {
    if err := db.loadSeqNo(); err != nil {
      return nil, err
    }
    if db.activeFile != nil {
      size, err := db.activeFile.IoManager.Size()
      if err != nil {
        return nil, err
      }
      db.activeFile.WriteOff = size
    }
  }

  return db, nil
}

// Close 关闭数据库
func (db *DB) Close() error {
  defer func() {
    if err := db.fileLock.Unlock(); err != nil {
      panic(fmt.Sprintf("failed to unlock the directory, %v", err))
    }
  }()
  if db.activeFile == nil {
    return nil
  }
  db.mu.Lock()
  defer db.mu.Unlock()

  // 关闭索引
  if err := db.index.Close(); err != nil {
    return err
  }

  // 保存当前事务序列号
  seqNoFile, err := data.OpenSeqNoFile(db.options.DirPath)
  if err != nil {
    return err
  }
  record := &data.LogRecord{
    Key:   []byte(seqNoKey),
    Value: []byte(strconv.FormatUint(db.seqNo, 10)),
  }
  encRecord, _ := data.EncodeLogRecord(record)
  if err := seqNoFile.Write(encRecord); err != nil {
    return err
  }
  if err := seqNoFile.Sync(); err != nil {
    return err
  }

  // 关闭当前活跃文件
  if err := db.activeFile.Close(); err != nil {
    return err
  }

  // 关闭当前旧文件
  for _, file := range db.olderFiles {
    if err := file.Close(); err != nil {
      return err
    }
  }
  return nil
}

// Sync 持久化数据文件
func (db *DB) Sync() error {
  if db.activeFile == nil {
    return nil
  }
  db.mu.Lock()
  defer db.mu.Unlock()
  return db.activeFile.Sync()
}

// ListKeys 获取数据库中的所有 key
func (db *DB) ListKeys() [][]byte {
  iterator := db.index.Iterator(false)
  defer iterator.Close() // 关闭，防止读写互斥阻塞
  keys := make([][]byte, db.index.Size())
  var idx int
  for iterator.Rewind(); iterator.Valid(); iterator.Next() {
    keys[idx] = iterator.Key()
    idx++
  }
  return keys
}

// Fold 获取所有数据，并执行用户指定操作
func (db *DB) Fold(fn func(key []byte, value []byte) bool) error {
  db.mu.RLock()
  defer db.mu.RUnlock()

  iterator := db.index.Iterator(false)
  defer iterator.Close() // 关闭，防止读写互斥阻塞
  for iterator.Rewind(); iterator.Valid(); iterator.Next() {
    value, err := db.getValueByPosition(iterator.Value())
    if err != nil {
      return err
    }
    if !fn(iterator.Key(), value) {
      break
    }
  }
  return nil
}

// getValueByPosition 根据索引位置信息获取对应的 value
func (db *DB) getValueByPosition(logRecordPos *data.LogRecordPos) ([]byte, error) {
  // 根据文件 id 找到对应数据文件
  // 先看是不是活跃文件，否则在旧文件中获取
  var dataFile *data.DataFile
  if db.activeFile.FileId == logRecordPos.Fid {
    dataFile = db.activeFile
  } else {
    dataFile = db.olderFiles[logRecordPos.Fid]
  }

  // 根据文件 id，未找到该文件
  if dataFile == nil {
    return nil, ErrDataFileNotFound
  }

  // 找到了数据文件后，根据偏移量读取文件
  logRecord, _, err := dataFile.ReadLogRecord(logRecordPos.Offset)
  if err != nil {
    return nil, err
  }

  // 该数据已被删除
  if logRecord.Type == data.LogRecordDeleted {
    return nil, ErrKeyNotFound
  }

  return logRecord.Value, nil
}

func (db *DB) appendLogRecordWithLock(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
  db.mu.Lock()
  defer db.mu.Unlock()
  return db.appendLogRecord(logRecord)
}

// 将 LogRecord 追加写入活跃文件中
// 写完后返回索引位置，用于更新索引
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
  // 判断当前活跃文件是否存在，数据库刚初始化的时候没有任何数据文件存在，因此要新增一个文件
  if db.activeFile == nil {
    if err := db.setActiveDataFile(); err != nil {
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
    db.olderFiles[db.activeFile.FileId] = db.activeFile

    // 打开并设置新的活跃数据文件
    if err := db.setActiveDataFile(); err != nil {
      return nil, err
    }
  }

  // 写入操作
  writeOff := db.activeFile.WriteOff
  if err := db.activeFile.Write(encodedLogRecord); err != nil {
    return nil, err
  }

  db.bytesWrite += uint(size)
  // 根据用户配置项决定写入后是否进行持久化
  var needSync = db.options.SyncWrites
  if !needSync && db.options.BytesPerSync > 0 && db.bytesWrite >= db.options.BytesPerSync {
    needSync = true
  }
  if needSync {
    if err := db.activeFile.Sync(); err != nil {
      return nil, err
    }
    // 清空累计值
    if db.bytesWrite > 0 {
      db.bytesWrite = 0
    }
  }

  // 构造索引位置并返回
  pos := &data.LogRecordPos{
    Fid:    db.activeFile.FileId,
    Offset: writeOff,
    Size:   uint32(size),
  }
  return pos, nil
}

// 设置当前活跃文件 需要持有互斥锁
func (db *DB) setActiveDataFile() error {
  var initialFileId uint32 = 0

  // 已经有活跃文件了，则新的活跃文件 id 是当前活跃文件 id + 1（递增）
  if db.activeFile != nil {
    initialFileId = db.activeFile.FileId + 1
  }

  // 根据配置项中传递过来的目录，在该目录下打开新的数据文件，并将其设置会新的活跃文件
  dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileId, fio.StandardFIO)
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
    Key:   logRecordKeyWithSeq(key, nonTransactionSeqNo),
    Value: value,
    Type:  data.LogRecordNormal,
  }

  // 将构造出来的日志记录，追加写入数据文件，并得到索引位置
  pos, err := db.appendLogRecordWithLock(logRecord)
  if err != nil {
    return err
  }

  // 更新内存索引
  if oldPos := db.index.Put(key, pos); oldPos != nil {
    db.reclaimSize += int64(oldPos.Size)
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
  //  从数据文件中获取 value
  return db.getValueByPosition(logRecordPos)
}

// Delete 数据库删除操作，根据 key 删除数据
func (db *DB) Delete(key []byte) error {
  // 判断 key 有效性
  if len(key) == 0 {
    return ErrKeyIsEmpty
  }

  // 先检查 key 是否存在，若不存在直接返回
  if pos := db.index.Get(key); pos == nil {
    return nil
  }

  // 构造 LogRecord，并标记已删除，并将其写入数据文件
  logRecord := &data.LogRecord{
    Key:  logRecordKeyWithSeq(key, nonTransactionSeqNo),
    Type: data.LogRecordDeleted,
  }
  pos, err := db.appendLogRecordWithLock(logRecord)
  if err != nil {
    return err
  }
  db.reclaimSize += int64(pos.Size)
  // 并在内存索引中删除对应的 key
  oldPos, ok := db.index.Delete(key)
  if !ok {
    return ErrIndexUpdateFailed
  }
  if oldPos != nil {
    db.reclaimSize += int64(oldPos.Size)
  }
  return nil
}

// checkOptions 校验配置项
func checkOptions(options Options) error {
  if options.DirPath == "" {
    return errors.New("database dir path is empty")
  }
  if options.DataFileSize <= 0 {
    return errors.New("database data file size must be greater than 0")
  }
  if options.DataFileMergeRatio < 0 || options.DataFileMergeRatio > 1 {
    return errors.New("invalid merge ratio, must between 0 and 1")
  }
  return nil
}

// loadDataFiles 加载数据文件
func (db *DB) loadDataFiles() error {
  dirEntries, err := os.ReadDir(db.options.DirPath)
  if err != nil {
    return nil
  }

  var fileIds []int
  // 遍历目录下的文件，找到所有以【.data】结尾的文件
  for _, entry := range dirEntries {
    if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
      // 按点分割一下名字，例如 0001.data -> { "0001", "data" }
      splitedName := strings.Split(entry.Name(), ".")
      fileId, err := strconv.Atoi(splitedName[0])
      // 数据目录可能已损坏
      if err != nil {
        return ErrDataDirectoryCorrupted
      }
      fileIds = append(fileIds, fileId)
    }
  }

  // 对文件 id 进行排序，从小到大依次加载
  sort.Ints(fileIds)

  // 遍历文件 id，依次打开
  for i, fid := range fileIds {
    ioType := fio.StandardFIO
    if db.options.MMapAtStartup {
      ioType = fio.MemoryMap
    }
    dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fid), ioType)
    if err != nil {
      return err
    }
    // 将最后一个文件变为活跃文件
    if i == len(fileIds)-1 {
      db.activeFile = dataFile
    } else { // 说明是旧文件
      db.olderFiles[uint32(fid)] = dataFile
    }
  }
  db.fileIds = fileIds
  return nil
}

// loadIndexFromDataFiles 从数据文件中加载索引
// 遍历文件中的所有记录，并更新到内存索引中
func (db *DB) loadIndexFromDataFiles() error {
  // 没有文件直接返回
  if len(db.fileIds) == 0 {
    return nil
  }

  // 查看是否发生过 merge，并获取未被 merge 的第一个文件 id
  // 后续加载索引的时候，要排除参与过 merge 的文件，因为已经从 hint 文件中加载过了
  hasMerge, nonMergeFileId := false, uint32(0)
  mergeFinFileName := filepath.Join(db.options.DirPath, data.MergeFinishedFileName)
  if _, err := os.Stat(mergeFinFileName); err == nil {
    fid, err := db.getNonMergeFileId(db.options.DirPath)
    if err != nil {
      return err
    }
    hasMerge = true
    nonMergeFileId = fid
  }

  updateIndex := func(key []byte, typ data.LogRecordType, pos *data.LogRecordPos) {
    var oldPos *data.LogRecordPos
    // 对已删除的记录进行处理
    if typ == data.LogRecordDeleted {
      oldPos, _ = db.index.Delete(key)
      db.reclaimSize += int64(pos.Size)
    } else {
      oldPos = db.index.Put(key, pos)
    }
    if oldPos != nil {
      db.reclaimSize += int64(oldPos.Size)
    }
  }

  // 暂存事务操作中的数据
  txnRecords := make(map[uint64][]*data.TransactionRecord)
  var curSeqNo = nonTransactionSeqNo

  // 遍历所有文件 id，处理文件中的记录
  for i, fd := range db.fileIds {
    // 获取文件
    var fileId = uint32(fd)

    // 排除 merge 过的数据，其已经通过 hint 索引文件加载
    if hasMerge && fileId < nonMergeFileId {
      continue
    }

    var dataFile *data.DataFile
    if fileId == db.activeFile.FileId {
      dataFile = db.activeFile
    } else {
      dataFile = db.olderFiles[fileId]
    }

    // 循环处理文件中的所有记录
    var offset int64 = 0
    for {
      logRecord, size, err := dataFile.ReadLogRecord(offset)
      if err != nil {
        if err == io.EOF {
          break
        }
        return err
      }
      // 构造内存索引并保存
      logRecordPos := &data.LogRecordPos{
        Fid:    fileId,
        Offset: offset,
        Size:   uint32(size),
      }

      // 解析 Key，拿到事务序列号
      realKey, seqNo := parseLogRcordKeyWithSeqNo(logRecord.Key)
      if seqNo == nonTransactionSeqNo {
        // 非事务操作，直接更新内存索引
        updateIndex(realKey, logRecord.Type, logRecordPos)
      } else {
        // 事务完成，对应事务中的所有数据可以更新到内存索引中
        if logRecord.Type == data.LogRecordTxnFinished {
          for _, txnRecord := range txnRecords[seqNo] {
            updateIndex(txnRecord.Record.Key, txnRecord.Record.Type, txnRecord.Pos)
          }
          delete(txnRecords, seqNo)
        } else {
          //  事务中的数据还未全部获取，先暂存起来
          logRecord.Key = realKey
          txnRecords[seqNo] = append(txnRecords[seqNo], &data.TransactionRecord{
            Record: logRecord,
            Pos:    logRecordPos,
          })
        }
      }

      // 更新事务序列号
      if seqNo > curSeqNo {
        curSeqNo = seqNo
      }

      // 更新偏移量，下一次从文件新的偏移量处进行读取
      offset += size
    }

    // 还需要维护活跃文件的 Offset
    if i == len(db.fileIds)-1 {
      db.activeFile.WriteOff = offset
    }
  }

  // 更新数据库最新事务序列号
  db.seqNo = curSeqNo

  return nil
}

func (db *DB) loadSeqNo() error {
  fileName := filepath.Join(db.options.DirPath, data.SeqNoFileName)
  if _, err := os.Stat(fileName); os.IsNotExist(err) {
    return nil
  }

  seqNoFile, err := data.OpenSeqNoFile(db.options.DirPath)
  if err != nil {
    return err
  }
  record, _, err := seqNoFile.ReadLogRecord(0)
  seqNo, err := strconv.ParseUint(string(record.Value), 10, 64)
  if err != nil {
    return err
  }
  db.seqNo = seqNo
  db.seqNoFileExist = true
  return os.Remove(fileName)
}

// 将数据文件的 IO 类型设置为标准文件 IO
func (db *DB) resetIoType() error {
  if db.activeFile == nil {
    return nil
  }

  if err := db.activeFile.SetIOManager(db.options.DirPath, fio.StandardFIO); err != nil {
    return err
  }
  for _, dataFile := range db.olderFiles {
    err := dataFile.SetIOManager(db.options.DirPath, fio.StandardFIO)
    if err != nil {
      return err
    }
  }
  return nil
}
