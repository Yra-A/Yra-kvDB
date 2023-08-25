package bitcask_go

import (
  "bitcask-go/data"
  "bitcask-go/utils"
  "io"
  "os"
  "path"
  "path/filepath"
  "sort"
  "strconv"
)

const (
  mergeDirName     = "-merge"
  mergeFinishedKey = "merge.finished"
)

// Merge 清理无效数据，生成 Hint 文件
func (db *DB) Merge() error {
  // 如果数据库为空，则直接返回
  if db.activeFile == nil {
    return nil
  }

  db.mu.Lock()

  // 如果 Merge 已经正在进行当中，则直接返回
  if db.isMerging {
    db.mu.Unlock()
    return ErrMergeIsProgress
  }

  // 查看可以 merge 的数据量是否达到了阈值
  totalSize, err := utils.DirSize(db.options.DirPath)
  if err != nil {
    db.mu.Unlock()
    return err
  }
  if float32(db.reclaimSize)/float32(totalSize) < db.options.DataFileMergeRatio {
    db.mu.Unlock()
    return ErrMergeRatioUnreached
  }

  // 查看剩余的空间容量是否可以容纳 merge 之后的数据量
  availableDiskSize, err := utils.AvailableDiskSize()
  if err != nil {
    db.mu.Unlock()
    return err
  }
  // merge 临时库里存的是有效数据
  if uint64(totalSize-db.reclaimSize) >= availableDiskSize {
    db.mu.Unlock()
    return ErrNoEnoughSpaceForMerge
  }

  db.isMerging = true
  defer func() {
    db.isMerging = false
  }()

  // 持久化当前活跃文件
  if err := db.activeFile.Sync(); err != nil {
    db.mu.Unlock()
    return err
  }

  // 将当前活跃文件转化成旧数据文件
  db.olderFiles[db.activeFile.FileId] = db.activeFile
  // 打开新的活跃文件
  if err := db.setActiveDataFile(); err != nil {
    db.mu.Unlock()
    return nil
  }

  // 记录第一条没有参与 merge 的文件 id
  nonMergeFileId := db.activeFile.FileId

  // 取出所有需要 merge 的文件
  var mergeFiles []*data.DataFile
  for _, file := range db.olderFiles {
    mergeFiles = append(mergeFiles, file)
  }
  db.mu.Unlock()

  // 将 merge 的文件从小到大进行排序，依次 merge
  sort.Slice(mergeFiles, func(i, j int) bool {
    return mergeFiles[i].FileId < mergeFiles[j].FileId
  })

  mergePath := db.getMergePath()
  // 如果目录存在，说明之前 merge 过，需要先将其删除
  if _, err := os.Stat(mergePath); err == nil {
    if err := os.RemoveAll(mergePath); err != nil {
      return err
    }
  }

  // 新建一个 merge path 的目录
  if err := os.MkdirAll(mergePath, os.ModePerm); err != nil {
    return err
  }

  // 打开一个 merge 用的临时数据库实例
  mergeOptions := db.options
  mergeOptions.DirPath = mergePath
  mergeOptions.SyncWrites = false // 可以先暂时关闭持久化写入，提高性能。如果出现错误，merge 操作会失败，没持久化也不影响正确性
  mergeDB, err := Open(mergeOptions)
  if err != nil {
    return err
  }

  // 打开 Hint 文件存储索引
  hintFile, err := data.OpenHintFile(mergePath)
  if err != nil {
    return err
  }

  // 遍历处理每个数据文件
  // 将数据和内存索引上的记录进行比较，符合条件才算有效数据
  for _, dataFile := range mergeFiles {
    var offset int64 = 0
    for {
      logRecord, size, err := dataFile.ReadLogRecord(offset)
      if err != nil {
        if err == io.EOF {
          break
        }
        return err
      }
      realKey, _ := parseLogRcordKeyWithSeqNo(logRecord.Key)
      logRecordPos := db.index.Get(realKey)
      // 和内存索引中的索引位置进行比较，如果有效则重写
      if logRecordPos != nil &&
        logRecordPos.Fid == dataFile.FileId &&
        logRecordPos.Offset == offset {
        // 进行重写，因为是有效数据，所以可以直接清除事务序列号
        logRecord.Key = logRecordKeyWithSeq(realKey, nonTransactionSeqNo)
        pos, err := mergeDB.appendLogRecord(logRecord)
        if err != nil {
          return err
        }
        // 将当前位置索引写到 Hint 文件中
        if err := hintFile.WriteHintRecord(realKey, pos); err != nil {
          return err
        }
      }
      // 增加 offset
      offset += size
    }
  }

  // sync 所有数据写完后进行一次持久化，保证写入磁盘。（而非写一条数据，sync 一次）
  if err := hintFile.Sync(); err != nil {
    return err
  }
  if err := mergeDB.Sync(); err != nil {
    return err
  }

  // 写标识 merge 完成的文件
  mergeFinishedFile, err := data.OpenMergeFinishedFile(mergePath)
  if err != nil {
    return err
  }
  mergeFinRecord := &data.LogRecord{
    Key:   []byte(mergeFinishedKey),
    Value: []byte(strconv.Itoa(int(nonMergeFileId))), // 记录最近没有 merge 的 file id；比他小的 file id 都参与了 merge
  }
  encRecord, _ := data.EncodeLogRecord(mergeFinRecord)
  if err := mergeFinishedFile.Write(encRecord); err != nil {
    return err
  }
  if err := mergeFinishedFile.Sync(); err != nil {
    return err
  }

  return nil
}

// 得到 merge 用的数据库路径
// 例如数据库路径为 /tmp/bitcask，则得到 /tmp/bitcask-merge
func (db *DB) getMergePath() string {
  dir := path.Dir(path.Clean(db.options.DirPath)) // 当前数据库路径的父目录
  base := path.Base(db.options.DirPath)           // 数据库文件名称
  return filepath.Join(dir, base+mergeDirName)
}

// 加载 merge 数据目录
func (db *DB) loadMergeFiles() error {
  mergePath := db.getMergePath()
  // merge 目录不存在则直接返回
  if _, err := os.Stat(mergePath); os.IsNotExist(err) {
    return nil
  }
  // 加载完成后要删除 merge 目录
  defer func() {
    os.RemoveAll(mergePath)
  }()

  dirEntries, err := os.ReadDir(mergePath)
  if err != nil {
    return err
  }

  // 查找表示 merge 完成的文件，判断 merge 是否正常处理完成
  var mergeFinished bool
  var mergeFileNames []string // 保存 merge 后生成的文件名
  for _, entry := range dirEntries {
    if entry.Name() == data.MergeFinishedFileName {
      mergeFinished = true
    }
    if entry.Name() == data.SeqNoFileName {
      continue
    }
    mergeFileNames = append(mergeFileNames, entry.Name())
  }

  // 没有 merge 正常完成则直接正常返回
  if !mergeFinished {
    return nil
  }

  // 获得第一个未被 merge 的文件 id
  nonMergeFileId, err := db.getNonMergeFileId(mergePath)
  if err != nil {
    return nil
  }
  // 删除原数据库中已经被 merge 了的旧数据文件
  var fileId uint32 = 0
  for ; fileId < nonMergeFileId; fileId++ {
    fileName := data.GetDataFileName(db.options.DirPath, fileId)
    if _, err := os.Stat(fileName); err == nil {
      if err := os.Remove(fileName); err != nil {
        return err
      }
    }
  }

  // 将 merge 完成后的新的数据文件移动到原数据库中
  for _, fileName := range mergeFileNames {
    // 更改路径即可完成转移
    srcPath := filepath.Join(mergePath, fileName)
    destPath := filepath.Join(db.options.DirPath, fileName)
    if err := os.Rename(srcPath, destPath); err != nil {
      return err
    }
  }
  return nil
}

// getNonMergeFileId 读取第一个未被 merge 的文件 id
func (db *DB) getNonMergeFileId(dirPath string) (uint32, error) {
  mergeFinishedFile, err := data.OpenMergeFinishedFile(dirPath)
  if err != nil {
    return 0, err
  }

  // 读取存储着第一个未被 merge 的文件 id 的 LogRecord
  // 其 value 就是要的文件 id 值
  record, _, err := mergeFinishedFile.ReadLogRecord(0)
  if err != nil {
    return 0, err
  }
  nonMergeFileId, err := strconv.Atoi(string(record.Value))
  if err != nil {
    return 0, err
  }
  return uint32(nonMergeFileId), nil
}

// 从 hint 文件中加载索引
func (db *DB) loadIndexFromHintFile() error {
  // 查看 hint 文件是否存在
  hintFileName := filepath.Join(db.options.DirPath, data.HintFileName)
  if _, err := os.Stat(hintFileName); os.IsNotExist(err) {
    return nil
  }

  // 打开 hint 索引文件
  hintFile, err := data.OpenHintFile(db.options.DirPath)
  if err != nil {
    return err
  }

  // 读取文件中的索引
  var offset int64 = 0
  for {
    logRecord, size, err := hintFile.ReadLogRecord(offset)
    if err != nil {
      if err == io.EOF {
        break
      }
      return err
    }

    // 解码得到实际位置索引信息，并更新索引
    pos := data.DecodeLogRecordPos(logRecord.Value)
    db.index.Put(logRecord.Key, pos)
    offset += size
  }
  return nil
}
