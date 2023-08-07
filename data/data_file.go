package data

import "bitcask-go/fio"

const DataFileNameSuffix = ".data"

// DataFile 数据文件
type DataFile struct {
  FileId    uint32        // 当前文件 id
  WriteOff  int64         // 文件写偏移量，当前写到了哪个位置
  IoManager fio.IOManager // io 管理接口，可以调用用来进行 io 操作
}

// OpenDataFile TODO: 打开数据文件
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
  return nil, nil
}

// Sync TODO: 数据文件持久化
func (df *DataFile) Sync() error {
  return nil
}

// Write TODO: 文件写入操作
func (db *DataFile) Write(buf []byte) error {
  return nil
}

// ReadLogRecord TODO: 根据文件偏移量读取数据
func (db *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
  return nil, 0, nil
}
