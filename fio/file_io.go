package fio

import "os"

type FileIO struct {
  fd *os.File
}

// NewFileIOManager 初始化标准文件 IO
func NewFileIOManager(fileName string) (*FileIO, error) {
  fd, err := os.OpenFile(
    fileName,
    os.O_CREATE|os.O_RDWR|os.O_APPEND, // 标志位
    DataFilePerm,                      // 文件权限
  )
  if err != nil {
    return nil, err
  }
  return &FileIO{fd: fd}, nil
}

func (fio *FileIO) Read(b []byte, offset int64) (int, error) {
  return fio.fd.ReadAt(b, offset)
}

func (fio *FileIO) Write(b []byte) (int, error) {
  return fio.fd.Write(b)
}

func (fio *FileIO) Sync() error {
  return fio.fd.Sync()
}

func (fio *FileIO) Close() error {
  return fio.fd.Close()
}