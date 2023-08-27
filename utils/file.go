package utils

import (
  "io/fs"
  "os"
  "path/filepath"
  "strings"
  "syscall"
)

// DirSize 获取一个目录的大小
func DirSize(dirPath string) (int64, error) {
  var size int64
  err := filepath.Walk(dirPath, func(path string, info fs.FileInfo, err error) error {
    if err != nil {
      return err
    }
    if !info.IsDir() {
      size += info.Size()
    }
    return nil
  })
  return size, err
}

// AvailableDiskSize 获取磁盘剩余可用空间的大小
func AvailableDiskSize() (uint64, error) {
  // 获取工作目录
  wd, err := syscall.Getwd()
  if err != nil {
    return 0, err
  }
  // 获取文件系统的状态信息
  var stat syscall.Statfs_t
  if err = syscall.Statfs(wd, &stat); err != nil {
    return 0, err
  }
  return stat.Bavail * uint64(stat.Bsize), nil
}

// CopyDir 拷贝数据目录
func CopyDir(src, dest string, exclude []string) error {
  // 目标目录不存在则创建
  if _, err := os.Stat(dest); os.IsNotExist(err) {
    if err := os.MkdirAll(dest, os.ModePerm); err != nil {
      return err
    }
  }

  return filepath.Walk(src, func(path string, info fs.FileInfo, err error) error {
    // 对 src 目录下的每个文件，将前缀置为空，取出最后的文件名
    fileName := strings.Replace(path, src, "", 1)
    if fileName == "" {
      return nil
    }

    for _, e := range exclude {
      matched, err := filepath.Match(e, info.Name())
      if err != nil {
        return err
      }
      if matched {
        return nil
      }
    }

    // 如果是个文件夹，就要在 dest 创建一个同样的文件夹
    if info.IsDir() {
      return os.MkdirAll(filepath.Join(dest, fileName), info.Mode())
    }

    data, err := os.ReadFile(filepath.Join(src, fileName))
    if err != nil {
      return err
    }
    return os.WriteFile(filepath.Join(dest, fileName), data, info.Mode()) // 写入目标目录
  })
}
