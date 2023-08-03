package fio

// DataFilePerm 新建文件默认权限 0644
// 八进制数字，所有者有读写权限，所在组和其他用户仅有写权限
const DataFilePerm = 0644

// IOManager 一个 IO 管理的抽象接口，将各种 IO 接口封装在一起，支持不同的文件 IO 实现，目前只实现了标准系统文件 IO
type IOManager interface {
  // Read 从文件指定位置读取数据
  Read([]byte, int64) (int, error)

  // Write 写入数据到文件中
  Write([]byte) (int, error)

  // Sync 持久化数据
  Sync() error

  // Close 关闭文件
  Close() error
}
