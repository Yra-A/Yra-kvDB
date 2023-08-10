package utils

import (
  "fmt"
  "math/rand"
  "time"
)

var (
  letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
  randStr = rand.New(rand.NewSource(time.Now().Unix()))
)

// GetTestKey 获取测试使用的 key
func GetTestKey(i int) []byte {
  return []byte(fmt.Sprintf("bitcask-go-%09d", i))
}

// RandomValue 生成随机 value，用于测试
// 参数 n 表示生成 value 的长度
func RandomValue(n int) []byte {
  b := make([]byte, n)
  for i := range b {
    b[i] = letters[randStr.Intn(len(letters))]
  }
  return []byte("bitcask-go-value-" + string(b))
}
