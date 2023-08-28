package redis

import "errors"

// generic.go 存放通用命令

// Del 根据 key 删除数据
func (rds *RedisDataStructure) Del(key []byte) error {
	return rds.db.Delete(key)
}

// Type 对于 String，获取 value 中维护的类型；对于其他四种数据结构，获取元数据中存储的 Type
func (rds *RedisDataStructure) Type(key []byte) (redisDataType, error) {
	encValue, err := rds.db.Get(key)
	if err != nil {
		return 0, err
	}
	if len(encValue) == 0 {
		return 0, errors.New("value is null")
	}

	// Type 存储在第一个字节
	return encValue[0], nil
}
