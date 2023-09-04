package main

import (
	bitcask "bitcask-go"
	bitcask_redis "bitcask-go/redis"
	"github.com/tidwall/redcon"
	"log"
	"sync"
)

const addr = "127.0.0.1:6380"

type BitcaskServer struct {
	dbs    map[int]*bitcask_redis.RedisDataStructure
	server *redcon.Server
	mu     sync.RWMutex
}

func main() {
	// 打开 Redis 数据结构服务
	redisDataStructure, err := bitcask_redis.NewRedisDataStructure(bitcask.DefaultOptions)
	if err != nil {
		panic(err)
	}

	// 初始化 BitcaskServer
	bitcaskServer := &BitcaskServer{
		dbs: make(map[int]*bitcask_redis.RedisDataStructure),
	}
	bitcaskServer.dbs[0] = redisDataStructure

	// 初始化一个 Redis 服务端
	bitcaskServer.server = redcon.NewServer(addr, execClientCommand, bitcaskServer.accept, bitcaskServer.close)
	bitcaskServer.listen() // 监听端口
}

func (svr *BitcaskServer) listen() {
	log.Printf("bitcask server running, ready to accept connection.")
	_ = svr.server.ListenAndServe()
}

func (svr *BitcaskServer) accept(conn redcon.Conn) bool {
	// 建立连接，创建一个 BitcaskClient
	cli := new(BitcaskClient)
	svr.mu.Lock()
	defer svr.mu.Unlock()
	cli.server = svr
	cli.db = svr.dbs[0]
	conn.SetContext(cli)
	return true
}

// close 关闭 BitcaskServer
func (svr *BitcaskServer) close(conn redcon.Conn, err error) {
	for _, db := range svr.dbs {
		_ = db.Close()
	}
	_ = svr.server.Close()
}
