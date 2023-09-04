package main

import (
	bitcask "bitcask-go"
	bitcask_redis "bitcask-go/redis"
	"bitcask-go/utils"
	"fmt"
	"github.com/tidwall/redcon"
	"strings"
)

func newWrongNumberOfArgsError(cmd string) error {
	return fmt.Errorf("ERR wrong number of arguments for '%s' command", cmd)
}

type cmdHandler func(cli *BitcaskClient, args [][]byte) (interface{}, error)

var supportedCommands = map[string]cmdHandler{
	"set":   set,
	"get":   get,
	"hset":  hset,
	"sadd":  sadd,
	"lpush": lpush,
	"zadd":  zadd,
}

type BitcaskClient struct {
	server *BitcaskServer
	db     *bitcask_redis.RedisDataStructure
}

func execClientCommand(conn redcon.Conn, cmd redcon.Command) {
	command := strings.ToLower(string(cmd.Args[0]))
	cli := conn.Context().(*BitcaskClient)

	switch command {
	case "quit":
		_ = conn.Close()
	case "ping":
		conn.WriteString("PONG")
	default:
		cmdFun, ok := supportedCommands[command]
		if !ok {
			conn.WriteError("ERR unknown command '" + command + "'")
			return
		}
		res, err := cmdFun(cli, cmd.Args[1:])
		if err != nil {
			if err == bitcask.ErrKeyNotFound {
				conn.WriteNull()
			} else {
				conn.WriteError(err.Error())
			}
			return
		}
		conn.WriteAny(res)
	}
}

func set(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("set")
	}
	key, value := args[0], args[1]
	if err := cli.db.Set(key, 0, value); err != nil {
		return nil, err
	}
	return redcon.SimpleString("OK"), nil
}

func get(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumberOfArgsError("get")
	}
	key := args[0]
	value, err := cli.db.Get(key)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func hset(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, newWrongNumberOfArgsError("hset")
	}
	key, field, value := args[0], args[1], args[2]
	res, err := cli.db.HSet(key, field, value)
	if err != nil {
		return nil, err
	}
	var ok = 0
	if res {
		ok = 1
	}
	return redcon.SimpleInt(ok), nil
}

func sadd(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("sadd")
	}
	key, member := args[0], args[1]
	res, err := cli.db.SAdd(key, member)
	if err != nil {
		return nil, err
	}
	var ok = 0
	if res {
		ok = 1
	}
	return redcon.SimpleInt(ok), nil
}

func lpush(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("lpush")
	}
	key, value := args[0], args[1]
	res, err := cli.db.LPush(key, value)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(res), nil
}

func zadd(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, newWrongNumberOfArgsError("zadd")
	}
	key, score, member := args[0], args[1], args[2]
	res, err := cli.db.ZAdd(key, utils.FloatFromBytes(score), member)
	if err != nil {
		return nil, err
	}
	var ok = 0
	if res {
		ok = 1
	}
	return redcon.SimpleInt(ok), nil
}
