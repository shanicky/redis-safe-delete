package main

import (
	"github.com/garyburd/redigo/redis"
	"log"
	"fmt"
	"flag"
)


var (
	address string
	key string
	count int
)

func init() {
	flag.StringVar(&address, "address", "localhost:6379", "redis address")
	flag.StringVar(&key, "key", "__KEY__", "key")
	flag.IntVar(&count, "count", 10, "scan command count parameter")
	flag.Parse()
}

func main() {
	conn, err := redis.Dial("tcp", address)
	if err != nil {
		log.Fatalln(err)
	}

	defer conn.Close()
	keyType, err := redis.String(conn.Do("type", key))
	if err != nil {
		log.Fatalln(err)
	}

	switch keyType {
	case "list":
		err = commonPop(conn, key, "lpop")
	case "hash":
		err = commonScan(conn, key, "hscan", "hdel")
	case "zset":
		err = commonScan(conn, key, "zscan", "zrem")
	case "set":
		err = commonScan(conn, key, "sscan", "srem")
	case "none":
		err = fmt.Errorf("key %s not exists", key)
	default:
		err = fmt.Errorf("type %s is not supported", keyType)
	}

	if err != nil {
		log.Fatal(err)
	}

	log.Printf("delete key %s type %s successfully", key, keyType)
}

func commonPop(conn redis.Conn, key string, popCommand string) error {
	for {
		ret, err := conn.Do(popCommand, key)

		if err != nil {
			return err
		}

		if ret == nil {
			return nil
		}
	}
}

func commonScan(conn redis.Conn, key string, scanCommand string, delCommand string) error {
	var cursor int64
	var items []string

	for {
		values, err := redis.Values(conn.Do(scanCommand, key, cursor, "COUNT", count))
		if err != nil {
			return err
		}

		_, err = redis.Scan(values, &cursor, &items)
		if err != nil {
			return err
		}

		for _, item := range items {
			conn.Send(delCommand, key, item)
		}

		if err := conn.Flush(); err != nil {
			return err
		}

		if cursor == 0 {
			break
		}
	}

	return nil
}
