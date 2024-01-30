package myredis

import (
	"context"
	"log"

	"github.com/redis/go-redis/v9"
)

type MyRedis struct {
	Client *redis.Client
}

func New() *MyRedis {
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	if err := client.Ping(context.Background()).Err(); err != nil {
		log.Fatal(err)
	}

	return &MyRedis{Client: client}
}
