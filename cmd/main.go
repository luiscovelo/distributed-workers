package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/labstack/echo"
	"github.com/luiscovelo/distributed-workers/myredis"
	"github.com/luiscovelo/distributed-workers/worker"
)

var pubsubChannel string = "mosaics"

func main() {
	redisClient := myredis.New()

	for i := 0; i < 3; i++ {
		w := worker.New(redisClient)

		_, err := w.Register(pubsubChannel)
		if err != nil {
			log.Println("failed to register worker", err)
		}

		go w.ProcessMessages()

	}

	go func() {
		router := echo.New()
		router.POST("/publish/:name", func(c echo.Context) error {
			if err := publishToChannel(redisClient, c.Param("name")); err != nil {
				return c.JSON(http.StatusBadGateway, err.Error())
			}
			return c.NoContent(http.StatusOK)
		})

		if err := router.Start(":9000"); err != nil {
			log.Fatal(err)
		}
	}()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	<-sigs
	log.Println("exiting...")
}

func publishToChannel(myRedis *myredis.MyRedis, msg string) error {
	err := myRedis.Client.Publish(
		context.Background(),
		pubsubChannel,
		msg,
	).Err()

	if err != nil {
		return err
	}

	return nil
}
