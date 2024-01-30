package worker

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsm/redislock"
	"github.com/google/uuid"
	"github.com/luiscovelo/distributed-workers/myredis"
	"github.com/redis/go-redis/v9"
)

type Worker struct {
	Key            string
	MaxMosaic      int64
	MosaicsRunning int64

	pubSub  *redis.PubSub
	myRedis *myredis.MyRedis
	mx      sync.Mutex
}

func New(myRedis *myredis.MyRedis) *Worker {
	worker := &Worker{
		Key:       fmt.Sprintf("worker:%s", uuid.New().String()),
		MaxMosaic: 1,
		myRedis:   myRedis,
		mx:        sync.Mutex{},
	}

	return worker
}

func (w *Worker) Register(pubsubChannel string) (string, error) {
	err := w.myRedis.Client.Set(context.Background(), w.Key, 0, 5*time.Minute).Err()
	if err != nil {
		return "", err
	}

	w.pubSub = w.myRedis.Client.Subscribe(context.Background(), pubsubChannel)

	if err := w.updateWorkerCounter(); err != nil {
		return "", err
	}

	return w.Key, nil
}

func (w *Worker) updateWorkerCounter() error {
	return w.myRedis.Client.Incr(context.Background(), "workers_running").Err()
}

func (w *Worker) updateNumMosaicRunning() error {
	total := atomic.AddInt64(&w.MosaicsRunning, 1)
	w.MosaicsRunning = total
	return w.myRedis.Client.Incr(context.Background(), w.Key).Err()
}

func (w *Worker) ProcessMessages() {
	log.Printf("pub/sub of worker %s running", w.Key)
	for message := range w.pubSub.Channel() {
		log.Printf("[%s] %s", w.Key, message.Payload)
		w.GenerateMosaic(w.Key, message.Payload)
	}
}

func (w *Worker) GenerateMosaic(workerKey, name string) error {
	ctx := context.Background()
	key := fmt.Sprintf("mosaic:%s", name)

	if w.MosaicsRunning >= w.MaxMosaic {
		log.Println("this worker cannot process mosaic because has exceed limit")
		return nil
	}

	lock, err := redislock.Obtain(ctx, w.myRedis.Client, key, 100*time.Millisecond, nil)
	if err == redislock.ErrNotObtained {
		fmt.Println("Could not obtain lock!")
		return nil
	} else if err != nil {
		return err
	}

	defer lock.Release(ctx)

	if err := w.myRedis.Client.Set(ctx, key, w.Key, 15*time.Second).Err(); err != nil {
		return err
	}

	if err := w.updateNumMosaicRunning(); err != nil {
		return err
	}

	go func() {
		for {
			log.Printf("[mosaic:%s] running in %s", name, workerKey)

			err := w.myRedis.Client.Set(ctx, key, w.Key, 15*time.Second).Err()
			if err != nil {
				log.Printf("[mosaic:%s] failed to update TTL, error=%v", name, err)
				continue
			}

			time.Sleep(10 * time.Second)
		}
	}()

	return nil
}
