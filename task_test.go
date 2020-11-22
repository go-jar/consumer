package consumer

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/go-jar/golog"
)

func TestTask(t *testing.T) {
	workerCnt := 3
	logger, _ := golog.NewConsoleLogger(golog.LEVEL_INFO)
	consumer := new(DemoConsumer)
	dispatcher := NewDispatcher(workerCnt)
	workers := make([]IWorker, workerCnt)

	for i := 0; i < 3; i++ {
		worker := &DemoWorker{NewWorker(i)}
		worker.SetLogger(logger)
		worker.SetProcessMsg(worker.ProcessMsg)
		workers[i] = worker
	}

	task := NewTask("task", consumer, dispatcher, workers)
	task.SetLogger(logger)

	go task.Start()
	time.Sleep(time.Second * 1)
	task.Stop()
}

type DemoConsumer struct {
	Consumer
	stop bool
}

func (dc *DemoConsumer) Start() {
	i := 0

	for {
		msg := "msg " + strconv.Itoa(i)
		dc.MessageCallback([]byte(msg))

		//time.Sleep(time.Second * 1)
		i++

		if dc.stop {
			return
		}
	}
}

func (dc *DemoConsumer) Stop() {
	dc.stop = true
}

type DemoWorker struct {
	*Worker
}

func (dw *DemoWorker) ProcessMsg(msg []byte) error {
	fmt.Println("worker " + strconv.Itoa(dw.Id) + " process msg: " + string(msg))
	return nil
}
