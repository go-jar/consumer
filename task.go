package consumer

import (
	"bytes"
	"sync"

	"github.com/go-jar/golog"
)

type ITask interface {
	Start()
	Stop()
}

type Task struct {
	name string

	consumer   IConsumer
	dispatcher IDispatcher
	workers    []IWorker

	logger golog.ILogger
	wg     *sync.WaitGroup
}

func NewTask(name string, consumer IConsumer, dispatcher IDispatcher, workers []IWorker) *Task {
	t := &Task{
		name: name,

		consumer:   consumer,
		dispatcher: dispatcher,
		workers:    workers,

		logger: new(golog.NoopLogger),
		wg:     new(sync.WaitGroup),
	}

	t.consumer.SetMessageCallback(t.messageCallback)

	return t
}

func (t *Task) SetConsumer(consumer IConsumer) *Task {
	t.consumer = consumer
	t.consumer.SetMessageCallback(t.messageCallback)

	return t
}

func (t *Task) SetLogger(logger golog.ILogger) {
	t.logger = logger
}

func (t *Task) Start() {
	for _, worker := range t.workers {
		go worker.Start(t.wg)
		t.wg.Add(1)
	}

	t.consumer.Start()
}

func (t *Task) Stop() {
	t.consumer.Stop()

	for _, worker := range t.workers {
		go worker.Stop()
	}

	t.wg.Wait()
}

func (t *Task) messageCallback(msg []byte) error {
	for _, line := range bytes.Split(msg, []byte("\n")) {
		line = bytes.TrimSpace(line)
		if len(line) > 0 {
			i := t.dispatcher.Dispatch()
			t.workers[i].ObtainMsg(line)
		}
	}
	return nil
}
