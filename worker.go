package consumer

import (
	"sync"

	"github.com/go-jar/golog"
)

type ProcessMsg func(msg []byte) error

type IWorker interface {
	Start(wg *sync.WaitGroup)
	Stop()
	ObtainMsg(msg []byte)
}

type Worker struct {
	logger     golog.ILogger
	processMsg ProcessMsg

	Id       int
	lineChan chan []byte
	stopChan chan bool
}

func NewWorker(id int) *Worker {
	return &Worker{
		logger: new(golog.NoopLogger),

		Id:       id,
		lineChan: make(chan []byte),
		stopChan: make(chan bool),
	}
}

func (w *Worker) SetLogger(logger golog.ILogger) {
	w.logger = logger
}

func (w *Worker) SetProcessMsg(pm ProcessMsg) {
	w.processMsg = pm
}

func (w *Worker) Start(wg *sync.WaitGroup) {
	defer func() {
		wg.Done()
	}()

	if w.lineChan == nil {
		w.lineChan = make(chan []byte)
	}

	for {
		select {
		case msg := <-w.lineChan:
			if err := w.processMsg(msg); err != nil {
				w.logger.Error([]byte("worker process msg error: " + err.Error()))
			}
		case <-w.stopChan:
			return
		}
	}
}

func (w *Worker) Stop() {
	w.stopChan <- true
}

func (w *Worker) ObtainMsg(msg []byte) {
	w.lineChan <- msg
}
