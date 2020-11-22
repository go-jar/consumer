package consumer

type IDispatcher interface {
	Dispatch() int
}

type Dispatcher struct {
	workerIndex int
	workerCnt   int
}

func NewDispatcher(workerCnt int) *Dispatcher {
	return &Dispatcher{
		workerIndex: 0,
		workerCnt:   workerCnt,
	}
}

func (d *Dispatcher) Dispatch() int {
	defer func() {
		d.workerIndex = (d.workerIndex + 1) % d.workerCnt
	}()
	return d.workerIndex
}
