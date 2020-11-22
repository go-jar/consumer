package consumer

type FetchMessage func(msg []byte) error

type IConsumer interface {
	SetMessageCallback(mc FetchMessage)
	Start()
	Stop()
}

type Consumer struct {
	MessageCallback FetchMessage
}

func (c *Consumer) SetMessageCallback(fm FetchMessage) {
	c.MessageCallback = fm
}
