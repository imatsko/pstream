package pstream

import "log"

type EchoClientProtoHandler struct {
	sendChan chan interface{}
	recvChan chan interface{}
}

func StartEchoClientProtoHandler() *EchoClientProtoHandler {
	h := new(EchoClientProtoHandler)
	h.sendChan = make(chan interface{})
	h.recvChan = make(chan interface{})
	h.Serve()
	return h
}

func (h *EchoClientProtoHandler) SendChan() <-chan interface{} {
	return h.sendChan
}
func (h *EchoClientProtoHandler) RecvChan() chan<- interface{} {
	return h.recvChan
}

func (h *EchoClientProtoHandler) Serve() {
	go h.ServeRecv()
	go h.ServeSend()
}

func (h *EchoClientProtoHandler) ServeSend() {
	for {
	}
}

func (h *EchoClientProtoHandler) ServeRecv() {
	for {
		msg := <- h.recvChan
		if _, ok := msg.(string); !ok {
			log.Printf("Cant cast message %#v", msg)
			continue
		}
		strMsg := msg.(string)

		log.Printf("Got response string %s", strMsg)
	}
}

func (h *EchoClientProtoHandler) Send(str string) {
	log.Printf("Send string for echo %v", str)
	h.sendChan <- str
}



type EchoProtoHandler struct {
	sendChan chan interface{}
	recvChan chan interface{}
}

func StartEchoProtoHandler() *EchoProtoHandler {
	h := new(EchoProtoHandler)
	h.sendChan = make(chan interface{})
	h.recvChan = make(chan interface{})
	h.Serve()
	return h
}

func (h *EchoProtoHandler) SendChan() <-chan interface{} {
	return h.sendChan
}
func (h *EchoProtoHandler) RecvChan() chan<- interface{} {
	return h.recvChan
}

func (h *EchoProtoHandler) Serve() {
	go h.ServeRecv()
	go h.ServeSend()
}

func (h *EchoProtoHandler) ServeSend() {
	for {
	}
}

func (h *EchoProtoHandler) ServeRecv() {
	for {
		msg := <- h.recvChan
		log.Printf("got message for echo %#v", msg)
		if _, ok := msg.(string); !ok {
			log.Printf("Cant cast message %#v", msg)
			continue
		}
		strMsg := msg.(string)

		log.Printf("Echoing string %s", strMsg)
		h.sendChan <- strMsg
	}
}