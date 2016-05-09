package pstream

import "log"

type EchoClientProtoHandler struct {
	sendChan chan interface{}
	recvChan chan interface{}
	Quit     chan bool
}

func StartEchoClientProtoHandler() *EchoClientProtoHandler {
	h := new(EchoClientProtoHandler)
	h.sendChan = make(chan interface{})
	h.recvChan = make(chan interface{})
	h.Quit = make(chan bool)
	h.Serve()
	return h
}

func (h *EchoClientProtoHandler) SendChan() chan interface{} {
	return h.sendChan
}
func (h *EchoClientProtoHandler) RecvChan() chan interface{} {
	return h.recvChan
}

func (h *EchoClientProtoHandler) Serve() {
	go h.ServeRecv()
	go h.ServeSend()
}

func (h *EchoClientProtoHandler) ServeSend() {
	for {
		select {
		case <-h.Quit:
			log.Printf("Stop serve client send")
			return
		default:
		}
	}
}

func (h *EchoClientProtoHandler) ServeRecv() {
	for {
		select {
		case <-h.Quit:
			log.Printf("Stop serve client recv")
			return
		default:
		}

		msg, ok_recv := <- h.recvChan
		if !ok_recv {
			notify_quit(h.Quit)
		}

		if _, ok_cast := msg.(string); !ok_cast {
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
	quit chan bool
}

func StartEchoProtoHandler() *EchoProtoHandler {
	h := new(EchoProtoHandler)
	h.sendChan = make(chan interface{})
	h.recvChan = make(chan interface{})
	h.quit = make(chan bool)
	h.Serve()
	return h
}

func (h *EchoProtoHandler) SendChan() chan interface{} {
	return h.sendChan
}
func (h *EchoProtoHandler) RecvChan() chan interface{} {
	return h.recvChan
}

func (h *EchoProtoHandler) Serve() {
	go h.ServeRecv()
	go h.ServeSend()
}

func (h *EchoProtoHandler) ServeSend() {
	for {
		select {
		case <-h.quit:
			log.Printf("Stop serve send")
			return
		default:
		}
	}
}

func (h *EchoProtoHandler) ServeRecv() {
	for {
		select {
		case <-h.quit:
			log.Printf("Stop serve recv")
			return
		default:
		}

		msg, recv_ok := <- h.recvChan
		if !recv_ok {
			notify_quit(h.quit)
			continue
		}
		log.Printf("got message for echo %#v", msg)
		if _, cast_ok := msg.(string); !cast_ok {
			log.Printf("Cant cast message %#v", msg)
			continue
		}
		strMsg := msg.(string)

		log.Printf("Echoing string %s", strMsg)
		h.sendChan <- strMsg
	}
}