package pstream

import (
	"encoding/gob"
	"log"
	"net"
)

type GobNetworkConnection struct {
	Stream   net.Conn
	recvChan chan ProtocolMessage
	sendChan chan ProtocolMessage
	quit     chan bool
}

func StartGobNetworkConnection(conn net.Conn) (c *GobNetworkConnection) {
	c = new(GobNetworkConnection)
	c.Stream = conn
	c.recvChan = make(chan ProtocolMessage)
	c.sendChan = make(chan ProtocolMessage)
	c.quit = make(chan bool)
	go c.Serve()
	return
}

func (c *GobNetworkConnection) ServeRecv() {
	decoder := gob.NewDecoder(c.Stream)
	for {
		select {
		case <-c.quit:
			log.Printf("quit recv received")
			close(c.recvChan)
			return
		default:
		}
		var recvMessage ProtocolMessage
		err := decoder.Decode(&recvMessage)
		if err != nil {
			if err.Error() == "EOF" {
				log.Printf("ERR: EOF")
				notify_quit(c.quit)
			}
			log.Printf("ERR: input decode err %#v", err)
			continue
		}
		log.Printf("recv msg %#v", recvMessage)

		c.recvChan <- recvMessage
	}
}

func (c *GobNetworkConnection) ServeSend() {
	encoder := gob.NewEncoder(c.Stream)
	for {
		select {
		case <-c.quit:
			log.Printf("quit send received")
			close(c.sendChan)
			return
		default:
		}
		sendMessage := <-c.sendChan
		log.Printf("send msg %#v", sendMessage)

		err := encoder.Encode(sendMessage)
		if err != nil {
			log.Printf("ERR: output encode err %v", err)
			continue
		}
	}
}

func (c *GobNetworkConnection) Serve() {
	go c.ServeRecv()
	go c.ServeSend()
}

func (c *GobNetworkConnection) SendChan() chan ProtocolMessage {
	return c.sendChan
}

func (c *GobNetworkConnection) RecvChan() chan ProtocolMessage {
	return c.recvChan
}
