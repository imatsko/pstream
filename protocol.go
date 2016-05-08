package pstream

import "log"

type ProtocolMessage struct {
	ProtoID int
	Payload interface{}
}

const (
	PROTO_KADEMLIA = 1
)

type ProtoHandler interface {
	SendChan() <-chan interface{}
	RecvChan() chan<- interface{}
}

type ProtocolNetworkConnection interface {
	SendChan() chan ProtocolMessage
	RecvChan() chan ProtocolMessage
}

type ProtocolServer struct {
	network   ProtocolNetworkConnection
	serve_map map[int]ProtoHandler
}

func StartProtocolServer(network ProtocolNetworkConnection) *ProtocolServer {
	s := new(ProtocolServer)
	s.serve_map = make(map[int]ProtoHandler)
	s.network = network
	go s.Serve()
	return s
}

func (s *ProtocolServer) Register(proto int, handler ProtoHandler) {
	if _, ok := s.serve_map[proto]; ok {
		log.Printf("Proto %v already defined. Redefine by %v", proto, handler)
	}
	log.Printf("register proto %v handler %#v", proto, handler)
	s.serve_map[proto] = handler
}

func (s *ProtocolServer) Serve() {
	go s.ServeRecv()
	go s.ServeSend()
}

func (s *ProtocolServer) ServeRecv() {
	for {
		select {
		case msg := <-s.network.RecvChan():
			log.Printf("Proto got message %#v", msg)
			if handler, ok := s.serve_map[msg.ProtoID]; ok {
				log.Printf("Proto handler found %#v", handler)
				handler.RecvChan() <- msg.Payload
			}
		}
	}
}

func (s *ProtocolServer) ServeSend() {
	for {
		for proto, handler := range s.serve_map {
			select {
			case newMsg := <-handler.SendChan():
				log.Printf("Proto send %#v from handler %#v proto %v", newMsg, handler, proto)
				s.network.SendChan() <- ProtocolMessage{proto, newMsg}
			}
		}
	}
}

type KademliaProtoHandler struct {
	sendChan chan interface{}
	recvChan chan interface{}
	handler  *KademliaRPCHandler
	client   *KademliaRPCClient
}

func NewKademliaProtoHandler(handler *KademliaRPCHandler, client *KademliaRPCClient) *KademliaProtoHandler {
	h := new(KademliaProtoHandler)
	h.sendChan = make(chan interface{})
	h.sendChan = make(chan interface{})
	h.client = client
	h.handler = handler
	return h
}

func (h *KademliaProtoHandler) SendChan() <-chan interface{} {
	return h.sendChan
}
func (h *KademliaProtoHandler) RecvChan() chan<- interface{} {
	return h.recvChan
}

func (h *KademliaProtoHandler) Serve() {
	go h.ServeRecv()
	go h.ServeSend()
}

func (h *KademliaProtoHandler) ServeSend() {
	for {
		if h.handler != nil {
			select {
			case kadMsg := <-h.handler.ResMsgChan:
				h.sendChan <- kadMsg
			}
		}
		if h.client != nil {
			select {
			case kadMsg := <-h.client.ReqMsgChan:
				h.sendChan <- kadMsg
			}
		}
	}
}

func (h *KademliaProtoHandler) ServeRecv() {
	for {
		msg := <-h.recvChan
		if _, ok := msg.(KademliaMessage); !ok {
			log.Printf("Cant cast message %#v", msg)
			continue
		}
		kadMsg := msg.(KademliaMessage)

		switch kadMsg.KademliaType {
		case KAD_PING_REQ, KAD_FIND_NODE_REQ, KAD_FIND_VALUE_REQ, KAD_STORE_VALUE_REQ:
			if h.handler != nil {
				h.handler.ReqMsgChan <- &kadMsg
			}
		case KAD_PING_RES, KAD_FIND_NODE_RES, KAD_FIND_VALUE_RES, KAD_STORE_VALUE_RES:
			if h.client != nil {
				h.client.ResMsgChan <- &kadMsg
			}
		default:
			log.Printf("UNEXPECTED MESSAGE %#v", kadMsg)
		}
	}
}
