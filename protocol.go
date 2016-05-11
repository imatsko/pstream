package pstream

import "log"

type ProtocolMessage struct {
	ProtoID int
	Payload interface{}
}

func notify_quit(Chan chan bool) {
	var closed bool
	select {
	case _, ok := <-Chan:
		closed = !ok
	default:
	}
	if !closed {
		close(Chan)
	}
}

const (
	PROTO_KADEMLIA = 1
)

type ProtoHandler interface {
	SendChan() chan interface{}
	RecvChan() chan interface{}
}

type ProtocolNetworkConnection interface {
	SendChan() chan ProtocolMessage
	RecvChan() chan ProtocolMessage
}

type ProtocolServer struct {
	network ProtocolNetworkConnection
	// TODO: concurrent map https://github.com/streamrail/concurrent-map
	serve_map map[int]ProtoHandler
	quit      chan bool
}

func StartProtocolServer(network ProtocolNetworkConnection) *ProtocolServer {
	s := new(ProtocolServer)
	s.serve_map = make(map[int]ProtoHandler)
	s.network = network
	s.quit = make(chan bool)
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
		case <-s.quit:
			log.Printf("Close all protocols recv")
			for proto_id, handler := range s.serve_map {
				log.Printf("Close proto recv %v %#v", proto_id, handler)
				close(handler.RecvChan())
			}
			return
		case msg, ok := <-s.network.RecvChan():
			if !ok {
				log.Printf("Proto got connection closed")
				notify_quit(s.quit)
				continue
			}

			log.Printf("Proto got message %#v", msg)
			if handler, found := s.serve_map[msg.ProtoID]; found {
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
			case <-s.quit:
				log.Printf("Close all protocols send")
				for proto_id, handler := range s.serve_map {
					log.Printf("Close proto send %v %#v", proto_id, handler)
					close(handler.SendChan())
				}
				return
			case newMsg := <-handler.SendChan():
				log.Printf("Proto send %#v from handler %#v proto %v", newMsg, handler, proto)
				s.network.SendChan() <- ProtocolMessage{proto, newMsg}
			}
		}
	}
}
