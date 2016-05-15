package pstream

import (
	"time"
)

type PeerNeighboursState struct {}


type Peer interface {
	SelfId() string
	Neighbours() PeerNeighboursState
	Buffer() BufferState
	InChunks() chan<- *Chunk
	ConnectionClosed(c *Connection)
	SendRate() <-chan bool

}

type PeerImpl struct {
	selfId string
	buf *Buffer
	In chan *Chunk
	Out chan *Chunk
	send_rate chan bool
}

func NewRatedPeer(selfId string) *PeerImpl {
	p := new(PeerImpl)
	p.selfId = selfId
	p.In = make(chan *Chunk)
	p.Out = make(chan *Chunk, 32)
	p.buf = NewBuffer(p.In, p.Out)
	go p.ServeSendRate(100*time.Millisecond)
	return p
}

func NewPeer(selfId string) *PeerImpl {
	p := new(PeerImpl)
	p.selfId = selfId
	p.In = make(chan *Chunk)
	p.Out = make(chan *Chunk, 32)
	p.buf = NewBuffer(p.In, p.Out)
	go p.ServeInfiniteSendRate()
	return p
}

func (p *PeerImpl) ServeSendRate(period time.Duration) {
	ticker := time.NewTicker(period).C
	p.send_rate = make(chan bool)

	for {
		select {
		case <- ticker:
			p.send_rate <- true
		default:
		}
	}
}

func (p *PeerImpl) ServeInfiniteSendRate() {
	p.send_rate = make(chan bool)
	for {
		p.send_rate <- true
	}
}

func (p *PeerImpl) SendRate() <-chan bool {
	return p.send_rate
}

func (p *PeerImpl) SelfId() string {
	return p.selfId
}

func (p *PeerImpl) Neighbours() PeerNeighboursState {
	return PeerNeighboursState{}
}

func (p *PeerImpl) Buffer() BufferState {
	return p.buf.State()
}

func (p *PeerImpl) InChunks() chan<- *Chunk {
	return p.In
}

func (p *PeerImpl) ConnectionClosed(c *Connection) {
}
