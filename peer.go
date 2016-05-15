package pstream

type PeerNeighboursState struct {}


type Peer interface {
	SelfId() string
	Neighbours() PeerNeighboursState
	Buffer() BufferState
	InChunks() chan<- *Chunk
	ConnectionClosed(c *Connection)
}

type PeerImpl struct {
	selfId string
	buf *Buffer
	In chan *Chunk
	Out chan *Chunk
}

func NewPeer(selfId string) *PeerImpl {
	p := new(PeerImpl)
	p.selfId = selfId
	p.In = make(chan *Chunk)
	p.Out = make(chan *Chunk, 32)
	p.buf = NewBuffer(p.In, p.Out)
	return p
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
