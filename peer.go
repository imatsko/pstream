package pstream

import (
	"time"
	"github.com/op/go-logging"
	"net"
	"fmt"
	"sync"
	"math/rand"
)

const (
	cmd_peer_close = 1
	cmd_peer_open = 2
	cmd_peer_notify_update = 3
	cmd_peer_reconfigure_net = 4
	cmd_peer_bootstrap_net = 5
)

var peer_log = logging.MustGetLogger("peer")


type PeerNeighboursState struct {}


type Peer interface {
	SelfId() string
	Neighbours() PeerNeighboursState
	Buffer() BufferState
	InChunks() chan<- *Chunk
	ConnectionClosed(c *Connection)
	ConnectionOpened(c *Connection)

}

type PeerImpl struct {
	selfId           string
	buf              *Buffer
	In               chan *Chunk
	buf_in           chan *Chunk
	Out              chan *Chunk
	rate_ch          chan bool
	SendPeriod       time.Duration


	conn_counter     int
	conn_counter_mut sync.Mutex

	cmd_ch           chan command

	sink_conn        map[string]*Connection
	src_conn         map[string]*Connection

}

func NewPeer(selfId string) *PeerImpl {
	p := new(PeerImpl)
	p.selfId = selfId
	p.In = make(chan *Chunk)
	p.buf_in = make(chan *Chunk)
	p.Out = make(chan *Chunk, 32)

	p.buf = NewBuffer(p.buf_in, p.Out)

	p.cmd_ch = make(chan command, 32)

	p.conn_counter = 1

	p.sink_conn = make(map[string]*Connection)

	return p
}

func (p *PeerImpl) NotifyUpdate(chunk_id uint64) {
	p.cmd_ch <- command{
		cmdId: cmd_peer_notify_update,
		args: chunk_id,
	}
}

func (p *PeerImpl) ReconfigureNetwork() {
	p.cmd_ch <- command{
		cmdId: cmd_peer_reconfigure_net,
	}
}

func (p *PeerImpl) BootstrapNetwork() {
	p.cmd_ch <- command{
		cmdId: cmd_peer_bootstrap_net,
	}
}

func (p *PeerImpl) ServeSendRate(period time.Duration) {
	peer_log.Infof("start rate %v", period)
	ticker := time.NewTicker(period).C
	p.rate_ch = make(chan bool)

	for {
		select {
		case <- ticker:
			p.rate_ch <- true
		default:
		}
	}
}

func (p *PeerImpl) ServeInfiniteSendRate() {
	peer_log.Infof("start unlimited rate")
	p.rate_ch = make(chan bool)
	for {
		p.rate_ch <- true
	}
}

func (p *PeerImpl) Serve() {

	if p.SendPeriod == 0 {
		go p.ServeInfiniteSendRate()
	} else {
		go p.ServeSendRate(p.SendPeriod)
	}

	for {
		select {
		case cmd := <-p.cmd_ch:
			switch cmd.cmdId {
			case cmd_peer_close:
				p.handleCmdClose(cmd)
			case cmd_peer_open:
				p.handleCmdOpen(cmd)
			case cmd_peer_notify_update:
				p.handleCmdNotifyUpdate(cmd)
			case cmd_peer_reconfigure_net:
				p.handleCmdReconfigureNetwork(cmd)
			default:
				p.handleCmdUnexpected(cmd)
			}
		case chunk := <- p.In:
			p.NotifyUpdate(chunk.Id)
			p.buf_in <- chunk
		case <- p.rate_ch:
			p.handleSend()
		}
	}
}

func (p *PeerImpl)ServeConnections(addr string) {
	peer_log.Infof("Start peer")
	listen, err := net.Listen("tcp", addr)
	if err != nil {
		panic(err)
	}

	for {
		conn, err := listen.Accept() // this blocks until connection or error
		if err != nil {
			peer_log.Errorf("New conneciton error %v", err)
			continue
		}

		new_num := p.getConnNumber()

		new_conn := NewConnection(fmt.Sprintf("%d", new_num), conn, CONN_UNDEFINED, p)
		go new_conn.Serve()
	}
}


func (p *PeerImpl)getConnNumber() int{
	p.conn_counter_mut.Lock()
	new_num := p.conn_counter
	p.conn_counter += 1
	p.conn_counter_mut.Unlock()
	return new_num
}


func (p *PeerImpl) handleCmdNotifyUpdate(cmd command) {
	chunk_id, ok := cmd.args.(uint64)
	peer_log.Warningf("Got update %v %v", chunk_id, ok)
	// TODO add notify
}

func (p *PeerImpl) handleCmdReconfigureNetwork(cmd command) {
	peer_log.Warningf("Do reconfigure %v", cmd)
	// TODO add reconfigure
}

func (p *PeerImpl) handleCmdBootstrapNetwork(cmd command) {
	peer_log.Warningf("Do bootstrap %v", cmd)
	// TODO add bootstrap
}


func (p *PeerImpl) handleCmdClose(cmd command) {
	c := cmd.args.(*Connection)
	peer_log.Warningf("Got closed connection %v", c)
	if _, ok := p.sink_conn[c.ConnId]; ok {
		peer_log.Warningf("Remove sink connection %v", c.ConnId)
		delete(p.sink_conn, c.ConnId)
	} else if _, ok := p.src_conn[c.ConnId]; ok {
		peer_log.Warningf("Remove src connection %v", c.ConnId)
		delete(p.src_conn, c.ConnId)
	} else {
		peer_log.Errorf("Unexpected closed connection %v", c.ConnId)
	}
}

func (p *PeerImpl) handleCmdOpen(cmd command) {
	c := cmd.args.(*Connection)
	peer_log.Warningf("Got new connection %v", c)

	var storage map[string]*Connection

	if c.ConnType == CONN_SEND {
		peer_log.Warningf("Add connection %v type: sink", c.ConnId)
		storage = p.sink_conn
	} else if c.ConnType == CONN_RECV {
		peer_log.Warningf("Add connection %v type: src", c.ConnId)
		storage = p.src_conn
	}

	if _, ok := storage[c.ConnId]; !ok {
		peer_log.Warningf("Add connection %v", c.ConnId)
		storage[c.ConnId] = c
	} else {
		peer_log.Errorf("connection %v already exists, close new", c.ConnId)
		c.Close()
	}
}


func (p *PeerImpl) handleCmdUnexpected(cmd command) {
	peer_log.Warningf("Got unexpected comand %#v", cmd)
}

func (p *PeerImpl) handleSend() {
	sinks := make([]*Connection, 0, len(p.sink_conn))

	for _, c := range p.sink_conn {
		sinks = append(sinks, c)
	}

	var conn *Connection
	var chunk *Chunk
	for i := 0; i < 10 && chunk != nil; i++{
		conn = sinks[rand.Intn(len(sinks))]
		peer_b := conn.PeerBuffer
		if peer_b == nil {
			// skip peer without state
			continue
		}
		chunks := peer_b.Chunks
		chunk = p.buf.LatestUseful(chunks)
	}
	if chunk == nil {
		peer_log.Infof("Nothing to send (%v %v)", chunk, conn)
		return
	}

	peer_log.Infof("Send chunk %#v to sink %#v", chunk, conn)
	conn.Send(chunk)
	peer_log.Infof("Chunk %#v to sink %#v delivered", chunk, conn)
}



//========================================================================
// Peer interface
//========================================================================
func (p *PeerImpl) SendRate() <-chan bool {
	return p.rate_ch
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
	p.cmd_ch <- command{
		cmdId: cmd_peer_close,
		args: c,
	}
}

func (p *PeerImpl) ConnectionOpened(c *Connection) {
	p.cmd_ch <- command{
		cmdId: cmd_peer_open,
		args: c,
	}
}
