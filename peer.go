package pstream

import (
	rand_c "crypto/rand"
	"encoding/hex"
	"fmt"
	"github.com/op/go-logging"
	"math/rand"
	"net"
	"sync"
	"time"
)

const (
	peer_cmd_close           = 1
	peer_cmd_open            = 2
	peer_cmd_notify_update   = 3
	peer_cmd_reconfigure_net = 4
	peer_cmd_bootstrap_net   = 5
	peer_cmd_net_stat        = 6
	peer_cmd_exit            = 7
)

var peer_log = logging.MustGetLogger("peer")

type PeerStat struct {
	Host        string
	Port        int
	SourceCount int
	SinkCount   int
}

type PeerNeighboursState struct {
	Sinks   []PeerStat
	Sources []PeerStat
}

type Peer interface {
	SelfId() string
	Host() string
	Port() int
	Neighbours() PeerNeighboursState
	Buffer() BufferState
	InChunks() chan<- *Chunk
	ConnectionClosed(c *Connection)
	ConnectionOpened(c *Connection)
}

type PeerImpl struct {
	selfId     string
	listenAddr string

	buf_input chan *Chunk
	buf       *Buffer

	In  chan *Chunk
	Out chan *Chunk

	sendPeriod time.Duration
	rate_ch    chan bool

	port int
	host string

	quit   chan bool
	cmd_ch chan command

	conn_counter     int
	conn_counter_mut sync.Mutex

	sink_conn map[string]*Connection
	src_conn  map[string]*Connection
}

func NewPeer(selfId string, listen string, sendPeriod time.Duration) *PeerImpl {
	p := new(PeerImpl)
	if selfId == "" {
		p.generateRandomId()
	} else {
		p.selfId = selfId
	}
	p.listenAddr = listen
	p.sendPeriod = sendPeriod

	p.In = make(chan *Chunk)
	p.Out = make(chan *Chunk, 32)

	p.buf_input = make(chan *Chunk)
	p.buf = NewBuffer(p.buf_input, p.Out)

	p.cmd_ch = make(chan command, 32)
	p.quit = make(chan bool)

	p.conn_counter = 1

	p.sink_conn = make(map[string]*Connection)
	p.src_conn = make(map[string]*Connection)

	return p
}

func (p *PeerImpl) generateRandomId() {
	buffer := make([]byte, 20)
	_, err := rand_c.Read(buffer)
	if err != nil {
		panic(err)
	}
	p.selfId = hex.EncodeToString(buffer)
}

func (p *PeerImpl) NotifyUpdate(chunk_id uint64) {
	p.cmd_ch <- command{
		cmdId: peer_cmd_notify_update,
		args:  chunk_id,
	}
}

func (p *PeerImpl) ReconfigureNetwork() {
	p.cmd_ch <- command{
		cmdId: peer_cmd_reconfigure_net,
	}
}

func (p *PeerImpl) BootstrapNetwork(peers []string) {
	p.cmd_ch <- command{
		cmdId: peer_cmd_bootstrap_net,
		args:  peers,
	}
}

func (p *PeerImpl) ServeSendRate(period time.Duration) {
	peer_log.Infof("start rate %v", period)
	ticker := time.NewTicker(period).C
	p.rate_ch = make(chan bool)

	for {
		select {
		case <-p.quit:
			return
		case <-ticker:
			p.rate_ch <- true
		default:
		}
	}
}

func (p *PeerImpl) ServeInfiniteSendRate() {
	peer_log.Infof("start unlimited rate")
	p.rate_ch = make(chan bool)
	for {
		select {
		case <-p.quit:
			return
		default:
			p.rate_ch <- true
		}
	}
}

func (p *PeerImpl) Serve() {

	if p.sendPeriod == 0 {
		go p.ServeInfiniteSendRate()
	} else {
		go p.ServeSendRate(p.sendPeriod)
	}

	if p.listenAddr != "" {
		go p.ServeConnections()
	}

	for {
		select {
		case <-p.quit:
			return
		case cmd := <-p.cmd_ch:
			switch cmd.cmdId {
			case peer_cmd_close:
				p.handleCmdClose(cmd)
			case peer_cmd_open:
				p.handleCmdOpen(cmd)
			case peer_cmd_notify_update:
				p.handleCmdNotifyUpdate(cmd)
			case peer_cmd_reconfigure_net:
				p.handleCmdReconfigureNetwork(cmd)
			case peer_cmd_bootstrap_net:
				p.handleCmdBootstrapNetwork(cmd)
			case peer_cmd_net_stat:
				p.handleCmdNetworkStatus(cmd)
			case peer_cmd_exit:
				p.handleCmdExit(cmd)
			default:
				p.handleCmdUnexpected(cmd)
			}
		case chunk := <-p.In:
			p.NotifyUpdate(chunk.Id)
			p.buf_input <- chunk
		case <-p.rate_ch:
			p.handleSend()
		}
	}
}

func (p *PeerImpl) ServeConnections() {
	peer_log.Infof("Start peer")
	listen, err := net.Listen("tcp", p.listenAddr)
	if err != nil {
		panic(err)
	}

	p.port = listen.Addr().(*net.TCPAddr).Port
	p.host = listen.Addr().(*net.TCPAddr).IP.String()

	for {
		select {
		case <-p.quit:
			return
		default:
		}

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

func (p *PeerImpl) getConnNumber() int {
	p.conn_counter_mut.Lock()
	new_num := p.conn_counter
	p.conn_counter += 1
	p.conn_counter_mut.Unlock()
	return new_num
}

func (p *PeerImpl) createSourceConnection(addr string) {
	peer_log.Infof("Start source connection")
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		peer_log.Errorf("New source conneciton error %v", err)
		return
	}

	new_num := p.getConnNumber()

	new_conn := NewConnection(fmt.Sprintf("%d", new_num), conn, CONN_RECV, p)
	go new_conn.Serve()

}

func (p *PeerImpl) createSinkConnection(addr string) {
	peer_log.Infof("Start sink connection")
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		peer_log.Errorf("New sink conneciton error %v", err)
		return
	}
	new_num := p.getConnNumber()
	new_conn := NewConnection(fmt.Sprintf("%d", new_num), conn, CONN_SEND, p)
	go new_conn.Serve()
}

func (p *PeerImpl) handleCmdNotifyUpdate(cmd command) {
	chunk_id := cmd.args.(uint64)
	peer_log.Warningf("Got update %v", chunk_id)
	peer_log.Debugf("src_conn %v", p.src_conn)
	for _, conn := range p.src_conn {
		conn.SendUpdateChunk(chunk_id)
	}
}

func (p *PeerImpl) handleCmdReconfigureNetwork(cmd command) {
	peer_log.Warningf("Do reconfigure %v", cmd)
	// TODO add reconfigure
}

func (p *PeerImpl) handleCmdBootstrapNetwork(cmd command) {
	peer_log.Infof("Perform bootstrap %v", cmd)
	peers := cmd.args.([]string)
	for _, peer := range peers {
		p.createSourceConnection(peer)
	}
}

func (p *PeerImpl) handleCmdNetworkStatus(cmd command) {
	peer_log.Infof("get network status %v", cmd)
	sinks := make([]PeerStat, 0)

	for _, conn := range p.sink_conn {
		host := conn.PeerHost
		port := conn.PeerPort
		source_count := 0
		sink_count := 0
		if conn.PeerNeighbours != nil {
			source_count = len(conn.PeerNeighbours.Sources)
			sink_count = len(conn.PeerNeighbours.Sinks)
		}
		sinks = append(sinks,
			PeerStat{
				Host:        host,
				Port:        port,
				SourceCount: source_count,
				SinkCount:   sink_count,
			})
	}

	sources := make([]PeerStat, 0)
	for _, conn := range p.src_conn {
		host := conn.PeerHost
		port := conn.PeerPort
		source_count := 0
		sink_count := 0
		if conn.PeerNeighbours != nil {
			source_count = len(conn.PeerNeighbours.Sources)
			sink_count = len(conn.PeerNeighbours.Sinks)
		}
		sources = append(sinks,
			PeerStat{
				Host:        host,
				Port:        port,
				SourceCount: source_count,
				SinkCount:   sink_count,
			})
	}

	cmd.resp <- PeerNeighboursState{
		Sources: sources,
		Sinks:   sinks,
	}
}

func (p *PeerImpl) handleCmdExit(cmd command) {
	peer_log.Warningf("Do exit %v", cmd)

	for _, c := range p.sink_conn {
		c.Close()
	}
	for _, c := range p.src_conn {
		c.Close()
	}
	close(p.quit)
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
	if len(p.sink_conn) == 0 {
		peer_log.Warningf("No clients")
		return
	}

	sinks := make([]*Connection, 0, len(p.sink_conn))

	for _, c := range p.sink_conn {
		sinks = append(sinks, c)
	}

	peer_log.Debugf("sinks %#v", sinks)
	//peer_log.Debugf("buf %#v", p.buf)

	var conn *Connection
	var chunk *Chunk
	for i := 0; i < 10 && chunk == nil; i++ {
		conn = sinks[rand.Intn(len(sinks))]
		peer_log.Debugf("check sink %#v", conn)

		peer_b := conn.PeerBuffer
		if peer_b == nil {
			peer_log.Debugf("peer buf not ready %#v", conn)
			// skip peer without state
			continue
		}

		chunks := peer_b.Chunks
		peer_log.Debugf("conn chunks %#v", chunks)
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
	resp_ch := make(chan interface{})
	p.cmd_ch <- command{
		cmdId: peer_cmd_net_stat,
		resp:  resp_ch,
	}
	return (<-resp_ch).(PeerNeighboursState)
}

func (p *PeerImpl) Buffer() BufferState {
	return p.buf.State()
}

func (p *PeerImpl) InChunks() chan<- *Chunk {
	return p.In
}

func (p *PeerImpl) ConnectionClosed(c *Connection) {
	p.cmd_ch <- command{
		cmdId: peer_cmd_close,
		args:  c,
	}
}

func (p *PeerImpl) ConnectionOpened(c *Connection) {
	peer_log.Debugf("connection ready %v", c)
	p.cmd_ch <- command{
		cmdId: peer_cmd_open,
		args:  c,
	}
}

func (p *PeerImpl) Host() string {
	return p.host
}
func (p *PeerImpl) Port() int {
	return p.port
}

func (p *PeerImpl) Exit() {
	p.cmd_ch <- command{
		cmdId: peer_cmd_exit,
	}
}
