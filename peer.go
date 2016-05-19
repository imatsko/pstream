package pstream

import (
	rand_c "crypto/rand"
	"encoding/hex"
	"fmt"
	"github.com/op/go-logging"
	"math"
	"net"
	"sync"
	"time"
)

const STREAM_CHUNK_PERIOD = time.Millisecond * 100

const (
	PEER_MIN_SOURCES = 2
	PEER_MIN_SINKS   = 1

	PEER_NETWORK_RECONFIGURE_PERIOD      = time.Second * 5
	PEER_NETWORK_RECONFIGURE_PERIOD_INIT = time.Second * 1
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

type logger struct {
	prefix string
	log    *logging.Logger
}

func NewLogger(l *logging.Logger, p string) *logger {
	new_l := new(logger)
	new_l.prefix = p
	new_l.log = l
	return new_l
}

func (l *logger) Printf(format string, args ...interface{}) {
	l.log.Infof(l.prefix+format, args...)
}

type PeerStat struct {
	Host        string
	Port        int
	Id          string
	SourceCount int
	SinkCount   int
}

type PeerNeighboursState struct {
	Sinks   []PeerStat
	Sources []PeerStat
	Connectivity int
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
	log        *logger
	selfId     string
	listenAddr string

	Fake_recv bool

	buf_input chan *Chunk
	buf       *Buffer

	In  chan *Chunk
	Out chan *Chunk

	send_rate float64
	rate_ch   chan bool

	addr_mut sync.Mutex
	port     int
	host     string

	quit   chan bool
	cmd_ch chan command

	conn_counter     int
	conn_counter_mut sync.Mutex

	sink_conn map[string]*Connection
	src_conn  map[string]*Connection

	sim_send *Semaphore
}

func NewPeer(selfId string, listen string, rate float64) *PeerImpl {
	p := new(PeerImpl)
	if selfId == "" {
		p.generateRandomId()
	} else {
		p.selfId = selfId
	}
	p.log = NewLogger(peer_log, fmt.Sprintf("PEER (%s): ", p.selfId))
	p.listenAddr = listen
	p.send_rate = rate

	p.In = make(chan *Chunk)
	p.Out = make(chan *Chunk, 2)

	p.buf_input = make(chan *Chunk)

	p.buf = NewBuffer(p.buf_input, p.Out, time.Duration(float64(STREAM_CHUNK_PERIOD)*5))

	p.cmd_ch = make(chan command, 32)
	p.quit = make(chan bool)

	p.conn_counter = 1

	p.sink_conn = make(map[string]*Connection)
	p.src_conn = make(map[string]*Connection)

	p.sim_send = NewSemaphore(3)
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

func (p *PeerImpl) ServeReconfigure(period time.Duration) {
	p.log.Printf("start reconfigure %v", period)
	ticker := time.NewTicker(period).C
	for {
		select {
		case <-p.quit:
			return
		case <-ticker:
			p.ReconfigureNetwork()
		}
	}
}

func (p *PeerImpl) ServeInfiniteSendRate() {
	p.log.Printf("start unlimited rate")
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

func (p *PeerImpl) ServeSendRate(rate float64) {
	p.log.Printf("start rate %v", rate)
	period := time.Duration(float64(STREAM_CHUNK_PERIOD) / rate)

	ticker := time.NewTicker(period).C
	p.rate_ch = make(chan bool)

	for {
		select {
		case <-p.quit:
			return
		case <-ticker:
			p.rate_ch <- true
		}
	}
}

func (p *PeerImpl) ServeSendRate2(rate float64) {
	p.log.Printf("start rate %v", rate)
	period := STREAM_CHUNK_PERIOD
	p.log.Printf("period %v", period)

	ticker := time.NewTicker(period).C

	//p.rate_ch = make(chan bool)
	p.rate_ch = make(chan bool, int(math.Ceil(rate)))
	var prev float64
	var count int64
	for {
		select {
		case <-p.quit:
			return
		default:
			if float64(count) <= prev {
				//p.log.Printf("send %v %v", count, prev)
				count += 1
				p.rate_ch <- true
			} else {
				//p.log.Printf("sleep %v %v", count, prev)
				prev += rate
				<-ticker
			}
		}
	}
}

func (p *PeerImpl) Serve() {

	if p.send_rate >= 100 {
		go p.ServeInfiniteSendRate()
	} else {
		go p.ServeSendRate2(p.send_rate)
	}

	if p.listenAddr != "" {
		go p.ServeConnections()
	}

	reconfigure_ticker := time.NewTicker(PEER_NETWORK_RECONFIGURE_PERIOD).C
	go func() {
		<-time.After(PEER_NETWORK_RECONFIGURE_PERIOD_INIT)
		p.ReconfigureNetwork()
	}()

	//go p.ServeReconfigure(PEER_NETWORK_RECONFIGURE_PERIOD)

	for {
		select {
		case <-p.quit:
			return
		case chunk := <-p.In:
			p.log.Printf("New chunk %d", chunk.Id)
			p.buf_input <- chunk
			p.NotifyUpdate(chunk.Id)
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
		case <-p.rate_ch:
			p.handleSend()
		case <-reconfigure_ticker:
			p.handleCmdReconfigureNetwork(command{})
		}
	}
}

func (p *PeerImpl) ServeConnections() {
	p.log.Printf("Start peer")
	listen, err := net.Listen("tcp", p.listenAddr)
	if err != nil {
		panic(err)
	}

	p.addr_mut.Lock()
	p.port = listen.Addr().(*net.TCPAddr).Port
	p.host = listen.Addr().(*net.TCPAddr).IP.String()
	p.addr_mut.Unlock()

	for {
		select {
		case <-p.quit:
			return
		default:
		}

		conn, err := listen.Accept() // this blocks until connection or error
		if err != nil {
			p.log.Printf("New conneciton error %v", err)
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
	p.log.Printf("Start source connection")
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		p.log.Printf("New source conneciton error %v", err)
		return
	}

	new_num := p.getConnNumber()

	new_conn := NewConnection(fmt.Sprintf("%d", new_num), conn, CONN_RECV, p)
	go new_conn.Serve()

}

func (p *PeerImpl) createSinkConnection(addr string) {
	p.log.Printf("Start sink connection")
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		p.log.Printf("New sink conneciton error %v", err)
		return
	}
	new_num := p.getConnNumber()
	new_conn := NewConnection(fmt.Sprintf("%d", new_num), conn, CONN_SEND, p)
	go new_conn.Serve()
}

func (p *PeerImpl) handleCmdNotifyUpdate(cmd command) {
	chunk_id := cmd.args.(uint64)
	//p.log.Printf("Notify update %v", chunk_id)
	for _, conn := range p.src_conn {
		conn.SendUpdateChunk(chunk_id)
	}
}

func (p *PeerImpl) handleCmdReconfigureNetwork(cmd command) {
	p.log.Printf("Do reconfigure %v", cmd)

	//p.reconfigureNetworkFixedN()
	for _, conn := range p.sink_conn {
		conn.FlushUsed()
	}
}

func (p *PeerImpl) handleCmdBootstrapNetwork(cmd command) {
	p.log.Printf("Perform bootstrap %v", cmd)
	peers := cmd.args.([]string)
	for _, peer := range peers {
		p.createSourceConnection(peer)
	}
	go func() {
		time.Sleep(PEER_NETWORK_RECONFIGURE_PERIOD_INIT)
		p.ReconfigureNetwork()
	}()
}

func (p *PeerImpl) collectNetworkStatus() PeerNeighboursState {
	collect_from_map := func(conn_map map[string]*Connection) []PeerStat {
		res := make([]PeerStat, 0)
		for _, conn := range conn_map {
			host := conn.PeerHost
			port := conn.PeerPort
			id := conn.PeerId
			source_count := 0
			sink_count := 0

			conn_neighbours := conn.Neighbours()
			if conn_neighbours != nil {
				source_count = len(conn_neighbours.Sources)
				sink_count = len(conn_neighbours.Sinks)
			}
			res = append(res,
				PeerStat{
					Id:          id,
					Host:        host,
					Port:        port,
					SourceCount: source_count,
					SinkCount:   sink_count,
				})
		}
		return res
	}

	sinks := collect_from_map(p.sink_conn)
	sources := collect_from_map(p.src_conn)

	return PeerNeighboursState{
		Sources: sources,
		Sinks:   sinks,
		Connectivity: len(sinks),
	}
}

func (p *PeerImpl) getIndirectPeers() map[string]PeerStat {
	res_map := make(map[string]PeerStat)

	collect_from_map := func(conn_map map[string]*Connection) {
		for _, conn := range conn_map {

			conn_neighbours := conn.Neighbours()
			if conn_neighbours == nil {
				p.log.Printf("DBG conn %v empty neigh", conn)
				continue
			}
			for _, peer := range conn_neighbours.Sources {
				if peer.Id == "" || peer.Host == "" || peer.Port == 0 {
					// skip not filled neighbours
					continue
				}
				if peer.Id == p.selfId {
					// skip self
					continue
				}
				_, ok_sink := p.sink_conn[peer.Id]
				if ok_sink {
					// skip already connected
					continue
				}
				_, ok_res := res_map[peer.Id]
				if ok_res {
					// skip already in result
					continue
				}
				res_map[peer.Id] = peer
			}
			for _, peer := range conn_neighbours.Sinks {
				if peer.Id == "" || peer.Host == "" || peer.Port == 0 {
					// skip not filled neighbours
					continue
				}
				if peer.Id == p.selfId {
					// skip self
					continue
				}

				_, ok_sink := p.sink_conn[peer.Id]
				if ok_sink {
					// skip already connected
					continue
				}
				_, ok_res := res_map[peer.Id]
				if ok_res {
					// skip already in result
					continue
				}
				res_map[peer.Id] = peer
			}

		}
	}

	collect_from_map(p.sink_conn)
	collect_from_map(p.src_conn)

	return res_map
}

func (p *PeerImpl) handleCmdNetworkStatus(cmd command) {
	p.log.Printf("get network status %v", cmd)

	res := p.collectNetworkStatus()
	cmd.resp <- res
}

func (p *PeerImpl) handleCmdExit(cmd command) {
	p.log.Printf("Do exit %v", cmd)

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
	p.log.Printf("Got closed connection %v", c)
	if _, ok := p.sink_conn[c.ConnId]; ok {
		p.log.Printf("Remove sink connection %v", c.ConnId)
		delete(p.sink_conn, c.ConnId)
	} else if _, ok := p.src_conn[c.ConnId]; ok {
		p.log.Printf("Remove src connection %v", c.ConnId)
		delete(p.src_conn, c.ConnId)
	} else {
		p.log.Printf("Unexpected closed connection %v", c.ConnId)
	}
}

func (p *PeerImpl) handleCmdOpen(cmd command) {
	c := cmd.args.(*Connection)
	p.log.Printf("Got new connection %v", c)

	var storage map[string]*Connection

	if c.ConnType == CONN_SEND {
		p.log.Printf("Add connection %v type: sink", c.ConnId)
		storage = p.sink_conn
	} else if c.ConnType == CONN_RECV {
		p.log.Printf("Add connection %v type: src", c.ConnId)
		storage = p.src_conn
	}

	if _, ok := storage[c.ConnId]; !ok {
		p.log.Printf("Add connection %v", c.ConnId)
		storage[c.ConnId] = c
	} else {
		p.log.Printf("connection %v already exists, close new", c.ConnId)
		c.Close()
	}
}

func (p *PeerImpl) handleCmdUnexpected(cmd command) {
	p.log.Printf("Got unexpected comand %#v", cmd)
}

func (p *PeerImpl) handleSend() {
	//p.handleSendRandom()
	p.handleSendDesired()
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
	p.cmd_ch <- command{
		cmdId: peer_cmd_open,
		args:  c,
	}
}

func (p *PeerImpl) Host() string {
	var h string
	p.addr_mut.Lock()
	h = p.host
	p.addr_mut.Unlock()
	return h
}
func (p *PeerImpl) Port() int {
	var port int
	p.addr_mut.Lock()
	port = p.port
	p.addr_mut.Unlock()
	return port
}

func (p *PeerImpl) Exit() {
	p.cmd_ch <- command{
		cmdId: peer_cmd_exit,
	}
}
