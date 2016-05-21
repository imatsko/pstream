package pstream

import (
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/op/go-logging"
	"net"
	"sync"
	"time"
)

var conn_log = logging.MustGetLogger("connection")

const (
	PROTO_INIT         = 1
	PROTO_UPDATE       = 2
	PROTO_CLOSE        = 3
	PROTO_DATA         = 4
	PROTO_ASK_UPDATE   = 5
	PROTO_UPDATE_CHUNK = 6

	CONN_UNDEFINED = 0
	CONN_RECV      = 1
	CONN_SEND      = 2

	conn_cmd_init              = 1
	conn_cmd_close             = 2
	conn_cmd_send_update       = 3
	conn_cmd_send_data         = 4
	conn_cmd_send_ask_update   = 5
	conn_cmd_send_update_chunk = 6

	conn_cmd_get_buffer     = 7
	conn_cmd_get_neighbours = 8
	conn_cmd_flush_used     = 9

	CONN_SEND_MSG_TIMEOUT = 5000 * time.Millisecond
	CONN_SEND_SIM_SEND_LOCK_TIMEOUT_MULT = 1

	CONN_UPDATE_PERIOD     = 5 * time.Second
	CONN_ASK_UPDATE_PERIOD = 10 * time.Second
)

type confirmMessage struct {
	msg  *ProtocolMessage
	conf chan bool
}

type ProtocolMessage struct {
	MsgType int
	Payload interface{}
}

type InitMessage struct {
	SelfId   string
	Host     string
	Port     int
	ConnType int
}

type DataMessage struct {
	Chunk Chunk
}

type UpdateMessage struct {
	Buffer     BufferState
	Neighbours PeerNeighboursState
}

type UpdateChunkMessage struct {
	NewChunk uint64
}

func init() {
	gob.Register(ProtocolMessage{})
	gob.Register(InitMessage{})
	gob.Register(DataMessage{})
	gob.Register(Chunk{})
	gob.Register(UpdateMessage{})
	gob.Register(UpdateChunkMessage{})
}

type command struct {
	cmdId int
	args  interface{}
	resp  chan interface{}
}

func (c Connection) String() string {
	return fmt.Sprintf("(CONN %v %v used=%v host=%v port=%v)", c.ConnId, c.PeerId, c.Used, c.PeerHost, c.PeerPort)
}

type Connection struct {
	Peer     Peer
	ConnId   string
	ConnType int

	PeerHost string
	PeerPort int
	PeerId   string

	Used bool

	// TODO protect buff and neighb state
	buf_mut          sync.Mutex
	buffer_state     *BufferState
	neighbours_mut   sync.Mutex
	neighbours_state *PeerNeighboursState
	stream           net.Conn
	log              *logging.Logger
	cmd_ch           chan command
	in_msg           chan ProtocolMessage
	out_msg          chan confirmMessage
	close            chan bool
	data_send_lock   chan bool
}

func NewConnection(id string, conn net.Conn, t int, peer Peer) *Connection {
	c := new(Connection)
	c.ConnId = id
	c.stream = conn
	c.Peer = peer
	c.ConnType = t
	c.cmd_ch = make(chan command, 2)
	c.in_msg = make(chan ProtocolMessage)
	c.out_msg = make(chan confirmMessage)
	c.close = make(chan bool)
	c.data_send_lock = make(chan bool, 1)

	return c
}

func (c *Connection) Close() error {
	c.cmd_ch <- command{cmdId: conn_cmd_close}
	return nil
}

func (c *Connection) SendUpdate() {
	c.cmd_ch <- command{cmdId: conn_cmd_send_update}
}

func (c *Connection) SendUpdateChunk(chunk_id uint64) {
	c.cmd_ch <- command{cmdId: conn_cmd_send_update_chunk, args: chunk_id}
}

func (c *Connection) AskUpdate() {
	c.cmd_ch <- command{cmdId: conn_cmd_send_ask_update}
}

func (c *Connection) FlushUsed() {
	c.cmd_ch <- command{cmdId: conn_cmd_flush_used}
}

func (c *Connection) LockSend() bool {
	select {
	case c.data_send_lock <- true:
		return true
	default:
		return false
	}
}

func (c *Connection) UnlockSend() {
	<-c.data_send_lock
}

func (c *Connection) Send(chunk *Chunk) (send bool, delivered bool) {
	l := c.LockSend()
	if !l {
		return false, false
	}

	resp_chan := make(chan interface{})
	c.cmd_ch <- command{
		cmdId: conn_cmd_send_data,
		args:  chunk,
		resp:  resp_chan,
	}

	select {
	case r := <-resp_chan:
		c.UnlockSend()
		return true, r.(bool)
	case <-time.After(CONN_SEND_SIM_SEND_LOCK_TIMEOUT_MULT*c.Peer.Buf().Period()):
		go func() {
			<-resp_chan
			c.UnlockSend()
		}()
		return false, false
	}
}

func (c *Connection) Buffer() *BufferState {
	//TODO protect
	//resp_chan := make(chan interface{})
	//c.cmd_ch <- command{
	//	cmdId: conn_cmd_get_buffer,
	//	resp:  resp_chan,
	//}
	//return (<-resp_chan).(*BufferState)

	c.buf_mut.Lock()
	defer c.buf_mut.Unlock()
	if c.buffer_state == nil {
		return nil
	}

	new_state := new(BufferState)
	new_state.LastId = c.buffer_state.LastId

	new_state.Chunks = make([]uint64, len(c.buffer_state.Chunks))
	copy(new_state.Chunks, c.buffer_state.Chunks)

	return new_state
}

func (c *Connection) Neighbours() *PeerNeighboursState {
	// TODO protect
	//resp_chan := make(chan interface{})
	//c.cmd_ch <- command{
	//	cmdId: conn_cmd_get_neighbours,
	//	resp:  resp_chan,
	//}
	//return (<-resp_chan).(*PeerNeighboursState)

	c.neighbours_mut.Lock()
	defer c.neighbours_mut.Unlock()
	if c.neighbours_state == nil {
		return nil
	}

	new_state := new(PeerNeighboursState)
	new_state.Sinks = c.neighbours_state.Sinks[:]
	new_state.Sources = c.neighbours_state.Sources[:]
	new_state.Connectivity = c.neighbours_state.Connectivity
	return new_state
}

func (c *Connection) Serve() {
	go c.serveRecv()
	go c.serveSend()

	if c.ConnType != CONN_UNDEFINED {
		go c.sheduleSendInit()
	}

	go c.SendUpdate()
	go c.scheduleSendUpdate()

	for {
		select {
		case <-c.close:
			c.stream.Close()
			//conn_log.Warningf("connection %d: quit received", c.ConnId)
			return
		case msg := <-c.in_msg:
			switch msg.MsgType {
			case PROTO_INIT:
				c.handleMsgInit(msg)
			case PROTO_DATA:
				c.handleMsgData(msg)
			case PROTO_ASK_UPDATE:
				c.handleMsgAskUpdate(msg)
			case PROTO_UPDATE:
				c.handleMsgUpdate(msg)
			case PROTO_UPDATE_CHUNK:
				c.handleMsgUpdateChunk(msg)
			case PROTO_CLOSE:
				c.handleMsgClose(msg)
			default:
				c.handleUnexpected(msg)
			}
		case cmd := <-c.cmd_ch:
			switch cmd.cmdId {
			case conn_cmd_init:
				c.handleCmdInit(cmd)
			case conn_cmd_close:
				c.handleCmdClose(cmd)
			case conn_cmd_send_update:
				c.handleCmdSendUpdate(cmd)
			case conn_cmd_send_update_chunk:
				c.handleCmdSendUpdateChunk(cmd)
			case conn_cmd_send_ask_update:
				c.handleCmdSendAskUpdate(cmd)
			case conn_cmd_send_data:
				c.handleCmdSendData(cmd)
			case conn_cmd_flush_used:
				c.handleCmdFlushUsed(cmd)
			case conn_cmd_get_buffer:
				c.handleCmdGetBuffer(cmd)
			case conn_cmd_get_neighbours:
				c.handleCmdGetNeighbours(cmd)
			default:
				c.handleCmdUnexpected(cmd)
			}
		}
	}
}

func (c *Connection) sheduleSendInit() {
	cmd := command{
		cmdId: conn_cmd_init,
	}
	c.cmd_ch <- cmd
}

func (c *Connection) scheduleSendUpdate() {
	if c.ConnType != CONN_RECV {
		return
	}

	t := time.NewTicker(CONN_UPDATE_PERIOD)

	for {
		select {
		case <-c.close:
			//close connection
			//conn_log.Warningf("connection %v: Handle close connection", c.ConnId)
			return
		case <-t.C:
			go c.SendUpdate()
		}
	}
}

func (c *Connection) scheduleSendAskUpdate() {
	if c.ConnType != CONN_SEND {
		return
	}

	t := time.NewTicker(CONN_ASK_UPDATE_PERIOD)

	for {
		select {
		case <-c.close:
			//close connection
			//conn_log.Warningf("connection %d: Handle close connection", c.ConnId)
			return
		case <-t.C:
			go c.AskUpdate()
		}
	}
}

func (c *Connection) handleCmdInit(cmd command) {
	if c.ConnType == CONN_UNDEFINED {
		//conn_log.Errorf("Unexpected send init type")
		return
	}

	init := InitMessage{
		SelfId:   c.Peer.SelfId(),
		ConnType: c.ConnType,
		Host:     c.Peer.Host(),
		Port:     c.Peer.Port(),
	}
	msg := ProtocolMessage{
		MsgType: PROTO_INIT,
		Payload: init,
	}
	c.out_msg <- confirmMessage{msg: &msg}
}

func (c *Connection) handleCmdClose(cmd command) {
	//conn_log.Warningf("connection %d: Got close cmd", c.ConnId)
	c.Peer.ConnectionClosed(c)
	_, not_closed := <-c.close
	if not_closed {
		close(c.close)
	}
}

func (c *Connection) handleCmdSendData(cmd command) {
	c.Used = true
	chunk := cmd.args.(*Chunk)
	data_msg := DataMessage{
		Chunk: *chunk,
	}

	answer_msg := ProtocolMessage{
		MsgType: PROTO_DATA,
		Payload: data_msg,
	}

	go func() {
		m := confirmMessage{
			msg:  &answer_msg,
			conf: make(chan bool, 1),
		}
		c.out_msg <- m
		conn_log.Warningf("%v: data send to sender", c)

		select {
		case r := <-m.conf:
			if r {
				c.updateChunks(chunk.Id)
			}
			cmd.resp <- r
		}
	}()

}

func (c *Connection) updateChunks(id uint64) {
	c.buf_mut.Lock()
	defer c.buf_mut.Unlock()

	if c.buffer_state == nil {
		return
	}
	if len(c.buffer_state.Chunks) == 0 {
		c.buffer_state.Chunks = append(c.buffer_state.Chunks, id)
		return
	}

	var pos int
	for pos = len(c.buffer_state.Chunks) - 1; pos >= 0 && c.buffer_state.Chunks[pos] > id; pos -= 1 {
	}
	pos += 1
	if pos != 0 && c.buffer_state.Chunks[pos-1] == id {
		// repeated id
		return
	}

	c.buffer_state.Chunks = append(c.buffer_state.Chunks[:pos], append([]uint64{id}, c.buffer_state.Chunks[pos:]...)...)
}

func (c *Connection) handleCmdSendUpdate(cmd command) {
	//conn_log.Infof("SEND update")

	upd_msg := UpdateMessage{
		Buffer:     c.Peer.Buffer(),
		Neighbours: c.Peer.Neighbours(),
	}
	//conn_log.Infof("SEND update %v", upd_msg)

	answer_msg := ProtocolMessage{
		MsgType: PROTO_UPDATE,
		Payload: upd_msg,
	}

	c.out_msg <- confirmMessage{msg: &answer_msg}
}

func (c *Connection) handleCmdSendUpdateChunk(cmd command) {
	chunk_id := cmd.args.(uint64)

	//conn_log.Infof("Send update chunk %v", chunk_id)

	upd_msg := UpdateChunkMessage{NewChunk: chunk_id}
	answer_msg := ProtocolMessage{
		MsgType: PROTO_UPDATE_CHUNK,
		Payload: upd_msg,
	}
	c.out_msg <- confirmMessage{msg: &answer_msg}
}

func (c *Connection) handleCmdSendAskUpdate(cmd command) {
	msg := ProtocolMessage{
		MsgType: PROTO_ASK_UPDATE,
	}

	c.out_msg <- confirmMessage{msg: &msg}
}

func (c *Connection) handleCmdFlushUsed(cmd command) {
	c.Used = false
	return
}

func (c *Connection) handleCmdGetBuffer(cmd command) {
	cmd.resp <- c.buffer_state
	return
}

func (c *Connection) handleCmdGetNeighbours(cmd command) {
	cmd.resp <- c.neighbours_state
	return
}

func (c *Connection) handleCmdUnexpected(cmd command) {
	//conn_log.Warningf("connection %d: Got unexpected comand %//v", c.ConnId, cmd)
}

func (c *Connection) handleMsgInit(msg ProtocolMessage) {
	conn_log.Warningf("%v: Got init %v", c, msg)

	init := msg.Payload.(InitMessage)
	c.PeerId = init.SelfId
	c.PeerHost = c.stream.RemoteAddr().(*net.TCPAddr).IP.String()
	c.PeerPort = init.Port


	if c.ConnType == CONN_UNDEFINED {
	// passive
		conn_log.Warningf("%v: Got init passive", c)
		switch init.ConnType {
		case CONN_RECV:
			c.ConnType = CONN_SEND
			c.Peer.ConnectionOpened(c)
			go c.sheduleSendInit()
		case CONN_SEND:
			c.ConnType = CONN_RECV
			c.Peer.ConnectionOpened(c)
			go c.sheduleSendInit()
		default:
			//conn_log.Errorf("Unexpected connection type %v", init)
			go c.Close()
		}
	} else {
		conn_log.Warningf("%v: Got init active", c)
		//active
		c.Peer.ConnectionOpened(c)
	}
}

func (c *Connection) handleMsgData(msg ProtocolMessage) {
	if c.ConnType != CONN_RECV {
		//conn_log.Warningf("connection %d: Only receivers can store new data", c.ConnId)
		return
	}

	data := msg.Payload.(DataMessage)
	c.Peer.AddChunk(&data.Chunk)
}

func (c *Connection) handleMsgAskUpdate(msg ProtocolMessage) {
	if c.ConnType != CONN_RECV {
		//conn_log.Warningf("connection %d: Only receivers can send state", c.ConnId)
		return
	}

	go c.SendUpdate()
}

func (c *Connection) handleMsgUpdate(msg ProtocolMessage) {
	state := msg.Payload.(UpdateMessage)
	//conn_log.Infof("Got update %v", state)
	c.buf_mut.Lock()
	c.buffer_state = &state.Buffer
	c.buf_mut.Unlock()
	//c.neighbours_mut.Lock()
	c.neighbours_state = &state.Neighbours
	//c.neighbours_mut.Unlock()
}

func (c *Connection) handleMsgUpdateChunk(msg ProtocolMessage) {
	if c.ConnType != CONN_SEND {
		//conn_log.Warningf("connection %d: Only senders can recv update", c.ConnId)
		return
	}

	chunk := msg.Payload.(UpdateChunkMessage)
	//conn_log.Infof("Got update chunk %v", chunk)
	c.updateChunks(chunk.NewChunk)
}

func (c *Connection) handleMsgClose(msg ProtocolMessage) {
	//conn_log.Warningf("connection %d: Got close msg", c.ConnId)
	c.Close()
}

func (c *Connection) handleUnexpected(msg ProtocolMessage) {
	//conn_log.Warningf("connection %d: Got unexpected message %//v", c.ConnId, msg)
}

func (c *Connection) serveRecv() {
	// TODO read timeout
	decoder := gob.NewDecoder(c.stream)
	for {
		select {
		case <-c.close:
			//close connection
			//conn_log.Warningf("connection %d: Handle close connection", c.ConnId)
			return
		default:
		}
		var recvMessage ProtocolMessage
		err := decoder.Decode(&recvMessage)
		if err != nil {
			if err.Error() == "EOF" {
				//conn_log.Errorf("connection %d: input decode err %//v", c.ConnId, err)
				//conn_log.Warningf("connection %d: Close connection by EOF", c.ConnId)
				c.in_msg <- ProtocolMessage{MsgType: PROTO_CLOSE}
			}
			conn_log.Errorf("connection %d: input decode err %//v", c.ConnId, err)
			continue
		}
		//conn_log.Debugf("connection %d: recv msg %//v", c.ConnId, recvMessage)
		c.in_msg <- recvMessage
	}
}

func (c *Connection) serveSend() {
	encoder := gob.NewEncoder(c.stream)
	for {
		select {
		case <-c.close:
			//conn_log.Debugf("connection %d: quit send", c.ConnId)
			return
		default:
		}

		conf_msg := <-c.out_msg
		sendMessage := *conf_msg.msg
		//conn_log.Debugf("connection %d: send msg %//v", c.ConnId, sendMessage)

		res := make(chan error)

		go func() {
			err := encoder.Encode(sendMessage)
			res <- err
		}()

		var err error
		select {
		case err = <-res:
		case <-time.After(CONN_SEND_MSG_TIMEOUT):
			err = errors.New("Message send timeout")
			conn_log.Errorf("connection %d: msg err err %v", c.ConnId, err)
		}
		if conf_msg.conf != nil {
			if err == nil {
				conf_msg.conf <- true
			} else {
				conf_msg.conf <- false
			}
		}

		if err != nil {
			conn_log.Errorf("connection %d: output encode err %v", c.ConnId, err)
			c.in_msg <- ProtocolMessage{MsgType: PROTO_CLOSE}
		}
	}
}
