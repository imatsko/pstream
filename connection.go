package pstream

import (
	"encoding/gob"
	"net"
	"github.com/op/go-logging"
	"time"
)


var conn_log = logging.MustGetLogger("connection")

const (
	PROTO_INIT = 0
	PROTO_UPDATE = 1
	PROTO_CLOSE = 2
	PROTO_ERR = 3
	PROTO_DATA = 4
	PROTO_ASK_UPDATE = 5

	CONN_RECV = 1
	CONN_SEND = 2

	conn_cmd_init = 1
	conn_cmd_close = 2
	conn_cmd_send_update = 3
	conn_cmd_send_data = 4
	conn_cmd_send_ask_update = 5


	CONN_UPDATE_PERIOD = 5*time.Second
	CONN_ASK_UPDATE_PERIOD = 10*time.Second
)

type ProtocolMessage struct {
	MsgType int
	Payload interface{}
}

type InitMessage struct {
	SelfId string
}

type DataMessage struct {
	Chunk Chunk
}

type UpdateMessage struct {
	Buffer     BufferState
	Neighbours PeerNeighboursState
}

type command struct {
	cmdId int
	args  interface{}
	resp  chan interface{}
}

type Connection struct {
	Peer           Peer
	ConnType       int
	ConnId         string
	PeerId         string
	PeerBuffer     BufferState
	PeerNeighbours PeerNeighboursState
	stream         net.Conn
	log            *logging.Logger
	cmd_ch         chan command
	in_msg         chan ProtocolMessage
	out_msg        chan ProtocolMessage
	close          chan bool
}

func NewConnection(id string, conn net.Conn, t int, peer Peer) *Connection {
	c := new(Connection)
	c.ConnId = id
	c.stream = conn
	c.Peer = peer
	c.ConnType = t
	c.cmd_ch = make(chan command, 16)
	c.in_msg = make(chan ProtocolMessage)
	c.out_msg = make(chan ProtocolMessage)
	c.close = make(chan bool)

	return c
}

func (c *Connection) Close() error {
	c.cmd_ch <- command{cmdId:conn_cmd_close}
	return nil
}

func (c *Connection) SendUpdate(){
	c.cmd_ch <- command{cmdId:conn_cmd_send_update}
}

func (c *Connection) AskUpdate(){
	c.cmd_ch <- command{cmdId:conn_cmd_send_ask_update}
}

func (c *Connection) Send(chunk *Chunk){
	c.cmd_ch <- command{
		cmdId: conn_cmd_send_data,
		args: chunk,
	}
}

func (c *Connection) Serve() {
	go c.serveRecv()
	go c.serveSend()

	go c.sendInit()

	go c.SendUpdate()
	go c.sendAskUpdate()

	for {
		select {
		case <-c.close:
			conn_log.Warningf("connection %d: quit received", c.ConnId)
			return
		case cmd := <-c.cmd_ch:
			switch cmd.cmdId {
			case conn_cmd_init:
				c.handleCmdInit(cmd)
			case conn_cmd_close:
				c.handleCmdClose(cmd)
			case conn_cmd_send_update:
				c.handleCmdSendUpdate(cmd)
			case conn_cmd_send_ask_update:
				c.handleCmdSendAskUpdate(cmd)
			case conn_cmd_send_data:
				c.handleCmdSendData(cmd)
			default:
				c.handleCmdUnexpected(cmd)
			}
		// pass
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
			case PROTO_CLOSE:
				c.handleMsgClose(msg)
			default:
				c.handleUnexpected(msg)
			}

		default:
		}
	}
}

func (c *Connection) sendInit() {
	cmd := command{
		cmdId: conn_cmd_init,
	}
	c.cmd_ch <- cmd
}

func (c *Connection) sendUpdate() {
	if c.ConnType != CONN_RECV {
		return
	}

	t := time.NewTicker(CONN_UPDATE_PERIOD)

	for {
		select {
		case <-c.close:
		//close connection
			conn_log.Warningf("connection %d: Handle close connection", c.ConnId)
			return
		case <- t.C:
			c.SendUpdate()
		}
	}
}

func (c *Connection) sendAskUpdate() {
	if c.ConnType != CONN_SEND {
		return
	}

	t := time.NewTicker(CONN_ASK_UPDATE_PERIOD)

	for {
		select {
		case <-c.close:
		//close connection
			conn_log.Warningf("connection %d: Handle close connection", c.ConnId)
			return
		case <- t.C:
			c.AskUpdate()
		}
	}
}


func (c *Connection) handleCmdInit(cmd command) {
	init := InitMessage{
		SelfId: c.Peer.SelfId(),
	}
	msg := ProtocolMessage{
		MsgType: PROTO_INIT,
		Payload: init,
	}
	c.out_msg <- msg
}

func (c *Connection) handleCmdClose(cmd command) {
	conn_log.Warningf("connection %d: Got close cmd", c.ConnId)
	c.Peer.ConnectionClosed(c)
	close(c.close)
}

func (c *Connection) handleCmdSendData(cmd command) {
	chunk := cmd.args.(Chunk)
	data_msg := DataMessage{
		Chunk: chunk,
	}

	answer_msg := ProtocolMessage{
		MsgType: PROTO_DATA,
		Payload: data_msg,
	}

	c.out_msg <- answer_msg
}

func (c *Connection) handleCmdSendUpdate(cmd command) {
	upd_msg := UpdateMessage{
		Buffer: c.Peer.Buffer(),
		Neighbours: PeerNeighboursState{},
	}

	answer_msg := ProtocolMessage{
		MsgType: PROTO_UPDATE,
		Payload: upd_msg,
	}

	c.out_msg <- answer_msg
}

func (c *Connection) handleCmdSendAskUpdate(cmd command) {
	msg := ProtocolMessage{
		MsgType: PROTO_ASK_UPDATE,
	}

	c.out_msg <- msg
}

func (c *Connection) handleCmdUnexpected(cmd command) {
	conn_log.Warningf("connection %d: Got unexpected comand %#v", c.ConnId, cmd)
}

func (c *Connection) handleMsgInit(msg ProtocolMessage) {
	init := msg.Payload.(InitMessage)
	c.PeerId = init.SelfId
}

func (c *Connection) handleMsgData(msg ProtocolMessage) {
	if c.ConnType != CONN_RECV {
		conn_log.Warningf("connection %d: Only receivers can store new data", c.ConnId)
		return
	}

	data := msg.Payload.(DataMessage)
	c.Peer.InChunks() <- &data.Chunk
}

func (c *Connection) handleMsgAskUpdate(msg ProtocolMessage) {
	if c.ConnType != CONN_RECV {
		conn_log.Warningf("connection %d: Only receivers can send state", c.ConnId)
		return
	}

	c.SendUpdate()
}

func (c *Connection) handleMsgUpdate(msg ProtocolMessage) {
	if c.ConnType != CONN_SEND {
		conn_log.Warningf("connection %d: Only senders can recv update", c.ConnId)
		return
	}

	state := msg.Payload.(UpdateMessage)
	c.PeerBuffer = state.Buffer
	c.PeerNeighbours = state.Neighbours
}

func (c *Connection) handleMsgClose(msg ProtocolMessage) {
	conn_log.Warningf("connection %d: Got close msg", c.ConnId)
	c.Close()
}

func (c *Connection) handleUnexpected(msg ProtocolMessage) {
	conn_log.Warningf("connection %d: Got unexpected message %#v", c.ConnId, msg)
}

func (c *Connection) serveRecv() {
	decoder := gob.NewDecoder(c.stream)
	for {
		select {
		case <-c.close:
		//close connection
			conn_log.Warningf("connection %d: Handle close connection", c.ConnId)
			c.stream.Close()
			return
		default:
		}
		var recvMessage ProtocolMessage
		err := decoder.Decode(&recvMessage)
		if err != nil {
			if err.Error() == "EOF" {
				conn_log.Errorf("connection %d: input decode err %#v", c.ConnId, err)
				conn_log.Warningf("connection %d: Close connection by EOF", c.ConnId)
				c.in_msg <- ProtocolMessage{MsgType:PROTO_CLOSE}
				return
			}
			conn_log.Errorf("connection %d: input decode err %#v", c.ConnId, err)
			continue
		}
		conn_log.Debugf("connection %d: recv msg %#v", c.ConnId, recvMessage)
		c.in_msg <- recvMessage
	}
}

func (c *Connection) serveSend() {
	encoder := gob.NewEncoder(c.stream)
	for {
		select {
		case <-c.close:
			conn_log.Debugf("connection %d: quit send", c.ConnId)
			return
		default:
		}
		sendMessage := <-c.out_msg
		conn_log.Debugf("connection %d: send msg %#v", c.ConnId, sendMessage)
		err := encoder.Encode(sendMessage)
		if err != nil {
			conn_log.Errorf("connection %d: output encode err %v", c.ConnId, err)
			continue
		}
	}
}




