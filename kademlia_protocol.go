package pstream

import (
	"github.com/imatsko/kademlia"
	"errors"
	"log"
	"math/rand"
	"time"
)

const (
	KAD_PING_REQ        = 1
	KAD_PING_RES        = 2
	KAD_STORE_VALUE_REQ = 3
	KAD_STORE_VALUE_RES = 4
	KAD_FIND_VALUE_REQ  = 5
	KAD_FIND_VALUE_RES  = 6
	KAD_FIND_NODE_REQ   = 7
	KAD_FIND_NODE_RES   = 8
)

var call_timeout = 5 * time.Second
var call_table_clean_period = 15 * time.Second

type KademliaMessage struct {
	KademliaType int
	Id           int
	Data         interface{}
	Error        error
}

func NewResponseMsg(req *KademliaMessage, data interface{}, err error) KademliaMessage {
	res := KademliaMessage{}
	res.Id = req.Id
	switch req.KademliaType {
	case KAD_FIND_NODE_REQ:
		res.KademliaType = KAD_FIND_NODE_RES
	case KAD_FIND_VALUE_REQ:
		res.KademliaType = KAD_FIND_VALUE_RES
	case KAD_STORE_VALUE_REQ:
		res.KademliaType = KAD_STORE_VALUE_RES
	case KAD_PING_REQ:
		res.KademliaType = KAD_PING_RES
	}
	res.Data = data
	res.Error = err
	return res
}

func NewRequestMsg(kademlia_type int, id int, req_data interface{}) KademliaMessage {
	req := KademliaMessage{}
	req.Id = id
	req.KademliaType = kademlia_type
	req.Data = req_data
	return req
}

//===============================================================================
// RPC CLIENT
//===============================================================================

type KademliaRPCClient struct {
	wait_response map[int]*KademliaCall
	CallChan      chan *KademliaCall
	ResMsgChan    chan *KademliaMessage
	ReqMsgChan    chan *KademliaMessage
}

type KademliaCall struct {
	KademliaType int
	Ts           time.Time
	Id           int
	ReqData      interface{}
	ResData      chan interface{}
	ResError     chan interface{}
}

func (c *KademliaRPCClient) CallRPC(req_type int, req interface{}) (res interface{}, err error) {
	call := &KademliaCall{
		KademliaType: req_type,
		Ts:           time.Now(),
		Id:           rand.Int(),
		ReqData:      req,
		ResData:      make(chan interface{}),
		ResError:     make(chan interface{}),
	}

	c.CallChan <- call
	select {
	case err := <-call.ResError:
		if v, ok := err.(error); ok {
			return nil, v
		} else {
			return nil, errors.New("error type mismatch")
		}
	case res := <-call.ResData:
		return res, nil
	case <-time.After(call_timeout):
		return nil, errors.New("call timeout")
	}
}

func (c *KademliaRPCClient) ServeClient() {
	cleanTableTicker := time.NewTicker(call_table_clean_period)
	for {
		select {
		case call := <-c.CallChan:
			c.wait_response[call.Id] = call
			req := NewRequestMsg(call.KademliaType, call.Id, call.ReqData)
			c.ReqMsgChan <- &req
		case res := <-c.ResMsgChan:
			if call, ok := c.wait_response[res.Id]; ok {
				if res.Error != nil {
					call.ResError <- res.Error
				} else {
					call.ResData <- res.Data
				}
				delete(c.wait_response, call.Id)
			}
		case <-cleanTableTicker.C:
			for id, call := range c.wait_response {
				if time.Now().Sub(call.Ts) > call_table_clean_period {
					delete(c.wait_response, id)
				}
			}
		}

	}
}

func (c *KademliaRPCClient) FindNode(req kademlia.FindNodeRequest, res *kademlia.FindNodeResponse) (err error) {
	call_res, err := c.CallRPC(KAD_FIND_NODE_REQ, req)
	if err != nil {
		return err
	}

	if v, ok := call_res.(kademlia.FindNodeResponse); ok {
		*res = v
		return nil
	}
	return errors.New("response type mismatch")
}

func (c *KademliaRPCClient) FindValue(req kademlia.FindValueRequest, res *kademlia.FindValueResponse) (err error) {
	call_res, err := c.CallRPC(KAD_FIND_VALUE_REQ, req)
	if err != nil {
		return err
	}

	if v, ok := call_res.(kademlia.FindValueResponse); ok {
		*res = v
		return nil
	}
	return errors.New("response type mismatch")
}

func (c *KademliaRPCClient) StoreValue(req kademlia.StoreValueRequest, res *kademlia.StoreValueResponse) (err error) {
	call_res, err := c.CallRPC(KAD_STORE_VALUE_REQ, req)
	if err != nil {
		return err
	}

	if v, ok := call_res.(kademlia.StoreValueResponse); ok {
		*res = v
		return nil
	}
	return errors.New("response type mismatch")
}

func (c *KademliaRPCClient) Ping(req kademlia.PingRequest, res *kademlia.PingResponse) (err error) {
	call_res, err := c.CallRPC(KAD_PING_REQ, req)
	if err != nil {
		return err
	}

	if v, ok := call_res.(kademlia.PingResponse); ok {
		*res = v
		return nil
	}
	return errors.New("response type mismatch")
}

//===============================================================================
// RPC HANDLER
//===============================================================================

type KademliaRPCHandler struct {
	ReqMsgChan chan *KademliaMessage
	ResMsgChan chan *KademliaMessage

	kad kademlia.KademliaNodeHandler
}

func (c *KademliaRPCHandler) ServeHandler() {
	for {
		req := <-c.ReqMsgChan

		switch req.KademliaType {
		case KAD_FIND_NODE_REQ:
			c.findNodeHandler(req)
		case KAD_FIND_VALUE_REQ:
			c.findValueHandler(req)
		case KAD_STORE_VALUE_REQ:
			c.storeValueHandler(req)
		case KAD_PING_REQ:
			c.pingHandler(req)
		default:
			log.Printf("Unexpected message %#v", req)
		}
	}
}

func (c *KademliaRPCHandler) findNodeHandler(req *KademliaMessage) {
	var res KademliaMessage
	if call_req, ok := req.Data.(kademlia.FindNodeRequest); ok {
		call_res := kademlia.FindNodeResponse{}
		err := c.kad.FindNodeHandler(call_req, &call_res)
		if err != nil {
			res = NewResponseMsg(req, nil, err)
		} else {
			res = NewResponseMsg(req, call_res, nil)
		}
	} else {
		res = NewResponseMsg(req, nil, errors.New("Request data type mismatch"))
	}
	c.ResMsgChan <- &res
}

func (c *KademliaRPCHandler) findValueHandler(req *KademliaMessage) {
	var res KademliaMessage
	if call_req, ok := req.Data.(kademlia.FindValueRequest); ok {
		call_res := kademlia.FindValueResponse{}
		err := c.kad.FindValueHandler(call_req, &call_res)
		if err != nil {
			res = NewResponseMsg(req, nil, err)
		} else {
			res = NewResponseMsg(req, call_res, nil)
		}
	} else {
		res = NewResponseMsg(req, nil, errors.New("Request data type mismatch"))
	}
	c.ResMsgChan <- &res
}

func (c *KademliaRPCHandler) storeValueHandler(req *KademliaMessage) {
	var res KademliaMessage
	if call_req, ok := req.Data.(kademlia.StoreValueRequest); ok {
		call_res := kademlia.StoreValueResponse{}
		err := c.kad.StoreValueHandler(call_req, &call_res)
		if err != nil {
			res = NewResponseMsg(req, nil, err)
		} else {
			res = NewResponseMsg(req, call_res, nil)
		}
	} else {
		res = NewResponseMsg(req, nil, errors.New("Request data type mismatch"))
	}
	c.ResMsgChan <- &res
}

func (c *KademliaRPCHandler) pingHandler(req *KademliaMessage) {
	var res KademliaMessage
	if call_req, ok := req.Data.(kademlia.PingRequest); ok {
		call_res := kademlia.PingResponse{}
		err := c.kad.PingHandler(call_req, &call_res)
		if err != nil {
			res = NewResponseMsg(req, nil, err)
		} else {
			res = NewResponseMsg(req, call_res, nil)
		}
	} else {
		res = NewResponseMsg(req, nil, errors.New("Request data type mismatch"))
	}
	c.ResMsgChan <- &res
}

//=====================================================================================
// PROTOCOL HANDLER
//=====================================================================================

type KademliaProtoHandler struct {
	sendChan chan interface{}
	recvChan chan interface{}
	//TODO close handlers
	quit     chan bool
	handler  *KademliaRPCHandler
	client   *KademliaRPCClient
}

func NewKademliaProtoHandler(handler *KademliaRPCHandler, client *KademliaRPCClient) *KademliaProtoHandler {
	h := new(KademliaProtoHandler)
	h.sendChan = make(chan interface{})
	h.sendChan = make(chan interface{})
	h.quit = make(chan bool)
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
			default:
			}
		}
		if h.client != nil {
			select {
			case kadMsg := <-h.client.ReqMsgChan:
				h.sendChan <- kadMsg
			default:
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
