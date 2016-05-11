package pstream

import (
	"errors"
	"github.com/op/go-logging"
	"time"
)

var log = logging.MustGetLogger("StreamBuffer")

type StreamChunk struct {
	Id   uint64
	Data interface{}
}

const (
	SB_SIZE                = 100
	SB_MAX_SIZE            = 200
	SB_NEXT_CHUNK_PERIOD   = 100 * time.Millisecond // 10 chunks per second
	SB_NEXT_CHUNK_DEADLINE = 5 * SB_NEXT_CHUNK_PERIOD
	sb_cmd_state           = 1
)

type streamBufferCmd struct {
	cmdId int
	args  interface{}
	resp  chan interface{}
}

type StreamBufferState struct {
	LastId uint64
	Chunks []uint64
}

type StreamBuffer struct {
	buf    []*StreamChunk
	lastId uint64
	cmd_ch chan streamBufferCmd
	BufOut chan<- *StreamChunk
	BufIn  <-chan *StreamChunk
}

func NewStreamBuffer(in <-chan *StreamChunk, out chan<- *StreamChunk) *StreamBuffer {
	sb := new(StreamBuffer)
	sb.buf = make([]*StreamChunk, 0, SB_SIZE)
	sb.cmd_ch = make(chan streamBufferCmd)
	sb.BufOut = out
	sb.BufIn = in
	return sb
}

func (sb *StreamBuffer) insert(c *StreamChunk) bool {
	if len(sb.buf) == 0 {
		sb.buf = append(sb.buf, c)
		sb.cleanup()
		return true
	}
	var pos int
	for pos = len(sb.buf) - 1; pos >= 0 && sb.buf[pos].Id > c.Id; pos -= 1 {
	}
	pos += 1

	if pos != 0 && sb.buf[pos-1].Id == c.Id {
		// repeated id
		return false
	}

	sb.buf = append(sb.buf[:pos], append([]*StreamChunk{c}, sb.buf[pos:]...)...)
	sb.cleanup()
	return true
}

func (sb *StreamBuffer) cleanup() {
	if len(sb.buf) <= SB_MAX_SIZE {
		return
	}
	rm_pos := len(sb.buf) - SB_MAX_SIZE - 1
	sb.buf = sb.buf[rm_pos:]
}

func (sb *StreamBuffer) next_id(cur_id uint64) (n_id uint64, pos int, err error) {
	for i := 0; i < len(sb.buf); i += 1 {
		if sb.buf[i].Id > cur_id {
			return sb.buf[i].Id, i, nil
		}
	}
	return 0, 0, errors.New("next id not found")
}

func (sb *StreamBuffer) collectState() StreamBufferState {
	chunks := make([]uint64,0,10)
	for i := 0; i < len(sb.buf); i += 1 {
		if sb.buf[i].Id >= sb.lastId{
			chunks = append(chunks, sb.buf[i].Id)
		}
	}

	return StreamBufferState{
		LastId: sb.lastId,
		Chunks: chunks,
	}
}

func (sb *StreamBuffer) State() StreamBufferState {
	resp_ch := make(chan interface{})
	sb.cmd_ch <- streamBufferCmd{
		cmdId: sb_cmd_state,
		resp:  resp_ch,
	}
	r_i := <-resp_ch
	return r_i.(StreamBufferState)
}

func (sb *StreamBuffer) sendAny() bool {
	sent := false
	for {

		nextId, pos, err := sb.next_id(sb.lastId)
		if err != nil {
			log.Infof("Next chunk for %d not found (%v)", sb.lastId, err)
			break
		}
		if nextId != sb.lastId+1 {
			log.Infof("Next chunk for %d is %d and not following, wait", sb.lastId, nextId)
			break
		}
		sb.BufOut <- sb.buf[pos]
		sb.lastId = nextId
		sent = true
		log.Debugf("Chunk %d sent", nextId)
	}
	return sent
}

func (sb *StreamBuffer) Serve() {
	deadline_ch := time.After(SB_NEXT_CHUNK_DEADLINE)
	for {
		select {
		case cmd := <-sb.cmd_ch:
			switch cmd.cmdId {
			case sb_cmd_state:
				log.Debugf("Got collect state %#v", cmd)
				cmd.resp <- sb.collectState()
			default:
				log.Debugf("Got undefined command %#v", cmd)
			}
		case c := <-sb.BufIn:
			log.Debugf("Got chunk %d", c.Id)
			if c.Id <= sb.lastId {
				log.Infof("Chunk %d already played, skip", c.Id)
				continue
			}
			sb.insert(c)
			if sb.sendAny() {
				deadline_ch = time.After(SB_NEXT_CHUNK_DEADLINE)
			}
		case <-deadline_ch:
			deadline_ch = time.After(SB_NEXT_CHUNK_DEADLINE)
			nextId, pos, err := sb.next_id(sb.lastId)
			if err != nil {
				log.Infof("Next chunk for %d not found (%v) on deadline", sb.lastId, err)
				continue
			}
			sb.BufOut <- sb.buf[pos]
			sb.lastId = nextId
			log.Debugf("Chunk %d sent by deadline", nextId)
			// trigger send rest ready chunks
			if sb.sendAny() {
				deadline_ch = time.After(SB_NEXT_CHUNK_DEADLINE)
			}
		}
	}
}
