package pstream

import (
	"errors"
	"github.com/op/go-logging"
	"time"
)

var buf_log = logging.MustGetLogger("StreamBuffer")

type Chunk struct {
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


type BufferState struct {
	LastId uint64
	Chunks []uint64
}

type Buffer struct {
	buf    []*Chunk
	lastId uint64
	cmd_ch chan command
	BufOut chan<- *Chunk
	BufIn  <-chan *Chunk
}

func NewBuffer(in <-chan *Chunk, out chan<- *Chunk) *Buffer {
	sb := new(Buffer)
	sb.buf = make([]*Chunk, 0, SB_SIZE)
	sb.cmd_ch = make(chan command)
	sb.BufOut = out
	sb.BufIn = in
	return sb
}

func (sb *Buffer) insert(c *Chunk) bool {
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

	sb.buf = append(sb.buf[:pos], append([]*Chunk{c}, sb.buf[pos:]...)...)
	sb.cleanup()
	return true
}

func (sb *Buffer) cleanup() {
	if len(sb.buf) <= SB_MAX_SIZE {
		return
	}
	rm_pos := len(sb.buf) - SB_MAX_SIZE - 1
	sb.buf = sb.buf[rm_pos:]
}

func (sb *Buffer) next_id(cur_id uint64) (n_id uint64, pos int, err error) {
	for i := 0; i < len(sb.buf); i += 1 {
		if sb.buf[i].Id > cur_id {
			return sb.buf[i].Id, i, nil
		}
	}
	return 0, 0, errors.New("next id not found")
}

func (sb *Buffer) collectState() BufferState {
	chunks := make([]uint64,0,10)
	for i := 0; i < len(sb.buf); i += 1 {
		if sb.buf[i].Id >= sb.lastId{
			chunks = append(chunks, sb.buf[i].Id)
		}
	}

	return BufferState{
		LastId: sb.lastId,
		Chunks: chunks,
	}
}

func (sb *Buffer) State() BufferState {
	resp_ch := make(chan interface{})
	sb.cmd_ch <- command{
		cmdId: sb_cmd_state,
		resp:  resp_ch,
	}
	r_i := <-resp_ch
	return r_i.(BufferState)
}

func (sb *Buffer) sendAny() bool {
	sent := false
	for {

		nextId, pos, err := sb.next_id(sb.lastId)
		if err != nil {
			buf_log.Infof("Next chunk for %d not found (%v)", sb.lastId, err)
			break
		}
		if nextId != sb.lastId+1 {
			buf_log.Infof("Next chunk for %d is %d and not following, wait", sb.lastId, nextId)
			break
		}
		sb.BufOut <- sb.buf[pos]
		sb.lastId = nextId
		sent = true
		buf_log.Debugf("Chunk %d sent", nextId)
	}
	return sent
}

func (sb *Buffer) Serve() {
	deadline_ch := time.After(SB_NEXT_CHUNK_DEADLINE)
	for {
		select {
		case cmd := <-sb.cmd_ch:
			switch cmd.cmdId {
			case sb_cmd_state:
				buf_log.Debugf("Got collect state %#v", cmd)
				cmd.resp <- sb.collectState()
			default:
				buf_log.Debugf("Got undefined command %#v", cmd)
			}
		case c := <-sb.BufIn:
			buf_log.Debugf("Got chunk %d", c.Id)
			if c.Id <= sb.lastId {
				buf_log.Infof("Chunk %d already played, skip", c.Id)
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
				buf_log.Infof("Next chunk for %d not found (%v) on deadline", sb.lastId, err)
				continue
			}
			sb.BufOut <- sb.buf[pos]
			sb.lastId = nextId
			buf_log.Debugf("Chunk %d sent by deadline", nextId)
			// trigger send rest ready chunks
			if sb.sendAny() {
				deadline_ch = time.After(SB_NEXT_CHUNK_DEADLINE)
			}
		}
	}
}
