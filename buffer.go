package pstream

import (
	"errors"
	"github.com/op/go-logging"
	"time"
)

var buf_log = logging.MustGetLogger("StreamBuffer")

type Chunk struct {
	Id   uint64
	time.Duration
	Data interface{}
}

const (
	SB_MAX_SIZE          = 50
	SB_PERIOD_SIZE          = 5
	SB_NEW_MAX_SIZE      = 40
	sb_cmd_state         = 1
	sb_cmd_latest_useful = 2
	sb_cmd_latest        = 3
)

type BufferState struct {
	LastId uint64
	Chunks []uint64
}

type Buffer struct {
	buf            []*Chunk
	lastId         uint64
	cmd_ch         chan command
	BufOut         chan<- *Chunk
	BufIn          <-chan *Chunk
	period time.Duration
	period_buf []time.Duration
	deadline_mult float64
}

func NewBuffer(in <-chan *Chunk, out chan<- *Chunk, deadline_mult float64) *Buffer {
	sb := new(Buffer)
	sb.buf = make([]*Chunk, 0, SB_MAX_SIZE)
	sb.period_buf = make([]time.Duration, 0, 3)
	sb.period = DEFAULT_STREAM_CHUNK_PERIOD
	sb.cmd_ch = make(chan command)
	sb.BufOut = out
	sb.BufIn = in
	sb.deadline_mult = deadline_mult

	go sb.Serve()

	return sb
}

func (sb *Buffer) update_period(d time.Duration) {
	if len(sb.period_buf) < SB_PERIOD_SIZE {
		sb.period_buf = append(sb.period_buf, d)
	} else {
		sb.period_buf = append(sb.period_buf[1:], d)
	}

	var sum time.Duration
	for _, d := range sb.period_buf {
		sum += d
	}
	sb.period = time.Duration(uint64(sum)/uint64(len(sb.period_buf)))
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
	rm_pos := len(sb.buf) - SB_NEW_MAX_SIZE

	new_len := len(sb.buf[rm_pos:])

	new_buf := make([]*Chunk, new_len)

	for i, c := range sb.buf[rm_pos:] {
		new_buf[i] = c
	}
	sb.buf = new_buf
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
	chunks := make([]uint64, 0, 10)
	for i := 0; i < len(sb.buf); i += 1 {
		if sb.buf[i].Id >= sb.lastId {
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

func (sb *Buffer) LatestUseful(chunks []uint64) *Chunk {
	if len(chunks) > SB_MAX_SIZE {
		rm_pos := len(chunks) - SB_MAX_SIZE - 1
		chunks = chunks[rm_pos:]
	}

	resp_ch := make(chan interface{})
	sb.cmd_ch <- command{
		cmdId: sb_cmd_latest_useful,
		args:  chunks,
		resp:  resp_ch,
	}
	r_i := <-resp_ch
	return r_i.(*Chunk)
}
func (sb *Buffer) Period() time.Duration {
	return sb.period
}

func (sb *Buffer) deadline() time.Duration {
	return time.Duration(float64(sb.period)*sb.deadline_mult)
}

func (sb *Buffer) Latest() *Chunk {
	resp_ch := make(chan interface{})
	sb.cmd_ch <- command{
		cmdId: sb_cmd_latest,
		resp:  resp_ch,
	}
	r_i := <-resp_ch
	return r_i.(*Chunk)
}

func (sb *Buffer) getLatest() *Chunk {
	if len(sb.buf) == 0 {
		return nil
	}
	return sb.buf[len(sb.buf)-1]
}

func (sb *Buffer) getLatestUseful(chunks []uint64) *Chunk {
	if len(chunks) == 0 && len(sb.buf) != 0 {
		return sb.buf[len(sb.buf)-1]
	}

	var latest *Chunk

	for i_c, i_b := len(chunks)-1, len(sb.buf)-1; i_c >= 0 && i_b >= 0; {
		if chunks[i_c] < sb.buf[i_b].Id {
			latest = sb.buf[i_b]
			break
		}
		if chunks[i_c] > sb.buf[i_b].Id {
			i_c = i_c - 1
			continue
		}
		if chunks[i_c] == sb.buf[i_b].Id {
			i_c = i_c - 1
			i_b = i_b - 1
		}
	}

	return latest
}

func (sb *Buffer) sendAny() bool {
	sent := false
	for {

		nextId, pos, err := sb.next_id(sb.lastId)
		if err != nil {
			buf_log.Infof("BUF: Next chunk for %d not found (%v)", sb.lastId, err)
			break
		}
		if nextId != sb.lastId+1 {
			buf_log.Infof("BUF: Next chunk for %d is %d and not following, wait", sb.lastId, nextId)
			break
		}
		sb.BufOut <- sb.buf[pos]
		sb.lastId = nextId
		sent = true
		buf_log.Debugf("BUF: Chunk %d sent", nextId)
	}
	return sent
}

func (sb *Buffer) Serve() {

	deadline_ch := time.After(sb.deadline())
	for {
		select {
		case cmd := <-sb.cmd_ch:
			switch cmd.cmdId {
			case sb_cmd_state:
				buf_log.Debugf("BUF: Got collect state %#v", cmd)
				cmd.resp <- sb.collectState()
			case sb_cmd_latest_useful:
				//buf_log.Debugf("BUF: Got latest useful")
				cmd.resp <- sb.getLatestUseful(cmd.args.([]uint64))
			case sb_cmd_latest:
				//buf_log.Debugf("BUF: Got latest")
				cmd.resp <- sb.getLatest()
			default:
				buf_log.Debugf("BUF: Got undefined command %#v", cmd)
			}
		case c := <-sb.BufIn:
			buf_log.Debugf("BUF: Got chunk %d", c.Id)
			if c.Id <= sb.lastId {
				buf_log.Infof("BUF: Chunk %d already played, skip", c.Id)
				continue
			}
			sb.insert(c)

			if sb.sendAny() {
				deadline_ch = time.After(sb.deadline())
			}
		case <-deadline_ch:
			deadline_ch = time.After(sb.deadline())
			nextId, pos, err := sb.next_id(sb.lastId)
			if err != nil {
				buf_log.Infof("BUF: Next chunk for %d not found (%v) on deadline", sb.lastId, err)
				continue
			}
			sb.BufOut <- sb.buf[pos]
			sb.lastId = nextId
			buf_log.Debugf("BUF: Chunk %d sent by deadline", nextId)
			// trigger send rest ready chunks
			if sb.sendAny() {
				deadline_ch = time.After(sb.deadline())
			}
		}
	}
}
