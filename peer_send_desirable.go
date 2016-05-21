package pstream

import (
	"math"
	"time"
)

//========================================================
// send most useful to desired
//=======================================================

func desirability_fun(sink_count int) float64 {
	return math.Max(math.Sqrt(float64(sink_count)), 1)
}

func select_with_latest_useful(a []sink_rate) []sink_rate {
	var max_id uint64

	for _, r := range a {
		if r.latest_useful.Id > max_id {
			max_id = r.latest_useful.Id
		}
	}

	latest_useful := make([]sink_rate, 0, len(a))

	for _, r := range a {
		if r.latest_useful.Id == max_id {
			latest_useful = append(latest_useful, r)
		}
	}

	return latest_useful
}

func (n *PeerNeighboursState) desirability() float64 {

	var sink_count int

	if n != nil {
		sink_count = len(n.Sinks)
	}

	return desirability_fun(sink_count)

}

type sink_rate struct {
	id            string
	latest_useful *Chunk
	d             float64
}

func (r sink_rate) Measure() float64 {
	return r.d
}

func select_random_proportionally(sinks []sink_rate) sink_rate {
	latest_useful := select_with_latest_useful(sinks)
	selectable := make([]Selectable, len(latest_useful))

	for i := 0; i < len(latest_useful); i += 1 {
		selectable[i] = latest_useful[i]
	}

	res := SelectRandomItemProportionally(selectable)
	return res.(sink_rate)
}

func (p *PeerImpl) handleSendDesired() {
	acq := p.sim_send.TryAcquireOne()
	if !acq {
		//p.log.Printf("Send sim lock")
		return
	}

	if len(p.sink_conn) == 0 {
		//p.log.Printf("No clients")
		p.sim_send.Release(1)
		return
	}

	//latestChunk := p.buf.Latest()

	sinks := make([]sink_rate, 0)
	for id, conn := range p.sink_conn {
		conn_buf := conn.Buffer()
		if conn_buf == nil {
			//p.log.Printf("Sink %v empty buf info", conn.ConnId)

			//if latestChunk != nil {
			//	sinks = append(sinks, sink_rate{id: id, latest_useful: latestChunk, d: 0})
			//}
			continue
		}
		latest_useful := p.buf.LatestUseful(conn_buf.Chunks)
		if latest_useful == nil {
			//p.log.Printf("Sink %v nothing useful", conn.ConnId)
			continue
		}

		//p.log.Printf("Sink %v useful %#v", conn.ConnId, latest_useful.Id)

		neighbours := conn.Neighbours()
		//p.log.Printf("Sink %v neighbours %#v", conn.ConnId, neighbours)

		des := neighbours.desirability()
		//p.log.Printf("Sink %v desirability %v", conn.ConnId, des)

		sinks = append(sinks, sink_rate{id: id, latest_useful: latest_useful, d: des})
	}
	if len(sinks) == 0 {
		//p.log.Printf("Nothing to send")
		p.sim_send.Release(1)
		return
	}

	selected_rate := select_random_proportionally(sinks)

	conn := p.sink_conn[selected_rate.id]
	chunk := selected_rate.latest_useful

	go func() {
		t1 := time.Now()
		p.log.Printf("Send chunk %v to sink %v", chunk.Id, conn)
		send, delivered := conn.Send(chunk)
		t2 := time.Now()
		p.log.Printf("Chunk %v to sink %v send %v delivered %v t_diff %v", chunk.Id, conn, send, delivered, t2.Sub(t1))
		p.sim_send.Release(1)
	}()
}
