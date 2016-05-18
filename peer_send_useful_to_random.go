package pstream

import "math/rand"

//========================================================
// send most useful to random
//=======================================================

func (p *PeerImpl) handleSendRandom() {
	if len(p.sink_conn) == 0 {
		p.log.Printf("No clients")
		return
	}

	sinks := make([]*Connection, 0, len(p.sink_conn))

	for _, c := range p.sink_conn {
		sinks = append(sinks, c)
	}

	var conn *Connection
	var chunk *Chunk
	for i := 0; i < 10 && chunk == nil; i++ {
		conn = sinks[rand.Intn(len(sinks))]

		peer_b := conn.Buffer()
		if peer_b == nil {
			p.log.Printf("nil buffer")

			// skip peer without state
			continue
		}

		chunks := peer_b.Chunks
		chunk = p.buf.LatestUseful(chunks)
	}
	if chunk == nil {
		p.log.Printf("Nothing to send (%v %v)", chunk, conn)
		return
	}

	p.log.Printf("Send chunk %v to sink %v", chunk.Id, conn.ConnId)
	conn.Send(chunk)
	p.log.Printf("Chunk %v to sink %v delivered", chunk.Id, conn.ConnId)
}
