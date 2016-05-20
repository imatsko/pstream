package pstream

import (
	"math/rand"
	"fmt"
)

const PEER_FIXED_COUNT = 7

//========================================================
// reconfigure sinks to fixed count
//=======================================================
func (p *PeerImpl) reconfigureNetworkFixedN() {
	diff := (len(p.sink_conn) - p.peer_fixed_count)
	if diff == 0 {
		if rand.Intn(2) == 0 {
			diff = 1
		} else {
			diff = -1
		}
	}
	p.log.Printf("RECONFIGURE diff %v", diff)

	if diff < 0 {
		p.openRandom(-diff)
	} else {
		p.closeRandom(diff)
	}
}

func (p *PeerImpl) closeRandom(n int) {
	avail_sinks := len(p.sink_conn) - PEER_MIN_SINKS
	if avail_sinks < 1 {
		// nothing close
		return
	}

	if avail_sinks < n {
		n = avail_sinks
		p.log.Printf("Too few sink connectins, close %d", n)
	}

	id_list := make(map[string]interface{})

	ids := make([]string, 0)
	for id, _ := range p.sink_conn {
		ids = append(ids, id)
	}

	for len(id_list) < n {
		i := rand.Intn(len(ids))
		if _, ok := id_list[ids[i]]; ok {
			//already in result
			continue
		}
		id_list[ids[i]] = nil
	}

	p.log.Printf("RECONFIGURE close ids %v", id_list)

	for id, _ := range id_list {
		conn_neighbours := p.sink_conn[id].Neighbours()
		if conn_neighbours != nil && len(conn_neighbours.Sources) <= PEER_MIN_SOURCES {
			continue
		}
		p.sink_conn[id].Close()
	}
}

func (p *PeerImpl) openRandom(n int) {

	peers := p.getIndirectPeers()
	p.log.Printf("RECONFIGURE peers %v", peers)

	id_list := make(map[string]interface{})

	ids := make([]string, 0)
	for id, _ := range peers {
		ids = append(ids, id)
	}

	avail_peers := len(ids)
	if avail_peers < n {
		n = avail_peers
	}
	if n < 1 {
		p.log.Printf("RECONFIGURE no connections to open")
		return
	}

	for len(id_list) < n {
		i := rand.Intn(len(ids))
		if _, ok := id_list[ids[i]]; ok {
			//already in result
			continue
		}
		id_list[ids[i]] = nil
	}

	p.log.Printf("RECONFIGURE open ids %v", id_list)

	for id, _ := range id_list {
		p.createSinkConnection(fmt.Sprintf("%s:%d", peers[id].Host, peers[id].Port))
	}
}
