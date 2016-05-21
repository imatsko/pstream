package pstream

import (
	"fmt"
	"math"
	"math/rand"
)

//const PEER_FIXED_COUNT = 5

//========================================================
// reconfigure sinks to flexible count based on used links
//=======================================================
func (p *PeerImpl) reconfigureNetworkDesirable() {
	unused_high := 0.3
	unused_low := 0.1
	growth := 0.3
	add_sources := 1
	add_sinks := 1

	used := make([]*Connection, 0)
	unused := make([]*Connection, 0)

	for _, conn := range p.sink_conn {
		if conn.Used {
			used = append(used, conn)
		} else {
			unused = append(unused, conn)
		}
	}

	p.log.Printf("RECONFIGURE src %v sink: used %v unused %v tot %v", len(p.src_conn), len(used), len(unused), len(p.sink_conn))

	if len(p.src_conn) <= PEER_MIN_SOURCES {
		new_src_count := PEER_MIN_SOURCES - len(p.src_conn) + add_sources
		p.log.Printf("RECONFIGURE: too few sources (%v), bootstrap source %v links", len(p.src_conn), new_src_count)
		p.openRandomConnection(new_src_count, "source")
	}

	if len(p.sink_conn) <= PEER_MIN_SINKS {
		new_sink_count := PEER_MIN_SINKS - len(p.sink_conn) + add_sinks
		p.log.Printf("RECONFIGURE: too few sinks (%v), bootstrap sink %v links", len(p.sink_conn), new_sink_count)
		p.openRandomConnection(new_sink_count, "sink")
	}

	//if len(p.src_conn) <= PEER_MIN_SOURCES || len(p.sink_conn) <= PEER_MIN_SINKS {
	//	return
	//}

	var unused_ratio float64
	//if len(used) <= PEER_MIN_SINKS {
	//	unused_ratio = 0
	//} else {
		unused_ratio = float64(len(unused)) / float64(len(used))
	//}

	if unused_ratio < unused_low {
		open_sink_count := int(math.Ceil(float64(len(used)) * growth))
		p.log.Printf("RECONFIGURE: low unused ratio (%v), add %v links", unused_ratio, open_sink_count)
		p.openDesiredProportionally(open_sink_count)
	} else if unused_ratio > unused_high {
		p.log.Printf("RECONFIGURE: high unused ratio (%v)", unused_ratio)
		new_total_count := int(float64(len(used)) + math.Ceil(float64(len(used))*(unused_high+unused_low)/2))
		new_unused_count := (new_total_count - len(used))
		close_unused_count := len(unused) - new_unused_count
		if len(unused) <= PEER_MIN_SINKS {
			p.log.Printf("RECONFIGURE: too few unused (%v), nothing close", len(unused))
			return
		}
		p.log.Printf("RECONFIGURE: close %v unused links (new tot links %v, new unused %v)", close_unused_count, new_total_count, new_unused_count)
		p.closeUndesiredProportionally(unused, close_unused_count)
	}
}

func (p PeerStat) Measure() float64 {
	return desirability_fun(p.SinkCount)
}

func removeById(arr []Selectable, id string) []Selectable {
	for i, s := range arr {
		if s.(PeerStat).Id == id {
			return append(arr[:i], arr[:i+1]...)
		}
	}
	return arr
}

func idInList(arr []Selectable, id string) bool {
	for _, s := range arr {
		if s.(*Connection).PeerId == id {
			return true
		}
	}
	return false
}

func (p *PeerImpl) openDesiredProportionally(new_conn int) {
	peers := p.getIndirectPeers()

	selectable := make([]Selectable, 0, len(peers))
	for _, p := range peers {
		selectable = append(selectable, p)
	}

	var conn_opened int

	for conn_opened < new_conn {
		open_list := SelectRandomProportionally(selectable, new_conn-conn_opened)
		if len(open_list) == 0 {
			break
		}
		for _, s := range open_list {
			item := s.(PeerStat)
			p.log.Printf("RECONFIGURE: try add sink %v", item)
			err := p.createSinkConnection(fmt.Sprintf("%s:%d", item.Host, item.Port))
			if err == nil {
				p.log.Printf("RECONFIGURE: sink %v created", item)
				conn_opened += 1
				selectable = removeById(selectable, item.Id)
			} else {
				p.log.Printf("RECONFIGURE: sink %v create error %v", item, err)
			}
		}
	}
}

func (p *PeerImpl) openRandomConnection(new_conn int, t string) {
	p.log.Printf("RECONFIGURE: open %v random %v", new_conn, t)
	peers := p.getIndirectPeers()

	if len(peers) == 0 {
		p.log.Printf("RECONFIGURE: have 0 indirect peers")
		return
	}

	peers_arr := make([]PeerStat, 0, len(peers))
	for _, peer := range peers {
		peers_arr = append(peers_arr, peer)
	}

	for i := 0; i < new_conn; i += 1 {
		r := rand.Intn(len(peers_arr))
		item := peers_arr[r]
		p.log.Printf("RECONFIGURE: open random %v %v", t, item)
		if t == "sink" {
			err := p.createSinkConnection(fmt.Sprintf("%s:%d", item.Host, item.Port))
			p.log.Printf("RECONFIGURE: open random %v %v err %v", t, item, err)
		} else if t == "source" {
			err := p.createSourceConnection(fmt.Sprintf("%s:%d", item.Host, item.Port))
			p.log.Printf("RECONFIGURE: open random %v %v err %v", t, item, err)
		} else {
			p.log.Printf("RECONFIGURE: unexpected connection type %v", t)
		}
	}
}

func (c *Connection) Measure() float64 {
	var sink_count int

	neighbours := c.Neighbours()
	if neighbours != nil {
		sink_count = len(neighbours.Sinks)
	}

	return desirability_fun(sink_count)
}

func (p *PeerImpl) closeUndesiredProportionally(list []*Connection, close_conn int) {

	selectable := make([]Selectable, 0, len(list))
	for _, c := range list {
		selectable = append(selectable, c)
	}

	save_list := SelectRandomProportionally(selectable, len(list)-close_conn)

	close_list := make([]*Connection, 0, close_conn)

	for _, c := range list {
		if !idInList(save_list, c.PeerId) {
			close_list = append(close_list, c)
		}
	}

	for _, c := range close_list {
		p.log.Printf("RECONFIGURE: close conneciton %v", c)
		c.Close()
	}
}

func (p *PeerImpl) closeRandom1(n int) {
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
