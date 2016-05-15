
package main

import (
	"github.com/imatsko/pstream"
	"math/rand"
	"time"
	"sync"
	"github.com/op/go-logging"
)

var main_log = logging.MustGetLogger("Main")

func main() {

	p1 := pstream.NewPeer("peer_sender")
	p1.SendPeriod = time.Millisecond*50

	in := p1.In
	out := p1.Out

	go p1.Serve()
	//go p1.ServeConnections(":9000")



	wg := sync.WaitGroup{}

	send := func() {
		wg.Add(1)
		for i := 1; i <= 100; i += 1 {
			if rand.Intn(4) != 0 {
				time.Sleep(pstream.SB_NEXT_CHUNK_PERIOD)
				c := pstream.Chunk{uint64(i), i}
				main_log.Debugf("Sending %#v", c)
				in <- &c
				main_log.Debugf("Sending %#v finished", c)
			} else if rand.Intn(3) != 0 {
				time.Sleep(2*pstream.SB_NEXT_CHUNK_PERIOD)
				c := pstream.Chunk{uint64(i), i}
				main_log.Debugf("Sending slow %#v", c)
				in <- &c
				main_log.Debugf("Sending slow %#v finished", c)
			} else {
				main_log.Debugf("drop chunk %v", i)
			}
		}
		wg.Done()
	}

	recv := func() {
		for {
			c := <- out
			main_log.Debugf("Received %#v", c)
		}
	}

	go send()
	go recv()
	//go sb.Serve()

	wg.Wait()
	time.Sleep(30*time.Second)
}


