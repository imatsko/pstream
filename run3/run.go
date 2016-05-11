package main

import (
	"github.com/imatsko/pstream"
	"math/rand"
	"time"
	"fmt"
	"sync"
)

func main() {
	in := make(chan *pstream.StreamChunk)
	out := make(chan *pstream.StreamChunk, 32)

	sb := pstream.NewStreamBuffer(in, out)

	wg := sync.WaitGroup{}

	send := func() {
		wg.Add(1)
		for i := 1; i <= 100; i += 1 {
			if rand.Intn(4) != 0 {
				n := time.Now()
				time.Sleep(pstream.SB_NEXT_CHUNK_PERIOD)
				c := pstream.StreamChunk{uint64(i), i}
				fmt.Printf("%v Sending %#v \n", n, c)
				in <- &c
				n = time.Now()
				fmt.Printf("%v Sending %#v finished\n", n, c)
			} else if rand.Intn(3) != 0 {
				n := time.Now()
				time.Sleep(2*pstream.SB_NEXT_CHUNK_PERIOD)
				c := pstream.StreamChunk{uint64(i), i}
				fmt.Printf("%v Sending slow %#v \n", n, c)
				in <- &c
				n = time.Now()
				fmt.Printf("%v Sending slow %#v finished\n", n, c)
			} else {
				n := time.Now()
				fmt.Printf("%v drop chunk %v \n", n, i)
			}
		}
		wg.Done()
	}

	recv := func() {
		for {
			c := <- out
			n := time.Now()
			fmt.Printf("%v Received %#v \n", n, c)
			fmt.Printf("%v State %#v \n", n, sb.State())
		}
	}

	go send()
	go recv()
	go sb.Serve()

	wg.Wait()
	time.Sleep(30*time.Second)
}


