package main

import (
	"github.com/imatsko/pstream"
	"net"
	"fmt"
	"flag"
	"bufio"
	"os"
)

type P struct {
	M, N int64
}

func client(remoteAddress string) {
	fmt.Println("start client");
	conn, err := net.Dial("tcp", remoteAddress)
	if err != nil {
		panic(err)
	}
	c := pstream.StartGobNetworkConnection(conn)

	s := pstream.StartProtocolServer(c)

	echo_proto_client := pstream.StartEchoClientProtoHandler()
	s.Register(1, echo_proto_client)
	go func() {
		for {
			if _, ok := <-echo_proto_client.Quit; !ok {
				panic("client closed")
			}
		}
	}()

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		echo_proto_client.Send(scanner.Text())
	}

	fmt.Println("done");
}

func handleConnection(conn net.Conn) {
	c := pstream.StartGobNetworkConnection(conn)
	s := pstream.StartProtocolServer(c)
	echo_proto_handler := pstream.StartEchoProtoHandler()
	s.Register(1, echo_proto_handler)
}

func server(addr string) {
	fmt.Println("start");
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		// handle error
	}
	for {
		conn, err := ln.Accept() // this blocks until connection or error
		if err != nil {
			// handle error
			continue
		}
		go handleConnection(conn) // a goroutine handles conn so that the loop can accept other connections
	}
}

func main() {
	var run_server bool
	flag.BoolVar(&run_server, "server", false, "run as server")
	flag.Parse()
	addr := "127.0.0.1:8080"
	if run_server {
		server(addr)
	} else {
		client(addr)
	}
}
