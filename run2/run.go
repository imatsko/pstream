package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/imatsko/pstream"
	"github.com/op/go-logging"
	"io/ioutil"
	"math/rand"
	"strings"
	"time"
)

var main_log = logging.MustGetLogger("Main")

type StringComaList []string

func (l *StringComaList) String() string {
	return fmt.Sprint([]string(*l))
}

func (l *StringComaList) Set(v string) error {
	*l = strings.Split(v, ",")
	return nil
}

var Config = struct {
	ConfFile      string         `json:"conf_file"`
	Listen        string         `json:"listen"`
	Source        bool           `json:"source"`
	SourceChunks  uint64         `json:"count"`
	SendPeriod    time.Duration  `json:"send_period"`
	BootstrapList StringComaList `json:"bootstrap"`
}{
	ConfFile:      "config.json",
	Listen:        "127.0.0.1:9000",
	SourceChunks:  500,
	SendPeriod:    time.Millisecond * 100,
	BootstrapList: StringComaList(make([]string, 0)),
}

var configLogger, _ = logging.GetLogger("config")

func init() {
	flag.StringVar(&Config.ConfFile, "conf", Config.ConfFile, "Read config from conf file if it exists")
	flag.StringVar(&Config.Listen, "listen", Config.Listen, "Listen on addr:port ")

	flag.BoolVar(&Config.Source, "source", Config.Source, "Generate stream")
	flag.Uint64Var(&Config.SourceChunks, "chunks", Config.SourceChunks, "Source shunk count")
	flag.DurationVar(&Config.SendPeriod, "send", Config.SendPeriod, "Send period")

	flag.Var(&Config.BootstrapList, "bootstrap", "Coma separated list of bootstrap addr:port")
	flag.Parse()

	data, err := ioutil.ReadFile(Config.ConfFile)
	if err != nil {
		configLogger.Warningf("Error while reading config file: %v", err)
		return
	}

	if err := json.Unmarshal(data, &Config); err != nil {
		configLogger.Errorf("Error while parsing config file: %v", err)
	}
}

func start_source() {

	p1 := pstream.NewPeer("source_sender", Config.Listen, Config.SendPeriod)

	in := p1.In
	out := p1.Out

	go p1.Serve()

	send := func() {
		for i := uint64(1); i <= Config.SourceChunks; i += 1 {

			if rand.Intn(4) != 0 {
				time.Sleep(pstream.SB_NEXT_CHUNK_PERIOD)
				c := pstream.Chunk{uint64(i), i}
				main_log.Debugf("Sending %#v", c)
				in <- &c
				main_log.Debugf("Sending %#v finished", c)
			} else if rand.Intn(3) != 0 {
				time.Sleep(2 * pstream.SB_NEXT_CHUNK_PERIOD)
				c := pstream.Chunk{uint64(i), i}
				main_log.Debugf("Sending slow %#v", c)
				in <- &c
				main_log.Debugf("Sending slow %#v finished", c)
			} else {
				main_log.Debugf("drop chunk %v", i)
			}
		}
	}

	recv := func() {
		for {
			select {
			case c := <-out:
				main_log.Debugf("Received %#v", c)
			case <-time.After(time.Second * 30):
				main_log.Debugf("No new packages too long")
				p1.Exit()
			}
		}
	}

	go recv()
	send()

}

func start_peer() {
	p1 := pstream.NewPeer("", Config.Listen, Config.SendPeriod)
	go p1.Serve()
	p1.BootstrapNetwork([]string(Config.BootstrapList))

	out := p1.Out

	recv := func() {
		for {
			select {
			case c := <-out:
				main_log.Debugf("Received %#v", c)
			case <-time.After(time.Second * 30):
				main_log.Debugf("No new packages too long")
				p1.Exit()
			}
		}
	}

	recv()
}

func main() {
	main_log.Infof("Start with config %+v", Config)

	if Config.Source {
		start_source()
	} else {
		start_peer()
	}
}
