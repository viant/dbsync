package main

import (
	"dbsync/sync"
	"flag"
	"fmt"
	_ "github.com/alexbrainman/odbc"
	_ "github.com/go-sql-driver/mysql"
	"github.com/google/gops/agent"
	_ "github.com/mattn/go-oci8"
	_ "github.com/viant/bgc"
	"log"
	"os"
)

var port = flag.Int("port", 8080, "service port")
var url = flag.String("url", "cron", "schedule URL")
var debug = flag.Bool("debug", false, "debug flag")
var scheduleURLRefreshMs = flag.Int("urlRefresh", 100, "scheduleURL refresh in ms")
var statsHistory = flag.Int("statsHistory", 10, "max stats history")

func main() {
	flag.Parse()


	go func() {
		if err := agent.Listen(agent.Options{}); err != nil {
			log.Fatal(err)
		}
	}()

	config := &sync.Config{
		Debug:                *debug,
		ScheduleURL:          *url,
		ScheduleURLRefreshMs: *scheduleURLRefreshMs,
		MaxHistory:           *statsHistory,
	}
	service, err := sync.New(config)
	if err != nil {
		log.Fatal(err)
	}
	server := sync.NewServer(service, *port)
	go server.StopOnSiginals(os.Interrupt)
	fmt.Printf("dstransfer listening on :%d\n", *port)
	server.ListenAndServe()
}
