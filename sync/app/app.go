package main

import (
	"dbsync/sync"
	"dbsync/sync/shared"
	"flag"
	"fmt"
	//	_ "github.com/alexbrainman/odbc"
	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	"github.com/google/gops/agent"
	_ "github.com/lib/pq"
	//	_ "github.com/mattn/go-oci8"
	_ "github.com/adrianwit/dyndb"
	_ "github.com/vertica/vertica-sql-go"
	_ "github.com/viant/asc"
	_ "github.com/viant/bgc"
	"log"
	"os"
)

// Version app version
var Version string

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

	config := &shared.Config{
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
	fmt.Printf("dbsync %v listening on :%d\n", Version, *port)
	log.Fatal(server.ListenAndServe())
}
