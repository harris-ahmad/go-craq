package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/rpc"

	"github.com/despreston/go-craq/node"
	"github.com/despreston/go-craq/store/boltdb"
	"github.com/despreston/go-craq/transport/netrpc"
)

func main() {
	var host, port string
	var pubHost, pubPort string
	var cdrHost, cdrPort string
	var dbFile string

	flag.StringVar(&host, "h", "localhost", "Host to bind to")
	flag.StringVar(&port, "p", "1235", "Port to bind to")
	flag.StringVar(&pubHost, "ph", "", "Public hostname for other nodes (required)")
	flag.StringVar(&pubPort, "pp", "", "Public port (defaults to bind port)")
	flag.StringVar(&cdrHost, "ch", "localhost", "Coordinator hostname")
	flag.StringVar(&cdrPort, "cp", "1234", "Coordinator port")
	flag.StringVar(&dbFile, "f", "craq.db", "Database file path")
	flag.Parse()

	if pubHost == "" {
		log.Fatal("Public hostname (-ph) is required for cluster deployment")
	}
	if pubPort == "" {
		pubPort = port
	}

	bindAddr := fmt.Sprintf("%s:%s", host, port)
	pubAddr := fmt.Sprintf("%s:%s", pubHost, pubPort)
	cdrAddr := fmt.Sprintf("%s:%s", cdrHost, cdrPort)

	log.Printf("Node binding to %s, advertising as %s", bindAddr, pubAddr)
	log.Printf("Using coordinator at %s", cdrAddr)

	db := boltdb.New(dbFile, "yessir")
	if err := db.Connect(); err != nil {
		log.Fatal(err)
	}

	defer db.DB.Close()

	n := node.New(node.Opts{
		Address:           bindAddr,
		CdrAddress:        cdrAddr,
		PubAddress:        pubAddr,
		Store:             db,
		Transport:         netrpc.NewNodeClient,
		CoordinatorClient: netrpc.NewCoordinatorClient(),
		Log:               log.Default(),
	})

	b := netrpc.NodeBinding{Svc: n}
	if err := rpc.RegisterName("RPC", &b); err != nil {
		log.Fatal(err)
	}
	rpc.HandleHTTP()

	// Start the node
	go n.Start()

	// Start the rpc server
	log.Println("Listening at " + bindAddr)
	log.Fatal(http.ListenAndServe(bindAddr, nil))
}
