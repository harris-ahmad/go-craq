package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/rpc"

	"github.com/despreston/go-craq/coordinator"
	"github.com/despreston/go-craq/transport/netrpc"
)

func main() {
	var host, port string
	flag.StringVar(&host, "h", "0.0.0.0", "Host to bind to")
	flag.StringVar(&port, "p", "1234", "Port to bind to")
	flag.Parse()

	addr := fmt.Sprintf("%s:%s", host, port)
	log.Printf("Starting coordinator on %s", addr)

	c := coordinator.New(netrpc.NewNodeClient)

	binding := netrpc.CoordinatorBinding{Svc: c}
	if err := rpc.RegisterName("RPC", &binding); err != nil {
		log.Fatal(err)
	}
	rpc.HandleHTTP()

	// Start the Coordinator
	go c.Start()

	// Start the rpc server
	log.Println("Listening at " + addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}
