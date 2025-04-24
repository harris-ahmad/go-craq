package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/rpc"
	"time"

	"github.com/despreston/go-craq/coordinator"
	"github.com/despreston/go-craq/transport/netrpc"
)

func main() {
	// Basic configuration
	var host, port string
	flag.StringVar(&host, "h", "0.0.0.0", "Host to bind to")
	flag.StringVar(&port, "p", "1234", "Port to bind to")

	// Node failure detection parameters
	var pingTimeoutSec, pingIntervalSec float64
	var stabilizeRatio float64
	flag.Float64Var(&pingTimeoutSec, "ping-timeout", 5.0, "Time in seconds to wait for a ping response before considering a node failed")
	flag.Float64Var(&pingIntervalSec, "ping-interval", 1.0, "Time in seconds between ping cycles")
	flag.Float64Var(&stabilizeRatio, "stabilize-ratio", 2.0, "Ratio for stabilization period (e.g., 2.0 = 2x ping-timeout)")
	flag.Parse()

	// Convert to time.Duration
	pingTimeout := time.Duration(pingTimeoutSec * float64(time.Second))
	pingInterval := time.Duration(pingIntervalSec * float64(time.Second))

	addr := fmt.Sprintf("%s:%s", host, port)
	log.Printf("Starting coordinator on %s", addr)
	log.Printf("Configuration: ping-timeout=%v, ping-interval=%v, stabilize-ratio=%.1f",
		pingTimeout, pingInterval, stabilizeRatio)

	// Create coordinator with custom options
	c := coordinator.NewWithOpts(netrpc.NewNodeClient, coordinator.CoordinatorOpts{
		PingTimeout:    pingTimeout,
		PingInterval:   pingInterval,
		StabilizeRatio: stabilizeRatio,
	})

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
