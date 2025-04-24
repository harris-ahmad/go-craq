package main

import (
	"flag"
	"fmt"
	"log"
	"strings"

	"github.com/despreston/go-craq/transport/netrpc"
)

func main() {
	var cdrHost, cdrPort string

	flag.StringVar(&cdrHost, "ch", "localhost", "Coordinator host")
	flag.StringVar(&cdrPort, "cp", "8080", "Coordinator port")
	flag.Parse()

	cdrAddr := fmt.Sprintf("%s:%s", cdrHost, cdrPort)
	log.Printf("Connecting to coordinator at %s", cdrAddr)

	args := flag.Args()

	if len(args) < 1 {
		log.Fatal("No command given.")
	}

	cmd := args[0]

	if cmd == "readall" {
		n := netrpc.NewNodeClient()
		// For vanilla chain replication, we need to connect to the tail
		// Use coordinator to get the tail's address if -n flag wasn't specified with "tail"
		log.Println("Contacting coordinator to find tail node for read operation...")
		c := netrpc.NewCoordinatorClient()
		if err := c.Connect(cdrAddr); err != nil {
			log.Fatalf("Failed to connect to coordinator\n  %#v", err)
		}

		tailAddr, err := c.GetTailAddress()
		if err != nil {
			log.Fatalf("Failed to get tail address: %v", err)
		}
		log.Printf("Using tail node at %s for read operation", tailAddr)
		cdrAddr = tailAddr

		if err := n.Connect(cdrAddr); err != nil {
			log.Fatalf("Failed to connect to node\n  %#v", err)
		}

		log.Println("Reading all items from the node...")

		items, err := n.ReadAll()
		if err != nil {
			log.Fatal(err.Error())
		}

		for _, item := range *items {
			log.Printf("key: %s, value: %s", item.Key, string(item.Value))
		}

		return
	}

	if len(args) < 2 {
		log.Fatal("No key given.")
	}

	key := args[1]

	switch cmd {
	case "write":
		if len(args) < 3 {
			log.Fatal("No value given.")
		}
		val := strings.Join(args[2:], " ")
		c := netrpc.NewCoordinatorClient()
		if err := c.Connect(cdrAddr); err != nil {
			log.Fatalf("Failed to connect to coordinator\n  %#v", err)
		}
		log.Println("Writing to the node...")
		log.Println(c.Write(key, []byte(val)))
	case "read":
		n := netrpc.NewNodeClient()

		// For vanilla chain replication, we need to connect to the tail
		// Use coordinator to get the tail's address if -n flag wasn't specified with "tail"

		log.Println("Contacting coordinator to find tail node for read operation...")
		c := netrpc.NewCoordinatorClient()
		if err := c.Connect(cdrAddr); err != nil {
			log.Fatalf("Failed to connect to coordinator\n  %#v", err)
		}

		tailAddr, err := c.GetTailAddress()
		if err != nil {
			log.Fatalf("Failed to get tail address: %v", err)
		}
		log.Printf("Using tail node at %s for read operation", tailAddr)

		if err := n.Connect(tailAddr); err != nil {
			log.Fatalf("Failed to connect to node\n  %#v", err)
		}

		k, v, err := n.Read(key)
		if err != nil {
			log.Fatal(err.Error())
		}

		log.Printf("key: %s, value: %s", k, string(v))
	}
}
