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
	flag.StringVar(&cdrHost, "ch", "localhost", "Coordinator hostname")
	flag.StringVar(&cdrPort, "cp", "1234", "Coordinator port")
	flag.Parse()

	cdrAddr := fmt.Sprintf("%s:%s", cdrHost, cdrPort)
	log.Printf("Connecting to coordinator at %s", cdrAddr)

	args := flag.Args()

	if len(args) < 1 {
		log.Fatal("No command given.")
	}

	cmd := args[0]

	if cmd == "readall" {
		// Connect to the coordinator to get the tail node's address
		c := netrpc.NewCoordinatorClient()
		if err := c.Connect(cdrAddr); err != nil {
			log.Fatalf("Failed to connect to coordinator\n  %#v", err)
		}

		tailAddress, err := c.GetTailAddress()
		if err != nil {
			log.Fatalf("Failed to get tail node address\n  %#v", err)
		}

		n := netrpc.NewNodeClient()
		if err := n.Connect(tailAddress); err != nil {
			log.Fatalf("Failed to connect to tail node\n  %#v", err)
		}

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
		log.Println(c.Write(key, []byte(val)))
	case "read":
		// Connect to the coordinator to get the tail node's address
		c := netrpc.NewCoordinatorClient()
		if err := c.Connect(cdrAddr); err != nil {
			log.Fatalf("Failed to connect to coordinator\n  %#v", err)
		}

		tailAddress, err := c.GetTailAddress()
		if err != nil {
			log.Fatalf("Failed to get tail node address\n  %#v", err)
		}

		n := netrpc.NewNodeClient()
		if err := n.Connect(tailAddress); err != nil {
			log.Fatalf("Failed to connect to tail node\n  %#v", err)
		}

		k, v, err := n.Read(key)
		if err != nil {
			log.Fatal(err.Error())
		}

		log.Printf("key: %s, value: %s", k, string(v))
	}
}
