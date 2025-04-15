#! /bin/bash

# Build the Coordinator
go build -o bin/coordinator cmd/coordinator/coordinator.go

# Build the Node
go build -o bin/node cmd/node/node.go

# Build the Client
go build -o bin/client cmd/client/client.go