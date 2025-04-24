// coordinator package manages the state of the chain. The Coordinator is
// responsible for detecting and handling node failures, electing head and tail
// nodes, and adding new nodes to the chain.
//
// If the Coordinator process fails but the chain is still intact, reads would
// still be possible. Writes will not be possible because all writes are first
// sent to the Coordinator. The coordinator forwards write requests to the head
// node.

package coordinator

import (
	"errors"
	"log"
	"sync"
	"time"

	"github.com/despreston/go-craq/transport"
)

// Default timing values
var (
	DefaultPingTimeout    = 5 * time.Second // Default time to wait for a ping response
	DefaultPingInterval   = 1 * time.Second // Default interval between ping cycles
	DefaultStabilizeRatio = 2.0             // Default ratio for stabilization timeout (relative to ping timeout)
)

var ErrEmptyChain = errors.New("no nodes in the chain")

// Coordinator is responsible for tracking the Nodes in the chain.
type Coordinator struct {
	tport      transport.NodeClientFactory
	head, tail *node
	mu         sync.Mutex
	replicas   []*node

	// For testing the AddNode method. This WaitGroup is done when updates have
	// been sent to all nodes.
	Updates *sync.WaitGroup

	// Chain health tracking
	chainMu           sync.RWMutex
	chainStable       bool          // true if chain is stable, false during node failure detection
	failureDetectedAt time.Time     // when was a node failure last detected
	stabilizeTimeout  time.Duration // how long to wait before considering chain stable again

	// Specific tracking for tail node failures
	tailRecoveryMu sync.RWMutex
	tailRecovery   bool // true when tail is being recovered after failure

	// Configurable timing parameters
	pingTimeout  time.Duration // How long to wait for a ping response before considering a node failed
	pingInterval time.Duration // Interval between ping cycles
}

// CoordinatorOpts contains configuration options for creating a Coordinator.
// All fields are optional and will use defaults if not specified.
type CoordinatorOpts struct {
	PingTimeout    time.Duration // How long to wait for a node to respond before considering it failed
	PingInterval   time.Duration // How often to ping nodes
	StabilizeRatio float64       // Ratio of ping timeout for stabilization period (e.g., 2.0 = 2x pingTimeout)
}

func New(t transport.NodeClientFactory) *Coordinator {
	return NewWithOpts(t, CoordinatorOpts{})
}

// NewWithOpts creates a new Coordinator with custom configuration options.
func NewWithOpts(t transport.NodeClientFactory, opts CoordinatorOpts) *Coordinator {
	// Use defaults for any unspecified options
	if opts.PingTimeout <= 0 {
		opts.PingTimeout = DefaultPingTimeout
	}
	if opts.PingInterval <= 0 {
		opts.PingInterval = DefaultPingInterval
	}
	if opts.StabilizeRatio <= 0 {
		opts.StabilizeRatio = DefaultStabilizeRatio
	}

	return &Coordinator{
		Updates:          &sync.WaitGroup{},
		tport:            t,
		chainStable:      true,
		pingTimeout:      opts.PingTimeout,
		pingInterval:     opts.PingInterval,
		stabilizeTimeout: time.Duration(float64(opts.PingTimeout) * opts.StabilizeRatio),
	}
}

func (cdr *Coordinator) Start() {
	cdr.pingReplicas()
}

// Ping each node. If the response returns an error or the pingTimeout is
// reached, remove the node from the list of replicas.
func (cdr *Coordinator) pingReplicas() {
	log.Println("starting pinging")
	for {
		for _, n := range cdr.replicas {
			go func(n *node) {
				resultCh := make(chan bool, 1)

				go func() {
					err := n.rpc.Ping()
					resultCh <- err == nil
				}()

				select {
				case ok := <-resultCh:
					if !ok {
						cdr.RemoveNode(n.Address())
					}
				case <-time.After(cdr.pingTimeout):
					cdr.RemoveNode(n.Address())
				}
			}(n)
		}
		time.Sleep(cdr.pingInterval)
	}
}

func findReplicaIndex(address string, replicas []*node) (int, bool) {
	for i, replica := range replicas {
		if replica.Address() == address {
			return i, true
		}
	}
	return 0, false
}

func (cdr *Coordinator) updateAll() {
	wg := sync.WaitGroup{}
	for i := 0; i < len(cdr.replicas); i++ {
		wg.Add(1)
		go func(i int) {
			cdr.updateNode(i)
			wg.Done()
		}(i)
	}
	wg.Wait()
}

func (cdr *Coordinator) RemoveNode(address string) error {
	cdr.mu.Lock()
	defer cdr.mu.Unlock()

	// Mark the chain as unstable when a node fails
	cdr.chainMu.Lock()
	cdr.chainStable = false
	cdr.failureDetectedAt = time.Now()
	log.Printf("[CHAIN-HEALTH] Chain marked UNSTABLE due to node failure: %s", address)
	cdr.chainMu.Unlock()

	// Start a goroutine to mark the chain as stable again after stabilizeTimeout
	go func() {
		time.Sleep(cdr.stabilizeTimeout)
		cdr.chainMu.Lock()
		cdr.chainStable = true
		log.Printf("[CHAIN-HEALTH] Chain marked STABLE after reorganization period completed")
		cdr.chainMu.Unlock()
	}()

	idx, found := findReplicaIndex(address, cdr.replicas)
	if !found {
		return errors.New("unknown node")
	}

	wasTail := idx == len(cdr.replicas)-1
	cdr.replicas = append(cdr.replicas[:idx], cdr.replicas[idx+1:]...)
	log.Printf("removed node %s", address)

	if wasTail {
		// Specifically mark that we're in tail recovery mode
		// This will cause both reads and writes to hang
		cdr.tailRecoveryMu.Lock()
		cdr.tailRecovery = true
		log.Printf("[CHAIN-HEALTH] Tail node failed, entering tail recovery mode")
		cdr.tailRecoveryMu.Unlock()

		cdr.tail = nil
		if idx > 0 {
			cdr.tail = cdr.replicas[idx-1]

			// Schedule the end of tail recovery mode after stabilizeTimeout
			go func() {
				time.Sleep(cdr.stabilizeTimeout)
				cdr.tailRecoveryMu.Lock()
				cdr.tailRecovery = false
				log.Printf("[CHAIN-HEALTH] New tail node stabilized, exiting tail recovery mode")
				cdr.tailRecoveryMu.Unlock()
			}()
		}

		// Because the tail node changed, all the other nodes need to be updated to
		// know where the tail is.
		cdr.updateAll()
		return nil
	}

	// After removing the node, the successor, if there was one, now sits at idx
	// in the chain. Send a message to that node to update it's metadata.
	if len(cdr.replicas) > idx {
		err := cdr.updateNode(idx)
		if err != nil {
			log.Printf("Failed to update successor: %s\n", err.Error())
			return err
		}
	}

	// Send update to predecessor and update the tail
	if idx > 0 {
		err := cdr.updateNode(idx - 1)
		if err != nil {
			log.Printf("Failed to update predecessor: %v\n", err)
			return err
		}
	}

	return nil
}

// updateNode sends the latest metadata to a Node to tell it whether it's head
// or tail and what it's neighbors' addresses are.
func (cdr *Coordinator) updateNode(i int) error {
	n := cdr.replicas[i]

	log.Printf("Sending metadata to %s.\n", n.Address())

	var args transport.NodeMeta
	args.IsHead = i == 0
	args.IsTail = len(cdr.replicas) == i+1
	args.Tail = cdr.replicas[len(cdr.replicas)-1].Address()

	if len(cdr.replicas) > 1 {
		if i > 0 {
			// Not the first node, so add address to previous.
			args.Prev = cdr.replicas[i-1].Address()
		}
		if i+1 != len(cdr.replicas) {
			// Not the last node, so add address to next.
			args.Next = cdr.replicas[i+1].Address()
		}
	}

	// call Update method on node
	if err := n.rpc.Update(&args); err != nil {
		return err
	}

	return nil
}

// AddNode should be called by Nodes to announce themselves to the Coordinator.
// The coordinator then adds them to the end of the chain. The coordinator
// replies with some flags to let the node know if they're head or tail, and
// the address to the previous Node in the chain. The node is responsible for
// announcing itself to the previous Node in the chain.
func (cdr *Coordinator) AddNode(address string) (*transport.NodeMeta, error) {
	log.Printf("received AddNode from %s\n", address)

	n := &node{
		last:    time.Now(),
		address: address,
		rpc:     cdr.tport(),
	}

	if err := n.Connect(); err != nil {
		log.Printf("failed to connect to node %s\n", address)
		return nil, err
	}

	cdr.replicas = append(cdr.replicas, n)
	cdr.tail = n
	meta := &transport.NodeMeta{}
	meta.IsTail = true
	meta.Tail = address

	if len(cdr.replicas) == 1 {
		cdr.head = n
		meta.IsHead = true
	} else {
		meta.Prev = cdr.replicas[len(cdr.replicas)-2].Address()
	}

	// Because the tail node changed, all the other nodes need to be updated to
	// know where the tail is.
	for i := 0; i < len(cdr.replicas)-1; i++ {
		cdr.Updates.Add(1)
		go func(i int) {
			cdr.updateNode(i)
			cdr.Updates.Done()
		}(i)
	}

	return meta, nil
}

// Write a new object to the chain.
func (cdr *Coordinator) Write(key string, value []byte) error {
	if len(cdr.replicas) < 1 {
		return ErrEmptyChain
	}

	// Check chain stability status before proceeding with write
	cdr.chainMu.RLock()
	isStable := cdr.chainStable
	failureTime := cdr.failureDetectedAt
	cdr.chainMu.RUnlock()

	if !isStable {
		// Calculate how long to wait before timeout
		timeElapsed := time.Since(failureTime)
		timeLeft := cdr.stabilizeTimeout - timeElapsed

		if timeLeft > 0 {
			log.Printf("[CHAIN-HEALTH] Write for key %s hanging for %v due to ongoing chain reorganization",
				key, timeLeft.Round(time.Millisecond))

			// Wait until the chain stabilizes
			time.Sleep(timeLeft)

			log.Printf("[CHAIN-HEALTH] Resuming write for key %s after chain stabilized", key)
		}
	}

	// Acquire lock again to ensure we have the current head after waiting
	cdr.mu.Lock()
	if len(cdr.replicas) < 1 {
		cdr.mu.Unlock()
		return ErrEmptyChain
	}
	head := cdr.replicas[0]
	cdr.mu.Unlock()

	// Forward the write to the head
	return head.rpc.ClientWrite(key, value)
}

// GetTailAddress returns the address of the current tail node.
// This is useful for clients that need to send read requests directly to the tail
// in vanilla chain replication.
func (cdr *Coordinator) GetTailAddress() (string, error) {
	cdr.mu.Lock()
	defer cdr.mu.Unlock()

	if cdr.tail == nil || len(cdr.replicas) == 0 {
		return "", ErrEmptyChain
	}

	return cdr.tail.Address(), nil
}
