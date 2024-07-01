package week2

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// TODO:
// 1. Do not broadcast msg, instead follow point 2.
// 2. Keep track of messages on neighbouring nodes. After 100ms send the delta and receive all the messages at once?
// no. of msgs per ops pre node is reduced.

type Node struct {
	Values   map[int]bool
	Topology []string
	Mu       *sync.Mutex
}

func NewNode() *Node {
	values := make(map[int]bool, 0)
	topology := make([]string, 0)
	mu := &sync.Mutex{}

	return &Node{values, topology, mu}
}

func Efficient_broadcast() {
	ln := NewNode()
	n := maelstrom.NewNode()

	n.Handle("init", func(msg maelstrom.Message) error {
		waitPeriod := generateRandomWaitPeriod(n.ID())

		go ln.askForMessagesAndWriteItOnLocal(n, waitPeriod)

		return nil
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		message := int(reflect.ValueOf(body["message"]).Float())

		ln.Mu.Lock()
		ln.Values[message] = true
		ln.Mu.Unlock()

		return n.Reply(msg, map[string]any{"type": "broadcast_ok"})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body = map[string]any{}
		body["type"] = "read_ok"

		var keys []any
		ln.Mu.Lock()
		for k := range ln.Values {
			keys = append(keys, k)
		}
		ln.Mu.Unlock()

		body["messages"] = keys

		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		topologyFromClient := body["topology"].(map[string]interface{})
		for _, neighbor := range topologyFromClient[n.ID()].([]interface{}) {
			ln.Topology = append(ln.Topology, neighbor.(string))
		}

		body = map[string]any{}
		body["type"] = "topology_ok"

		fmt.Fprintf(os.Stderr, "topo- %v", ln.Topology, "\n")

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

// TODO: get messages from neighbours every randmonly between 10-100 ms and add it in your values
func (node *Node) askForMessagesAndWriteItOnLocal(mn *maelstrom.Node, waitPeriod int) {
	for {
		time.Sleep(time.Millisecond * time.Duration(waitPeriod))
		for _, neighbor := range node.Topology {
			payload := map[string]interface{}{
				"type": "read",
				// "msg_id": "",
			}

			if err := mn.RPC(neighbor, payload, func(msg maelstrom.Message) error {
				var body map[string]any
				if err := json.Unmarshal(msg.Body, &body); err != nil {
					return err
				}

				fmt.Fprintf(os.Stderr, "read resp body - %+v\n", body)

				// TODO: syncro msg write to local variable
				// to avoid same msg write compare length bcoz msg are not duplicate
				messages, ok := body["messages"].([]interface{})
				if ok && len(node.Values) < len(messages) {
					node.Mu.Lock()
					for _, m := range messages {
						node.Values[int(m.(float64))] = true
					}
					node.Mu.Unlock()
				}

				return nil
			}); err != nil {
				fmt.Fprintf(os.Stderr, "Error getting messages from neighbor node - %v\n", neighbor)
			}
		}
	}
}

func generateRandomWaitPeriod(nodeId string) int {
	// nodeId := n.ID()
	nodeNumber, _ := strconv.Atoi(nodeId[len(nodeId)-1:])

	s2 := rand.NewSource(time.Now().UnixNano() + int64(nodeNumber*10))
	r2 := rand.New(s2)
	max, min := 1000, 500
	waitPeriod := r2.Intn(max-min) + min

	fmt.Fprintf(os.Stderr, "Starting the node with wait period of %vms\n", waitPeriod)

	return waitPeriod
}
