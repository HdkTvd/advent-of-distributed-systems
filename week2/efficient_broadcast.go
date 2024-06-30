package week2

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"reflect"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func Efficient_broadcast() {
	n := maelstrom.NewNode()

	mu := &sync.Mutex{}
	values := make(map[any]bool)
	topology := make([]string, 0)

	jobChan := make(chan job, 100)
	pq := newPersistentQueue()

	go consumeJobChannel(jobChan, pq, n)

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		message := reflect.ValueOf(body["message"]).Float()
		seen := false
		mu.Lock()
		_, ok := values[message]
		if ok {
			seen = true
		} else {
			values[message] = true
		}
		mu.Unlock()

		if !seen {
			for _, neighbor := range topology {
				if neighbor != msg.Src {
					fmt.Fprintf(os.Stderr, "Adding job, src - %v, dst - %v, message - %v\n", n.ID(), neighbor, message)
					jobChan <- job{
						Src:   n.ID(),
						Dest:  neighbor,
						Value: message,
					}
				}
			}
		}

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
		mu.Lock()
		for k := range values {
			keys = append(keys, k)
		}
		mu.Unlock()

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
			topology = append(topology, neighbor.(string))
		}

		body = map[string]any{}
		body["type"] = "topology_ok"

		fmt.Fprintf(os.Stderr, "topo- %v", topology, "\n")

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
