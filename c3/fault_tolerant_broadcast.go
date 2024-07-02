package c3

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"reflect"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type job struct {
	Src   string
	Dest  string
	Value any
}

type persistentQueue struct {
	mu     sync.Mutex
	ackMap map[job]bool
}

func newPersistentQueue() *persistentQueue {
	return &persistentQueue{
		ackMap: make(map[job]bool),
	}
}

func (pq *persistentQueue) markAcked(j job) {
	pq.mu.Lock()
	pq.ackMap[j] = true
	pq.mu.Unlock()
}

func (pq *persistentQueue) isAcked(j job) bool {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return pq.ackMap[j]
}

func consumeJobChannel(jobChan chan job, pq *persistentQueue, n *maelstrom.Node) {
	log.Printf("Started job queue")

	factor := 50

	for j := range jobChan {
		go func(jb job) {
			body := map[string]any{}
			body["type"] = "broadcast"
			body["message"] = jb.Value

			attempt := 0
			for {
				if err := n.RPC(jb.Dest, body, func(msg maelstrom.Message) error {
					resBody := map[string]any{}
					if err := json.Unmarshal(msg.Body, &resBody); err != nil {
						return err
					}
					typ := reflect.ValueOf(resBody["type"]).String()
					if typ != "broadcast_ok" {
						return errors.New("broadcast not ok")
					}

					fmt.Fprintf(os.Stderr, "Acknowledged msg - %v, dest - %v \n", jb.Value, jb.Dest)
					pq.markAcked(jb)

					return nil
				}); err != nil || !pq.isAcked(jb) {
					attempt++
					fmt.Fprintf(os.Stderr, "Retry %v after err %v, attempt %d\n", jb, "retrying because not acknowledged", attempt)
					time.Sleep(time.Duration(attempt*factor) * time.Millisecond)
					continue
				}

				break
			}
		}(j)
	}
}

func Fault_tolerant_broadcast() {
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
