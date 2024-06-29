package week2

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
	queue  []job
	ackMap map[job]bool
}

func newPersistentQueue() *persistentQueue {
	return &persistentQueue{
		queue:  make([]job, 0),
		ackMap: make(map[job]bool),
	}
}

func (pq *persistentQueue) addJob(j job) {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	pq.queue = append(pq.queue, j)
}

func (pq *persistentQueue) getJob() (job, bool) {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	if len(pq.queue) == 0 {
		return job{}, false
	}
	j := pq.queue[0]
	pq.queue = pq.queue[1:]
	return j, true
}

func (pq *persistentQueue) markAcked(j job) {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	pq.ackMap[j] = true
}

func (pq *persistentQueue) isAcked(j job) bool {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return pq.ackMap[j]
}

func consumeJobChannel(pq *persistentQueue, n *maelstrom.Node) {
	log.Printf("Started job queue")
	for {
		j, ok := pq.getJob()
		if !ok {
			time.Sleep(100 * time.Millisecond)
			continue
		}
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
					pq.markAcked(jb)
					return nil
				}); err != nil {
					if pq.isAcked(jb) {
						return
					}
					attempt++
					log.Printf("Retry %v after err %v, attempt %d\n", jb, err.Error(), attempt)
					time.Sleep(time.Duration(attempt) * time.Second)
				} else {
					break
				}
			}
		}(j)
	}
}

func Fault_tolerant_broadcast_v2() {
	n := maelstrom.NewNode()

	mu := &sync.Mutex{}
	values := make(map[any]bool)
	topology := make(map[string]interface{})

	pq := newPersistentQueue()
	go consumeJobChannel(pq, n)

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
			for _, node := range topology[n.ID()].([]interface{}) {
				if node.(string) != msg.Src {
					pq.addJob(job{
						Src:   n.ID(),
						Dest:  node.(string),
						Value: message,
					})
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

		topology = body["topology"].(map[string]interface{})

		body = map[string]any{}
		body["type"] = "topology_ok"

		fmt.Fprintf(os.Stderr, "topo- %v", topology, "\n")

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
