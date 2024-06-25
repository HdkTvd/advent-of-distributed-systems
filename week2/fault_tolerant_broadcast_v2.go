package week2

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"reflect"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type job struct {
	Src   string
	Dest  string
	Value any
}

func consumeJobChannel(queue chan job, n *maelstrom.Node) {
	log.Printf("Started job queue")
	for {
		j, _ := <-queue
		go func(jb job) {
			body := map[string]any{}
			body["type"] = "broadcast"
			body["message"] = jb.Value

			if err := n.RPC(jb.Dest, body, func(msg maelstrom.Message) error {
				resBody := map[string]any{}
				if err := json.Unmarshal(msg.Body, &resBody); err != nil {
					log.Printf("Retry %v after err %v\n", jb, err.Error())
					queue <- jb
					return err
				}
				log.Printf("saw %v and got %v", jb, resBody)
				typ := reflect.ValueOf(resBody["type"]).String()
				if typ != "broadcast_ok" {
					log.Printf("Retry %v after return type %v\n", jb, typ)
					queue <- jb
					return errors.New("broadcast not ok")
				}

				return nil
			}); err != nil {
				log.Printf("Retry %v after err %v\n", jb, err.Error())
				queue <- jb
				return
			}
		}(j)
	}
}

func Fault_tolerant_broadcast_v2() {
	n := maelstrom.NewNode()

	mu := &sync.Mutex{}
	values := make(map[any]bool)
	topology := make(map[string]interface{})

	queue := make(chan job)

	go consumeJobChannel(queue, n)

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
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

		if seen == false {
			for _, node := range n.NodeIDs() {
				if node != n.ID() && node != msg.Src {
					queue <- job{
						Src:   n.ID(),
						Dest:  node,
						Value: message,
					}
				}
			}
		}

		return n.Reply(msg, map[string]any{"type": "broadcast_ok"})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
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
		// Unmarshal the message body as an loosely-typed map.
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
