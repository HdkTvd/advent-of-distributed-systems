package c4

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Node struct {
	Value    int
	Topology []string
	Mu       *sync.Mutex
}

func NewNode() *Node {
	return &Node{Value: 0, Topology: make([]string, 0), Mu: &sync.Mutex{}}
}

func GrowOnlyCoounter() {
	ln := NewNode()
	n := maelstrom.NewNode()
	skv := maelstrom.NewSeqKV(n)

	n.Handle("read", func(msg maelstrom.Message) error {
		ctx := context.Background()

		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body = map[string]any{}
		body["type"] = "read_ok"

		val, err := skv.ReadInt(ctx, "key")
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error in sequential counter read - %v\n", err)
			return err
		}

		body["value"] = val

		return n.Reply(msg, body)
	})

	// n.Handle("broadcast", func(msg maelstrom.Message) error {
	// 	ctx := context.Background()

	// 	var body map[string]any
	// 	if err := json.Unmarshal(msg.Body, &body); err != nil {
	// 		return err
	// 	}

	// 	delta := int(body["value"].(float64))

	// 	skv.CompareAndSwap(ctx, "key", )

	// 	val := ln.Value + delta
	// 	if err := skv.Write(ctx, "key", val); err != nil {
	// 		fmt.Fprintf(os.Stderr, "Error in sequential counter read - %v\n", err)
	// 		return err
	// 	}

	// 	ln.Mu.Lock()
	// 	ln.Value = val
	// 	ln.Mu.Unlock()

	// 	body = map[string]any{}
	// 	body["type"] = "add_ok"

	// 	return n.Reply(msg, body)
	// })

	n.Handle("add", func(msg maelstrom.Message) error {
		ctx := context.Background()

		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		delta := int(body["delta"].(float64))

		val := ln.Value + delta
		if err := skv.Write(ctx, "key", val); err != nil {
			fmt.Fprintf(os.Stderr, "Error in sequential counter read - %v\n", err)
			return err
		}

		ln.Mu.Lock()
		ln.Value = val
		ln.Mu.Unlock()

		body = map[string]any{}
		body["type"] = "add_ok"

		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		replyBody := map[string]any{}
		replyBody["type"] = "topology_ok"

		if source, _ := body["source"].(string); source != "nodeServer" {
			return n.Reply(msg, replyBody)
		}

		topologyFromClient := body["topology"].(map[string]interface{})

		for _, neighbor := range topologyFromClient[n.ID()].([]interface{}) {
			ln.Topology = append(ln.Topology, neighbor.(string))
		}

		return n.Reply(msg, replyBody)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
