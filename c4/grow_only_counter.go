package c4

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func GrowOnlyCoounter() {
	// topology := make(map[string]interface{}, 0)

	n := maelstrom.NewNode()
	skv := maelstrom.NewSeqKV(n)

	key := "counter"

	n.Handle("read", func(msg maelstrom.Message) error {
		ctx := context.Background()

		val, err := skv.ReadInt(ctx, key)
		// It's important to ignore this error as it may occure initially
		if err != nil && !strings.Contains(err.Error(), maelstrom.ErrorCodeText(maelstrom.KeyDoesNotExist)) {
			fmt.Fprintf(os.Stderr, "Error in sequential counter read - %q\n", err.Error())
			return err
		}

		return n.Reply(msg, map[string]any{
			"type":  "read_ok",
			"value": val,
		})
	})

	n.Handle("add", func(msg maelstrom.Message) error {
		ctx := context.Background()

		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		delta := int(body["delta"].(float64))

		// CAS ensures that the new value will only be updated if the old value also matches
		// this helps avoid race conditions in concurrent enviroments.
		// This loop guarantees the continuous retries even after the "key does not exist" error OR
		// the read value does not match the value while udpating with error "error current value a is not x"
		for {
			currentVal, err := skv.ReadInt(ctx, key)
			if err != nil && !strings.Contains(err.Error(), maelstrom.ErrorCodeText(maelstrom.KeyDoesNotExist)) {
				fmt.Fprintf(os.Stderr, "Error in sequential counter read for add - %q\n", err.Error())
				return err
			}

			newVal := currentVal + delta
			if err := skv.CompareAndSwap(ctx, key, currentVal, newVal, true); err == nil {
				break
			}
		}

		return n.Reply(msg, map[string]any{"type": "add_ok"})
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

		_ = body["topology"].(map[string]interface{})

		return n.Reply(msg, replyBody)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
