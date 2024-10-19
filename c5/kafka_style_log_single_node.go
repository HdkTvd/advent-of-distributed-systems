package c5

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func KafkaStyleLogSingleNode() {
	n := maelstrom.NewNode()
	seqKV := maelstrom.NewSeqKV(n)

	Node := struct {
		logs             map[string][][]int
		committedOffsets map[string]int
		mu               sync.RWMutex
	}{
		logs:             make(map[string][][]int, 0),
		committedOffsets: make(map[string]int),
		mu:               sync.RWMutex{},
	}

	// handlers
	n.Handle("send", func(msg maelstrom.Message) error {
		ctx := context.Background()

		body := make(map[string]interface{})
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			fmt.Fprintf(os.Stderr, "Error in Unmarshalling request - %q\n", err.Error())
			return err
		}

		key := body["key"].(string)
		data := body["msg"].(float64)
		var newOffset int

		for {
			var currentOffset int = -1
			if err := seqKV.ReadInto(ctx, key, &currentOffset); err != nil && !strings.Contains(err.Error(), maelstrom.ErrorCodeText(maelstrom.KeyDoesNotExist)) {
				fmt.Fprintf(os.Stderr, "Error in sequential offset read for send - %q\n", err.Error())
				return err
			}

			newOffset = currentOffset + 1

			fmt.Fprintf(os.Stderr, "new offset %v \n", newOffset)

			if err := seqKV.CompareAndSwap(ctx, key, currentOffset, newOffset, true); err == nil {
				Node.mu.Lock()
				defer Node.mu.Unlock()

				offsets, ok := Node.logs[key]
				if !ok {
					offsets = make([][]int, 0)
				}

				offsets = append(offsets, []int{newOffset, int(data)})
				Node.logs[key] = offsets

				break
			} else {
				fmt.Fprintf(os.Stderr, "Failed to store value in CAS - %v", err)
			}
		}

		fmt.Fprintf(os.Stderr, "send_ok | newOffset - %v | key - %v\n", newOffset, key)

		if err := n.Reply(msg, map[string]interface{}{
			"type":   "send_ok",
			"offset": newOffset,
		}); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to reply send_ok with new offset value - %q\n", err.Error())
			return err
		}

		return nil
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		body := make(map[string]interface{})
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			fmt.Fprintf(os.Stderr, "Error in Unmarshalling request - %q\n", err.Error())
			return err
		}

		reqLogOffsets := body["offsets"].(map[string]interface{})

		fmt.Fprintf(os.Stderr, "polled offsets - %v \n", reqLogOffsets)

		var response = make(map[string][][]int, 0)

		Node.mu.RLock()
		defer Node.mu.RUnlock()

		for key, offset := range reqLogOffsets {
			startOffset := int(offset.(float64))

			fmt.Fprintf(os.Stderr, "key %v | logs - %v \n", key, Node.logs[key])
			fmt.Fprintf(os.Stderr, "individual polled offsets - %v \n", offset)

			log, ok := Node.logs[key]
			if !ok {
				continue
			}

			if startOffset < len(log) {
				response[key] = log[startOffset:]
			} else {
				response[key] = nil
			}
		}

		if err := n.Reply(msg, map[string]interface{}{
			"type": "poll_ok",
			"msgs": response,
		}); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to reply poll_ok. ERR - [%v]", err)
			return err
		}

		return nil
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		body := make(map[string]interface{})
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			fmt.Fprintf(os.Stderr, "Error in Unmarshalling request - %q\n", err.Error())
			return err
		}

		Node.mu.Lock()
		for key, committedOffset := range body["offsets"].(map[string]interface{}) {
			Node.committedOffsets[key] = int(committedOffset.(float64))
		}
		Node.mu.Unlock()

		if err := n.Reply(msg, map[string]string{"type": "commit_offsets_ok"}); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to reply commit_offsets_ok - %q\n", err.Error())
			return err
		}

		return nil
	})

	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		Node.mu.RLock()
		defer Node.mu.RUnlock()

		if err := n.Reply(msg, map[string]interface{}{
			"type":    "list_committed_offsets_ok",
			"offsets": Node.committedOffsets,
		}); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to reply commit_offsets_ok - %q\n", err.Error())
			return err
		}

		return nil
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
