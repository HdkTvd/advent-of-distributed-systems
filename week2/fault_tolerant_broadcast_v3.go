package week2

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func Fault_tolerant_broadcast_v3() {
	var mu sync.Mutex
	// TODO: node Id to messages link required? Doesn't nodes have it's own working memory?
	messages := make([]int, 0)
	topology := make(map[string]interface{}, 0)

	replayCount := 5
	waitPeriodInSeconds := 1

	maelstromNode := maelstrom.NewNode()

	maelstromNode.Handle("broadcast", func(msg maelstrom.Message) error {
		reqBody := make(map[string]interface{})
		if err := json.Unmarshal(msg.Body, &reqBody); err != nil {
			fmt.Fprintf(os.Stderr, "Error unmarshalling message [%v]\n", err)
			return err
		}

		message := int(reqBody["message"].(float64))
		nodeId := maelstromNode.ID()

		mu.Lock()
		messages = append(messages, message)
		mu.Unlock()

		if err := maelstromNode.Reply(msg, map[string]interface{}{
			"type": "broadcast_ok",
		}); err != nil {
			fmt.Fprintf(os.Stderr, "Error sending message [%v]\n", err)
			// return err
		}

		receivers := make(map[string]interface{}, 0)
		if reqBody["receivers"] != nil {
			receivers = reqBody["receivers"].(map[string]interface{})
		}
		receivers[msg.Dest] = true

		// fmt.Fprintf(os.Stderr, "Debug: Previous receivers of this message -> %v | %v", message, receivers, "\n")

		payload := map[string]interface{}{
			"type":      "broadcast",
			"message":   message,
			"receivers": receivers,
		}

		// Nodes can communicate bidirectionally, this incurs repetition in messages
		// check prev receivers, do not send if present in the list
		for _, adjacentNode := range topology[nodeId].([]interface{}) {
			if _, ok := receivers[adjacentNode.(string)]; ok {
				continue
			}

			go func() {
				// replay messages which has failed due to network failure after some period of time.
				if err := replayRPCSend(replayCount, time.Duration(waitPeriodInSeconds), maelstromNode, payload, adjacentNode.(string), func(maelstromNode *maelstrom.Node, payload map[string]interface{}, dest string) error {
					if err := maelstromNode.RPC(dest, payload, func(msg maelstrom.Message) error {
						reqBody := make(map[string]interface{})
						if err := json.Unmarshal(msg.Body, &reqBody); err != nil {
							return err
						}

						// no signs of error in case of network broadcast failure # Maelstorm issue
						if reqBody["type"].(string) != "broadcast_ok" {
							return errors.New("broadcast response failure")
						}

						return nil
					}); err != nil {
						// no signs of error in case of network broadcast failure # Maelstorm issue
						return err
					}

					return nil
				}); err != nil {
					fmt.Fprintf(os.Stderr, "Error broadcasting message to node [%v] - [%v]\n", adjacentNode, err)
				}
			}()
		}

		return nil
	})

	maelstromNode.Handle("read", func(msg maelstrom.Message) error {
		payload := map[string]interface{}{
			"type":     "read_ok",
			"messages": messages,
		}

		if err := maelstromNode.Reply(msg, payload); err != nil {
			fmt.Fprintf(os.Stderr, "Error: Reply not okay for msg - %v", payload, "\n")
			return err
		}

		return nil
	})

	maelstromNode.Handle("topology", func(msg maelstrom.Message) error {
		reqBody := make(map[string]interface{})
		if err := json.Unmarshal(msg.Body, &reqBody); err != nil {
			fmt.Fprintf(os.Stderr, "Error unmarshalling message [%v]\n", err)
			return err
		}

		topology = reqBody["topology"].(map[string]interface{})

		if err := maelstromNode.Reply(msg, map[string]interface{}{
			"type": "topology_ok",
		}); err != nil {
			fmt.Fprintf(os.Stderr, "Error sending message [%v]\n", err)
			return err
		}

		return nil
	})

	if err := maelstromNode.Run(); err != nil {
		log.Fatal("Error running maelstrom node - ", err)
		return
	}
}
