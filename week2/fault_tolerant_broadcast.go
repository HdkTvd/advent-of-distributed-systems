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

func Fault_tolerant_broadcast() {
	var mu sync.Mutex
	// TODO: node Id to messages link required? Doesn't nodes have it's own working memory?
	messages := make(map[string][]int, 0)
	topology := make(map[string]interface{}, 0)

	replayCount := 3
	waitPeriodInSeconds := 2

	maelstromNode := maelstrom.NewNode()

	maelstromNode.Handle("broadcast", func(msg maelstrom.Message) error {
		reqBody := make(map[string]interface{})
		if err := json.Unmarshal(msg.Body, &reqBody); err != nil {
			log.Printf("Error unmarshalling message [%v]\n", err)
			return err
		}

		message := int(reqBody["message"].(float64))
		nodeId := maelstromNode.ID()

		if messageList, nodeExists := messages[nodeId]; !nodeExists {
			messages[nodeId] = []int{message}
		} else {
			mu.Lock()
			messageList = append(messageList, message)
			mu.Unlock()

			messages[nodeId] = messageList
		}

		if err := maelstromNode.Reply(msg, map[string]interface{}{
			"type": "broadcast_ok",
		}); err != nil {
			log.Printf("Error sending message [%v]\n", err)
			return err
		}

		receivers := make(map[string]interface{}, 0)
		if reqBody["receivers"] != nil {
			receivers = reqBody["receivers"].(map[string]interface{})
		}
		receivers[msg.Dest] = true

		fmt.Fprintf(os.Stderr, "Debug: Previous receivers of this message -> %v | %v", message, receivers, "\n")

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

			// TODO: replay messages which has failed due to network failure after some period of time.
			if err := replayRPCSend(replayCount, time.Duration(waitPeriodInSeconds), maelstromNode, payload, adjacentNode.(string), func(maelstromNode *maelstrom.Node, payload map[string]interface{}, dest string) error {
				if err := maelstromNode.RPC(dest, payload, func(msg maelstrom.Message) error {
					reqBody := make(map[string]interface{})
					if err := json.Unmarshal(msg.Body, &reqBody); err != nil {
						return err
					}

					// FIXME: no signs of error in case of network broadcast failure
					if reqBody["type"].(string) != "broadcast_ok" {
						fmt.Fprintf(os.Stderr, "Debug: Broadcast not okay for msg - %v | node - %v", message, dest, "\n")

						return errors.New("broadcast response failure")
					}

					return nil
				}); err != nil {
					// FIXME: no signs of error in case of network broadcast failure
					fmt.Fprintf(os.Stderr, "Debug: Broadcast failed for msg - %v | node - %v", message, dest, "\n")
					return err
				}

				return nil
			}); err != nil {
				log.Printf("Error broadcasting message to node [%v] - [%v]\n", adjacentNode, err)
			}
		}

		return nil
	})

	// TODO: replay read operation too?
	maelstromNode.Handle("read", func(msg maelstrom.Message) error {
		if err := maelstromNode.Reply(msg, map[string]interface{}{
			"type":     "read_ok",
			"messages": messages[maelstromNode.ID()],
		}); err != nil {
			log.Printf("Error sending message [%v]\n", err)
			return err
		}

		return nil
	})

	maelstromNode.Handle("topology", func(msg maelstrom.Message) error {
		reqBody := make(map[string]interface{})
		if err := json.Unmarshal(msg.Body, &reqBody); err != nil {
			log.Printf("Error unmarshalling message [%v]\n", err)
			return err
		}

		topology = reqBody["topology"].(map[string]interface{})

		if err := maelstromNode.Reply(msg, map[string]interface{}{
			"type": "topology_ok",
		}); err != nil {
			log.Printf("Error sending message [%v]\n", err)
			return err
		}

		return nil
	})

	if err := maelstromNode.Run(); err != nil {
		log.Fatal("Error running maelstrom node - ", err)
		return
	}
}

func replayRPCSend(replayCount int, waitPeriod time.Duration, maelstromNode *maelstrom.Node, payload map[string]interface{}, dest string, replayFunc func(maelstromNode *maelstrom.Node, payload map[string]interface{}, dest string) error) error {
	var err error
	for replay := 0; replay < replayCount; replay++ {
		if err = replayFunc(maelstromNode, payload, dest); err != nil {
			replay++
			time.Sleep(waitPeriod)
		} else {
			return nil
		}
	}

	return err
}
