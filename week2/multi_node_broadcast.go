package week2

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func Multi_node_broadcast() {
	var mu sync.Mutex
	// TODO: node Id to messages link required? Doesn't nodes have it's own working memory?
	messages := make(map[string][]int, 0)
	topology := make(map[string]interface{}, 0)

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
			if err := maelstromNode.RPC(adjacentNode.(string), payload, func(msg maelstrom.Message) error {
				reqBody := make(map[string]interface{})
				if err := json.Unmarshal(msg.Body, &reqBody); err != nil {
					log.Printf("Error unmarshalling message [%v]\n", err)
					return err
				}

				if reqBody["type"].(string) != "broadcast_ok" {
					return errors.New("broadcast response failure")
				}

				return nil
			}); err != nil {
				log.Printf("Error broadcasting message to node [%v] - [%v]\n", adjacentNode, err)
			}
		}

		return nil
	})

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
