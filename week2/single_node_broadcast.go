package week2

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func Single_node_broadcast() {
	var mu sync.Mutex
	messages := make([]int, 0)

	maelstromNode := maelstrom.NewNode()
	maelstromNode.Handle("broadcast", func(msg maelstrom.Message) error {
		reqBody := make(map[string]interface{})
		if err := json.Unmarshal(msg.Body, &reqBody); err != nil {
			log.Printf("Error unmarshalling message [%v]\n", err)
			return err
		}

		message := int(reqBody["message"].(float64))

		mu.Lock()
		messages = append(messages, message)
		mu.Unlock()

		if err := maelstromNode.Reply(msg, map[string]interface{}{
			"type": "broadcast_ok",
		}); err != nil {
			log.Printf("Error sending message [%v]\n", err)
			return err
		}

		return nil
	})

	maelstromNode.Handle("read", func(msg maelstrom.Message) error {
		if err := maelstromNode.Reply(msg, map[string]interface{}{
			"type":     "read_ok",
			"messages": messages,
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
