package node

import (
	"context"
	"encoding/json"
	"maelstrom-broadcast/internal/broadcast"
	"maelstrom-broadcast/internal/message"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Node struct {
	*maelstrom.Node
	neighbours     []string
	messageStore   *message.Store
	broadcastQueue *broadcast.Queue
}

// NewNode returns a new maelstrom node configured with message handlers.
func NewNode() *Node {
	n := &Node{
		Node:         maelstrom.NewNode(),
		messageStore: message.NewStore(),
	}

	n.Handle("broadcast", n.broadcastHandler)
	n.Handle("read", n.readHandler)
	n.Handle("topology", n.topologyHandler)

	return n
}

// Run starts the node and any dependencies.
func (n *Node) Run(workers int) error {
	broadcastFunc := func(dst string, message float64) error {
		body := make(map[string]any)
		body["type"] = "broadcast"
		body["message"] = message

		ctxTimeout, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()

		_, err := n.SyncRPC(ctxTimeout, dst, body)

		return err
	}

	n.broadcastQueue = broadcast.NewQueue(broadcastFunc)
	n.broadcastQueue.Run(workers)

	if err := n.Node.Run(); err != nil {
		return err
	}

	return nil
}

// broadcastHandler handles broadcast messages.
func (n *Node) broadcastHandler(msg maelstrom.Message) error {
	var reqBody map[string]any
	if err := json.Unmarshal(msg.Body, &reqBody); err != nil {
		return err
	}

	reqMsg := reqBody["message"].(float64)
	if !n.messageStore.Has(reqMsg) {
		for _, node := range n.neighbours {
			if node != msg.Src {
				n.broadcastQueue.Enqueue(node, reqMsg)
			}
		}
		n.messageStore.Add(reqMsg)
	}

	// Ack
	resBody := make(map[string]any)
	resBody["type"] = "broadcast_ok"
	return n.Reply(msg, resBody)
}

// readHandler handles read messages.
func (n *Node) readHandler(msg maelstrom.Message) error {
	body := make(map[string]any)
	body["type"] = "read_ok"
	body["messages"] = n.messageStore.Messages()
	return n.Reply(msg, body)
}

// topologyHandler handles topology messages.
func (n *Node) topologyHandler(msg maelstrom.Message) error {
	type topologyMessageBody struct {
		maelstrom.MessageBody
		Topology map[string][]string `json:"topology"`
	}

	var reqBody *topologyMessageBody
	if err := json.Unmarshal(msg.Body, &reqBody); err != nil {
		return err
	}

	n.neighbours = reqBody.Topology[n.ID()]

	// Ack
	body := make(map[string]any)
	body["type"] = "topology_ok"
	return n.Reply(msg, body)

}
