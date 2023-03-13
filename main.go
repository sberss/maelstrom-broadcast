package main

import (
	"log"
	"maelstrom-broadcast/internal/node"
)

func main() {
	n := node.NewNode()

	if err := n.Run(100); err != nil {
		log.Fatal(err)
	}
}
