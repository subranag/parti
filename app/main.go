package main

import (
	"fmt"

	"github.com/subranag/parti"
)

func main() {
	pmap, err := parti.NewSHA256PartitionMap("key_store", "partition", 100)
	if err != nil {
		fmt.Println(err)
		return
	}
	pmap.ResolvePartition([]byte("subbu is cool"))
}
