package main

import (
	"fmt"

	"github.com/subranag/parti"
)

func main() {
	pmap, err := parti.NewMD5PartitionMap("key_store", "partition", 100)
	if err != nil {
		fmt.Println(err)
		return
	}

	for _, p := range pmap.Partitions {
		fmt.Println(p)
	}
}
