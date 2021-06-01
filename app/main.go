package main

import (
	"encoding/json"
	"fmt"

	"github.com/subranag/parti"
)

func main() {
	pmap, err := parti.NewMD5PartitionMap("key_store", "partition", 100)
	if err != nil {
		fmt.Println(err)
		return
	}
	pmapJson, _ := json.Marshal(pmap)
	fmt.Println(string(pmapJson))
	pmap.ResolvePartition([]byte("nagarajan "))
}
