package main

import (
	"encoding/json"
	"fmt"

	"github.com/subranag/parti"
)

func main() {
	pmap, err := parti.NewSHA256PartitionMap("key_store", "partition", 100)
	if err != nil {
		fmt.Println(err)
		return
	}

	pmapJson, err := json.Marshal(pmap)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(string(pmapJson))
}
