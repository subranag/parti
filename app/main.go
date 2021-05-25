package main

import (
	"fmt"
	"math/big"

	"github.com/subranag/parti"
)

func main() {
	//ub := big.NewInt(13)
	ub := new(big.Int)
	_, ok := ub.SetString("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", 16)
	if !ok {
		panic("hex set did not go well")
	}
	p := &parti.Partition{Label: "part-A", LowerBound: big.NewInt(0), UpperBound: ub}
	splits, err := parti.NewEvenSplitter().Split(p, 100)
	if err != nil {
		panic(err)
	}

	for _, split := range splits {
		fmt.Println(split)
	}
}
