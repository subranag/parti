package main

import (
	"math/big"

	"github.com/subranag/parti"
)

func main() {
	p := &parti.Partition{Label: "part-A", LowerBound: big.NewInt(0), UpperBound: big.NewInt(13)}
	parti.NewEvenSplitter().Split(p, 4)
}
