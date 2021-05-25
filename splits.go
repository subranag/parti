package parti

import (
	"fmt"
	"math/big"
)

//Splitter represents an abstraction that can split a given partition into
//required number of partitions, a Splitter understands how to split a partition
//implementations can use any strategy to split the partitions as long as they are valid
//see reference implementation NewEvenSplitter for details
type Splitter interface {

	//Split splits the given partition p into numSplits if the splitter cannot split
	//the given partition then it returns an error
	Split(p *Partition, numSplits int) ([]*Partition, error)
}

type evenSplitter struct{}

func (e *evenSplitter) Split(p *Partition, numSplits int) ([]*Partition, error) {
	if p == nil {
		return nil, PartitionNilError
	}

	if numSplits <= 1 {
		return nil, InvalidNumSplits
	}

	if err := validatePartition(p); err != nil {
		return nil, err
	}

	// first get delta
	delta := new(big.Int)
	delta.Sub(p.UpperBound, p.LowerBound)

	// get divide result
	step := new(big.Int)
	step.Div(delta, big.NewInt(int64(numSplits)))

	// get mod for left over
	leftOver := new(big.Int)
	leftOver.Mod(delta, big.NewInt(int64(numSplits)))

	if leftOver.Int64() >= int64(numSplits) {
		// THIS SHOULD NEVER HAPPEN
		panic("this cannot happen something mod numSplits is greater than numSplits")
	}

	result := make([]*Partition, numSplits)
	var prevUpperBound *big.Int

	for i := 0; i < numSplits; i++ {

		label := fmt.Sprintf("%s-%d", p.Label, i)
		newPart := &Partition{Label: label}
		lb := new(big.Int)

		if prevUpperBound == nil {
			// set first part lb to original part lb
			lb.SetString(p.LowerBound.String(), 10)
			prevUpperBound = lb
		} else {
			lb.Add(prevUpperBound, BigOne)
		}

		newPart.LowerBound = lb
		newPart.UpperBound = setUpperBoundAndCarry(prevUpperBound, step, leftOver)

		result[i] = newPart
		prevUpperBound = newPart.UpperBound
	}

	return result, nil
}

func setUpperBoundAndCarry(base, step, leftOver *big.Int) *big.Int {
	// set the upper bound
	ub := new(big.Int)
	ub.Add(base, step)

	// if we need to carry over we need
	carry := false
	if leftOver.Cmp(BigZero) > 0 {
		carry = true
		leftOver.Sub(leftOver, BigOne)
	}
	if carry {
		ub.Add(ub, BigOne)
	}
	return ub
}

//NewEvenSplitter creates a Splitter that divides partitions into even splits
//here is the splitting logic
//
// Step1: calculate the range delta of the partition to be split
// range := (p.UpperBound - p.LowerBound) / numSplits
//
// Step2: see if does not evenly divide
// leftOver := (p.UpperBound - p.LowerBound) % numSplits
// if leftOver == 0 then numSplits evenly divides the partition
// if leftOver > 0 then we need to distribute numSplits evenly across the splits
// e.g. if numSplits = 10 and leftOver = 3 then we need to distribute 3 to some of the 10 splits
//
// Step3: from the p.LowerBound keep adding delta and generate closed intervals until
// the numSplits criteria is met
func NewEvenSplitter() Splitter {
	return &evenSplitter{}
}
