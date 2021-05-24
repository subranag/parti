package parti

import (
	"errors"
	"fmt"
	"hash"
	"math/big"
	"strings"
)

var PartitionNilError = errors.New("provided partition cannot be nil")

var InvalidNumSplits = errors.New("a partition can only be split into partitions > 1")

var BigOne *big.Int = big.NewInt(1)
var BigZero *big.Int = big.NewInt(0)

//InvalidPartitionError error that is returned after validating the partition
//Error string contains the details of why the partition failed validation
type InvalidPartitionError struct {
	reason string
}

func (e *InvalidPartitionError) Error() string {
	return e.reason
}

func invalidPartition(reason string) *InvalidPartitionError {
	return &InvalidPartitionError{reason: reason}
}

//Partition represents a single partition in the partition map
//single partition is a unique range in the entire hash range partition map
//the key into a partition is the partition label which is guranteed to be unique
//the partition bounds are closed intervals not open intervals
//a partition does not have existence by itself it always belongs to a partition map
type Partition struct {
	//Label uniquely identifies a partition in the partition map
	Label string

	//LowerBound of the partition, i.e. any given key in this partition will have
	//value >=LowerBound
	LowerBound *big.Int

	//UpperBound of the partition, i.e. any given key in this partition will have
	//value <=UpperBound
	UpperBound *big.Int
}

func (p *Partition) String() string {
	if p == nil {
		return ""
	}
	return fmt.Sprintf("partition{label:%s, lb:%s, ub:%s}", p.Label, p.LowerBound.Text(16), p.UpperBound.Text(16))
}

//PartitionMap represents a hash range partition map, a partition Map contains one
//or more partitions as dictated by the HashRange, a partition map is uniquely identified
//by partition map name, an optional descreption can be provided that provides more context
//on the partition map
type PartitionMap struct {
	//Name uniquely identifies a partition
	Name string

	//Partitions are the partitions in this partition map as dictated by the HashRange
	Partitions []*Partition

	//Range is the HashRange of the partition map
	Range HashRange
}

//HashRange is a combination of a hash function and lower and upper bounds of the
//range of the hash function, the implicit assumption here is that the hash has a
//implicit lower bound and upper bound inclusive
type HashRange interface {
	//hash.Hash the hash range is always and extension of a general purpose
	//hash function
	hash.Hash

	//GetLowerBound gets the lower bound of this hash range the absolute smallest
	//value as supported by the hash function, this is typically 0
	GetLowerBound() *big.Int

	//GetUpperBound gets the upper bound of this hash range the absolute largest
	//value as supported by the hash function, this is typically of the form 2^n-1
	//where n is the n bit hash e.g.
	//MD5 is a 128 bit hash hence the max value of any hash output is
	//0xffffffffffffffffffffffffffffffff
	GetUpperBound() *big.Int
}

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
	fmt.Println(delta)

	// get divide result
	step := new(big.Int)
	step.Div(delta, big.NewInt(int64(numSplits)))
	fmt.Println(step)

	// get mod for left over
	leftOver := new(big.Int)
	leftOver.Mod(delta, big.NewInt(int64(numSplits)))
	fmt.Println(leftOver)

	if leftOver.Int64() >= int64(numSplits) {
		panic("this cannot happen something mod numSplits is greater than numSplits")
	}

	result := make([]*Partition, numSplits)
	var prevUpperBound *big.Int

	for i := 0; i < numSplits; i++ {

		if prevUpperBound == nil {
			label := fmt.Sprintf("%s-%d", p.Label, i)
			firstPart := &Partition{Label: label}

			// set first part lb to original part lb
			lb := new(big.Int)
			lb.SetString(p.LowerBound.String(), 10)
			firstPart.LowerBound = lb

			// set the upper bound

			// if we need to carry over we need
			carry := false
			if leftOver.Cmp(BigZero) > 0 {
				carry = true
				leftOver.Sub(leftOver, BigOne)
			}
			ub := new(big.Int)
			ub.Add(firstPart.LowerBound, step)

			if carry {
				ub.Add(ub, BigOne)
			}
			firstPart.UpperBound = ub

			result[i] = firstPart
			prevUpperBound = ub
			fmt.Println(firstPart)
			continue
		}
	}

	return result, nil
}

func validatePartition(p *Partition) error {

	if p == nil {
		return PartitionNilError
	}

	if p.LowerBound == nil || p.UpperBound == nil {
		return invalidPartition("neither partition LowerBound nor UpperBound cannot be nil")
	}

	if p.LowerBound.Cmp(p.UpperBound) >= 0 {
		return invalidPartition("partition LowerBound should be strictly less than UpperBound: LowerBound < UpperBound")
	}

	if p.Label == "" || strings.Trim(p.Label, " ") == "" {
		return invalidPartition("partition label can be whitespaces/empty")
	}

	return nil
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
