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

var splitter = NewEvenSplitter()

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

	//keyMap is a map of partition labels to respective partitions
	//NOTE: this filed is not exposed
	keyMap map[string]*Partition
}

//NewMD5PartitionMap creates a partition map backed by the MD5 hash function
//
//name and partition label prefix have to be provided
//
//numSplits should be > 1
func NewMD5PartitionMap(name, partLabelPrefix string, numSplits int) (*PartitionMap, error) {
	return NewPartitionMap(newMD5HashRange(), name, partLabelPrefix, numSplits)
}

//NewSHA256PartitionMap creates a partition map backed by the SHA256 hash function
//
//name and partition label prefix have to be provided
//
//numSplits should be > 1
func NewSHA256PartitionMap(name, partLabelPrefix string, numSplits int) (*PartitionMap, error) {
	return NewPartitionMap(newSHA256HashRange(), name, partLabelPrefix, numSplits)
}

//NewPartitionMap makes a partition map
//
//the partition map will be backed by the provided HashRange
//
//name and partition label prefix have to be provided
//
//numSplits should be > 1
func NewPartitionMap(h HashRange, name, partLabelPrefix string, numSplits int) (*PartitionMap, error) {
	//create root partition that will be split
	root := &Partition{Label: partLabelPrefix,
		LowerBound: h.GetLowerBound(),
		UpperBound: h.GetUpperBound()}

	// split this root partition using an even splitter
	splits, err := splitter.Split(root, numSplits)

	if err != nil {
		return nil, err
	}

	//new partition map
	keyMap := make(map[string]*Partition, len(splits))
	for _, v := range splits {
		keyMap[v.Label] = v
	}
	return &PartitionMap{Name: name, Partitions: splits, Range: h, keyMap: keyMap}, nil
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
