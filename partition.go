package parti

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"math/big"
	"sort"
	"strings"
	"sync"
)

var PartitionNilError = errors.New("provided partition cannot be nil")

var InvalidNumSplits = errors.New("a partition can only be split into partitions > 1")

var InvalidKey = errors.New("Invalid key provided, key should be []byte with len > 0")

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
	//label uniquely identifies a partition in the partition map
	label string

	//lowerBound of the partition, i.e. any given key in this partition will have
	//value >=lowerBound
	lowerBound *big.Int

	//upperBound of the partition, i.e. any given key in this partition will have
	//value <=upperBound
	upperBound *big.Int
}

func (p *Partition) Label() string {
	return p.label
}

func (p *Partition) LowerBound() string {
	return p.lowerBound.Text(16)
}

func (p *Partition) UpperBound() string {
	return p.upperBound.Text(16)
}

func (p *Partition) String() string {
	if p == nil {
		return ""
	}
	return fmt.Sprintf("partition{label:%s, lb:%s, ub:%s}", p.label, p.lowerBound.Text(16), p.upperBound.Text(16))
}

func (p *Partition) MarshalJSON() ([]byte, error) {
	sb := bytes.NewBufferString("{")
	sb.WriteString(fmt.Sprintf("\"%v\":\"%v\",", "label", p.label))
	sb.WriteString(fmt.Sprintf("\"%v\":\"%v\",", "lower_bound", p.lowerBound.Text(16)))
	sb.WriteString(fmt.Sprintf("\"%v\":\"%v\"", "upper_bound", p.upperBound.Text(16)))
	sb.WriteString("}")
	return sb.Bytes(), nil
}

func (p *Partition) UnmarshalJSON(b []byte) error {
	pdata := make(map[string]string)
	if err := json.Unmarshal(b, pdata); err != nil {
		return err
	}
	p.label = pdata["label"]
	lb := new(big.Int)
	if _, ok := lb.SetString(pdata["lower_bound"], 16); !ok {
		return invalidPartition(fmt.Sprintf("invalid lower bound %v", pdata["lower_bound"]))
	}
	p.lowerBound = lb

	ub := new(big.Int)
	if _, ok := ub.SetString(pdata["upper_bound"], 16); !ok {
		return invalidPartition(fmt.Sprintf("invalid upper bound %v", pdata["lower_bound"]))
	}
	p.upperBound = ub
	return nil
}

func validatePartition(p *Partition) error {

	if p == nil {
		return PartitionNilError
	}

	if p.lowerBound == nil || p.upperBound == nil {
		return invalidPartition("neither partition LowerBound nor UpperBound cannot be nil")
	}

	if p.lowerBound.Cmp(p.upperBound) >= 0 {
		return invalidPartition("partition LowerBound should be strictly less than UpperBound: LowerBound < UpperBound")
	}

	if p.label == "" || strings.Trim(p.label, " ") == "" {
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
	Name string `json:"map_name"`

	//Partitions are the partitions in this partition map as dictated by the HashRange
	Partitions []*Partition `json:"partitions"`

	//Range is the HashRange of the partition map
	Range HashRange `json:"hash_function"`

	//keyMap is a map of partition labels to respective partitions
	//NOTE: this filed is not exposed
	keyMap map[string]*Partition `json:"-"`

	//hashMu is the mutex used when resolving a key and manages
	//safe concurrent access to calculation of hash given a key byte
	//sequence
	hashMu sync.Mutex

	//sortMu is the mutex used to sort the Partitions in the partition map
	//while resolving the key we use a binary search to isolate the key
	//so the partitions need to be sorted
	sortMu sync.Mutex
}

//ResolvePartition resolves the partition for the given key
//the hash of the key is calculated and slotted to the correct range
//in the partition map
func (pm *PartitionMap) ResolvePartition(key []byte) (*Partition, error) {
	if len(key) == 0 {
		return nil, InvalidKey
	}

	hash := pm.keyHash(key)
	fmt.Println(hash.Text(16))
	return nil, nil
}

func (pm *PartitionMap) keyHash(key []byte) *big.Int {
	pm.hashMu.Lock()
	defer pm.hashMu.Unlock()

	pm.Range.Reset()
	pm.Range.Write(key)
	hash := new(big.Int)
	hash.SetBytes(pm.Range.Sum(nil))
	return hash
}

func (pm *PartitionMap) sortPartitions() {
	sorted := sort.SliceIsSorted(pm.Partitions, func(i, j int) bool {
		return pm.Partitions[i].lowerBound.Cmp(pm.Partitions[j].lowerBound) < 0
	})

	if !sorted {
		pm.sortMu.Lock()
		defer pm.sortMu.Unlock()
		sort.Slice(pm.Partitions, func(i, j int) bool {
			return pm.Partitions[i].lowerBound.Cmp(pm.Partitions[j].lowerBound) < 0
		})
	}
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
	root := &Partition{label: partLabelPrefix,
		lowerBound: h.LowerBound(),
		upperBound: h.UpperBound()}

	// split this root partition using an even splitter
	splits, err := splitter.Split(root, numSplits)

	if err != nil {
		return nil, err
	}

	//new partition map
	keyMap := make(map[string]*Partition, len(splits))
	for _, v := range splits {
		keyMap[v.label] = v
	}
	pm := &PartitionMap{Name: name, Partitions: splits, Range: h, keyMap: keyMap}
	pm.sortPartitions()
	return pm, nil
}

//HashRange is a combination of a hash function and lower and upper bounds of the
//range of the hash function, the implicit assumption here is that the hash has a
//implicit lower bound and upper bound inclusive
type HashRange interface {
	//json.Marshaler hash range needs to provide a JSON Marshaler
	json.Marshaler

	//json.Unmarshaler hash range needs to provide a JSON Unmarshaler
	json.Unmarshaler

	//hash.Hash the hash range is always and extension of a general purpose
	//hash function
	hash.Hash

	//LowerBound gets the lower bound of this hash range the absolute smallest
	//value as supported by the hash function, this is typically 0
	LowerBound() *big.Int

	//UpperBound gets the upper bound of this hash range the absolute largest
	//value as supported by the hash function, this is typically of the form 2^n-1
	//where n is the n bit hash e.g.
	//MD5 is a 128 bit hash hence the max value of any hash output is
	//0xffffffffffffffffffffffffffffffff
	UpperBound() *big.Int

	//HashFunctionName returns the human readable name of the hash function
	//backing this HashRange e.g. MD5, SHA256, SPOOKY, MURMUR etc
	HashFunctionName() string
}
