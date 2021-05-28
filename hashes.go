package parti

import (
	"crypto/md5"
	"crypto/sha256"
	"fmt"
	"hash"
	"math/big"
)

type md5HashRange struct {
	hash.Hash
	lowerBound *big.Int
	upperBound *big.Int
}

func (md5 *md5HashRange) GetLowerBound() *big.Int {
	return md5.lowerBound
}

func (md5 *md5HashRange) GetUpperBound() *big.Int {
	return md5.upperBound
}

func (md5 *md5HashRange) HashFunctionName() string {
	return "MD5"
}

func (md5 *md5HashRange) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("\"%v\"", md5.HashFunctionName())), nil
}

func (md5 *md5HashRange) UnmarshalJSON(b []byte) error {
	// nothing to do while un-marshalling
	return nil
}

func newMD5HashRange() HashRange {
	md5 := md5.New()
	lb := big.NewInt(0)
	ub := new(big.Int)
	// MD5 is 128 bit hash so ub is 2^128 - 1
	ub.SetString("ffffffffffffffffffffffffffffffff", 16)
	return &md5HashRange{md5, lb, ub}
}

type sha256HashRange struct {
	hash.Hash
	lowerBound *big.Int
	upperBound *big.Int
}

func (sha *sha256HashRange) GetLowerBound() *big.Int {
	return sha.lowerBound
}

func (sha *sha256HashRange) GetUpperBound() *big.Int {
	return sha.upperBound
}

func (sha *sha256HashRange) HashFunctionName() string {
	return "SHA256"
}

func (sha *sha256HashRange) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("\"%v\"", sha.HashFunctionName())), nil
}

func (sha *sha256HashRange) UnmarshalJSON(b []byte) error {
	// nothing to do while un-marshalling
	return nil
}

func newSHA256HashRange() HashRange {
	sha := sha256.New()
	lb := big.NewInt(0)
	ub := new(big.Int)
	// SHA256 is 256 bit hash so ub is 2^256 - 1
	ub.SetString("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", 16)
	return &sha256HashRange{sha, lb, ub}
}
