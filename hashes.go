package parti

import (
	"crypto/md5"
	"crypto/sha256"
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

func newMD5HashRange() HashRange {
	md5 := md5.New()
	lb := big.NewInt(0)
	ub := new(big.Int)
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

func newSHA256HashRange() HashRange {
	sha := sha256.New()
	lb := big.NewInt(0)
	ub := new(big.Int)
	// SHA256 is 256 bit hash
	ub.SetString("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", 16)
	return &sha256HashRange{sha, lb, ub}
}
