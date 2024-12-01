package db

import (
	"encoding/binary"
	"fmt"
)

// Constants defining the size of a key-value pair and the maximum lengths of key and value
const pairSize = 124
const maxKeyLength = 30
const maxValueLength = 93

// pairs represents a key-value pair with lengths for the key and value, along with the actual key and value strings.
type pairs struct {
	keyLen   uint16 // 2 bytes for key length
	valueLen uint16 // 2 bytes for value length
	key      string // Key string (maximum length 30)
	value    string // Value string (maximum length 93)
}

// setKey sets the key and its length in the pairs structure.
func (p *pairs) setKey(key string) {
	p.key = key
	p.keyLen = uint16(len(key))
}

// setValue sets the value and its length in the pairs structure.
func (p *pairs) setValue(value string) {
	p.value = value
	p.valueLen = uint16(len(value))
}

// validate checks if the key and value are valid. It ensures the key is not empty, the value is not empty,
// and that the lengths of the key and value do not exceed their respective maximum lengths.
func (p *pairs) validate() error {
	if p.key == "" {
		return fmt.Errorf("Key should not be empty")
	}
	if p.value == "" {
		return fmt.Errorf("Value should not be empty")
	}
	if len(p.key) > maxKeyLength {
		return fmt.Errorf("key length should not be more than 30, currently it is %d", len(p.key))
	}
	if len(p.value) > maxValueLength {
		return fmt.Errorf("value length should not be more than 93, currently it is %d", len(p.value))
	}
	return nil
}

// newPair creates a new pairs instance with the given key and value.
func newPair(key string, value string) *pairs {
	pair := new(pairs)
	pair.setKey(key)
	pair.setValue(value)
	return pair
}

// convertPairsToBytes converts a pairs instance to a byte slice.
// The resulting byte slice contains the key length, value length, key, and value in a specific format.
func convertPairsToBytes(pair *pairs) []byte {
	pairByte := make([]byte, pairSize)
	var pairOffset uint16
	pairOffset = 0
	copy(pairByte[pairOffset:], uint16ToBytes(pair.keyLen)) // Copy key length
	pairOffset += 2
	copy(pairByte[pairOffset:], uint16ToBytes(pair.valueLen)) // Copy value length
	pairOffset += 2
	keyByte := []byte(pair.key)
	copy(pairByte[pairOffset:], keyByte[:pair.keyLen]) // Copy key bytes
	pairOffset += pair.keyLen
	valueByte := []byte(pair.value)
	copy(pairByte[pairOffset:], valueByte[:pair.valueLen]) // Copy value bytes
	return pairByte
}

// convertBytesToPair converts a byte slice back into a pairs instance.
// The byte slice should represent a key-value pair in the format defined by convertPairsToBytes.
func convertBytesToPair(pairByte []byte) *pairs {
	pair := new(pairs)
	var pairOffset uint16
	pairOffset = 0
	// Read key length from byte slice
	pair.keyLen = uint16FromBytes(pairByte[pairOffset:])
	pairOffset += 2
	// Read value length from byte slice
	pair.valueLen = uint16FromBytes(pairByte[pairOffset:])
	pairOffset += 2
	// Extract key and value from the byte slice
	pair.key = string(pairByte[pairOffset : pairOffset+pair.keyLen])
	pairOffset += pair.keyLen
	pair.value = string(pairByte[pairOffset : pairOffset+pair.valueLen])
	return pair
}

// uint16FromBytes converts a byte slice to a uint16 value using Little Endian byte order.
func uint16FromBytes(b []byte) uint16 {
	i := uint16(binary.LittleEndian.Uint64(b))
	return i
}

// uint16ToBytes converts a uint16 value to a byte slice using Little Endian byte order.
func uint16ToBytes(value uint16) []byte {
	b := make([]byte, 2)
	binary.LittleEndian.PutUint16(b, uint16(value))
	return b
}
