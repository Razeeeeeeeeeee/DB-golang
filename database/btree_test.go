package db

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func clearDB() string {
	path := "./db/test.db"
	if _, err := os.Stat(path); err == nil {
		// path/to/whatever exists
		err := os.Remove(path)
		if err != nil {
			panic(err)
		}
	}
	return path
}

func TestBtreeInsert(t *testing.T) {
	tree, err := initializeBtree(clearDB())
	if err != nil {
		t.Error(err)
	}
	for i := 1; i <= 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		if i == 230 {
			println("Inserted 229 elements")
		}
		tree.insert(newPair(key, value))
	}
	// tree.root.PrintTree()
}

func TestBtreeGet(t *testing.T) {
	tree, err := initializeBtree(clearDB())
	if err != nil {
		t.Error(err)
	}
	totalElements := 500
	for i := 1; i <= totalElements; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		tree.insert(newPair(key, value))
	}

	for i := 1; i <= totalElements; i++ {
		key := fmt.Sprintf("key-%d", i)
		value, found, err := tree.get(key)
		if err != nil {
			t.Error(err)
		}
		if !found || value == "" {
			t.Error("Value should be found ", key)
		}
	}

	for i := totalElements + 1; i <= totalElements+1+1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		_, found, err := tree.get(key)
		if err != nil {
			t.Error(err)
		}
		if found {
			t.Error("Value should not be found")

		}
	}
}

// Helper function to create a test tree with sample data
func createTestTree(t *testing.T) *btree {
	// Create a temporary file for the block service
	bt, err := initializeBtree(clearDB())
	if err != nil {
		t.Error(err)
	}

	// Create initial root node

	// Insert test data
	testData := []struct {
		key   string
		value string
	}{
		{"10", "value10"},
		{"20", "value20"},
		{"30", "value30"},
		{"40", "value40"},
		{"50", "value50"},
		{"60", "value60"},
		{"70", "value70"},
		{"80", "value80"},
		{"90", "value90"},
	}

	for _, data := range testData {
		err := bt.insert(&pairs{key: data.key, value: data.value})
		assert.NoError(t, err)
	}

	return bt
}

// Helper function to verify tree structure
func verifyTreeStructure(t *testing.T, n *DiskNode) {
	// Verify node properties
	if !n.isLeaf() {
		// Non-leaf node checks
		assert.True(t, len(n.keys) > 0, "Non-leaf node must have at least one key")
		assert.Equal(t, len(n.childrenBlockIDs), len(n.keys)+1, "Number of children should be number of keys + 1")

		// Verify child nodes
		children, err := n.getChildNodes()
		assert.NoError(t, err)

		for i, child := range children {
			if i > 0 {
				// Verify keys are in order
				assert.True(t, n.keys[i-1].key < child.keys[0].key,
					"Child's first key must be greater than parent's key")
			}
			// Recursively verify child nodes
			verifyTreeStructure(t, child)
		}
	} else {
		if !n.blockService.isRootNode(n) {
			minSize := (n.blockService.getMaxLeafSize() + 1) / 2
			assert.True(t, len(n.keys) >= minSize,
				"Leaf node must maintain minimum number of keys")
		}
	}
}

func TestDeleteSimple(t *testing.T) {
	bt := createTestTree(t)

	// Delete a key that doesn't require rebalancing
	err := bt.root.delete("90", bt)
	assert.NoError(t, err)

	// Verify key is deleted
	val, err := bt.root.getValue("90")
	assert.NoError(t, err)
	assert.Empty(t, val)

	// Verify tree structure is still valid
	diskNode, ok := bt.root.(*DiskNode)
	assert.True(t, ok)
	verifyTreeStructure(t, diskNode)
}

func TestDeleteKeyNotFound(t *testing.T) {
	bt := createTestTree(t)

	// Try to delete non-existent key
	err := bt.root.delete("95", bt)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "key 95 not found")
}

func TestDeleteRequiringBorrow(t *testing.T) {
	bt := createTestTree(t)

	// First, make a situation where borrowing will be needed
	// Delete keys to create an underflow situation
	keysToDelete := []string{"20", "30"}
	for _, key := range keysToDelete {
		err := bt.root.delete(key, bt)
		assert.NoError(t, err)
	}

	// Verify tree structure after borrowing operation
	diskNode, ok := bt.root.(*DiskNode)
	assert.True(t, ok)
	verifyTreeStructure(t, diskNode)

	// Verify all remaining keys are still accessible
	remainingKeys := []string{"10", "40", "50", "60", "70", "80", "90"}
	for _, key := range remainingKeys {
		val, err := bt.root.getValue(key)
		assert.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("value%s", key), val)
	}
}

func TestDeleteRequiringMerge(t *testing.T) {
	bt := createTestTree(t)

	// Create a situation where merging will be needed
	// Delete enough keys to force a merge operation
	keysToDelete := []string{"20", "30", "40", "50"}
	for _, key := range keysToDelete {
		err := bt.root.delete(key, bt)
		assert.NoError(t, err)
	}

	// Verify tree structure after merge operation
	diskNode, ok := bt.root.(*DiskNode)
	assert.True(t, ok)
	verifyTreeStructure(t, diskNode)

	// Verify all remaining keys are still accessible
	remainingKeys := []string{"10", "60", "70", "80", "90"}
	for _, key := range remainingKeys {
		val, err := bt.root.getValue(key)
		assert.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("value%s", key), val)
	}
}

func TestDeleteAllElements(t *testing.T) {
	bt := createTestTree(t)

	// Delete all keys
	allKeys := []string{"10", "20", "30", "40", "50", "60", "70", "80", "90"}
	for _, key := range allKeys {
		err := bt.root.delete(key, bt)
		assert.NoError(t, err)
	}

	// Verify tree is empty but valid
	diskNode, ok := bt.root.(*DiskNode)
	assert.True(t, ok)
	assert.True(t, len(diskNode.keys) == 0, "Root should be empty")
	verifyTreeStructure(t, diskNode)

	// Verify no keys are accessible
	for _, key := range allKeys {
		val, err := bt.root.getValue(key)
		assert.NoError(t, err)
		assert.Empty(t, val)
	}
}

func TestDeleteWithRootChange(t *testing.T) {
	bt := createTestTree(t)

	// Create a situation where root will change
	// Delete enough keys to force root change
	keysToDelete := []string{"20", "30", "40", "50", "60", "70", "80"}
	for _, key := range keysToDelete {
		err := bt.root.delete(key, bt)
		assert.NoError(t, err)
	}

	// Verify new root structure
	diskNode, ok := bt.root.(*DiskNode)
	assert.True(t, ok)
	verifyTreeStructure(t, diskNode)

	// Verify remaining keys are accessible
	remainingKeys := []string{"10", "90"}
	for _, key := range remainingKeys {
		val, err := bt.root.getValue(key)
		assert.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("value%s", key), val)
	}
}

func TestConcurrentDeletes(t *testing.T) {
	bt := createTestTree(t)

	// Test concurrent deletes
	done := make(chan bool)
	errChan := make(chan error)

	keysToDelete := []string{"30", "50", "70", "90"}

	for _, key := range keysToDelete {
		go func(k string) {
			err := bt.root.delete(k, bt)
			if err != nil {
				errChan <- err
				return
			}
			done <- true
		}(key)
	}

	// Wait for all deletes to complete
	for i := 0; i < len(keysToDelete); i++ {
		select {
		case err := <-errChan:
			t.Fatalf("Error during concurrent delete: %v", err)
		case <-done:
			continue
		}
	}

	// Verify tree structure is still valid
	diskNode, ok := bt.root.(*DiskNode)
	assert.True(t, ok)
	verifyTreeStructure(t, diskNode)

	// Verify deleted keys are not accessible
	for _, key := range keysToDelete {
		val, err := bt.root.getValue(key)
		assert.NoError(t, err)
		assert.Empty(t, val)
	}

	// Verify remaining keys are still accessible
	remainingKeys := []string{"10", "20", "40", "60", "80"}
	for _, key := range remainingKeys {
		val, err := bt.root.getValue(key)
		assert.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("value%s", key), val)
	}
}
