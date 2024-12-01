package db

import "os"

// btree represents the in-memory B-tree structure.
// It manages the root node and provides methods for interacting with the tree.
type btree struct {
	root node // The root node of the B-tree.
}

// node defines the interface that all nodes (e.g., leaf and internal) must implement.
// It provides methods for inserting, deleting, retrieving, and printing elements in the tree.
type node interface {
	// InsertPair inserts a key-value pair into the node.
	// The node may split if it exceeds its capacity, and the split may propagate upwards.
	// Parameters:
	// - value: A pointer to the key-value pair to be inserted.
	// - bt: A reference to the B-tree to which the node belongs.
	// Returns: An error if the insertion fails.
	InsertPair(value *pairs, bt *btree) error

	// Delete removes a key-value pair from the node.
	// The node may merge with its siblings if it becomes underfilled, and the merge may propagate upwards.
	// Parameters:
	// - key: The key to be Deleted from the node.
	// - bt: A reference to the B-tree to which the node belongs.
	// Returns: An error if the deletion fails.
	Delete(key string, bt *btree) error

	// GetValue retrieves the value associated with a key in the node.
	// Parameters:
	// - key: The key to search for in the node.
	// Returns: The value associated with the key, and an error if the key is not found.
	GetValue(key string) (string, error)

	// PrintTree prints the structure of the node and its descendants.
	// Parameters:
	// - level: The depth level of the node in the tree (used for indentation).
	PrintTree(level int)
}

// isRootNode checks if a given node is the root node of the B-tree.
// Parameters:
// - n: The node to check.
// Returns: True if the node is the root, otherwise false.
func (bt *btree) isRootNode(n node) bool {
	return bt.root == n
}

// initializeBtree initializes and returns a new B-tree.
// If a file path is provided, it is used to persist the B-tree; otherwise, a default path is used.
// Parameters:
// - path: Optional. A file path to store the B-tree data.
// Returns: A pointer to the initialized B-tree and an error if the operation fails.
func initializeBtree(path ...string) (*btree, error) {
	if len(path) == 0 {
		path = make([]string, 1)
		path[0] = "./db/freedom.db"
	}

	file, err := os.OpenFile(path[0], os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	dns := NewDiskNodeService(file)

	root, err := dns.getRootNodeFromDisk()
	if err != nil {
		panic(err)
	}
	return &btree{root: root}, nil
}

// insert adds a key-value pair to the B-tree.
// The insertion is delegated to the root node, and the root may split if necessary.
// Parameters:
// - value: A pointer to the key-value pair to be inserted.
// Returns: An error if the insertion fails.
func (bt *btree) insert(value *pairs) error {
	return bt.root.InsertPair(value, bt)
}

// get retrieves the value associated with a key in the B-tree.
// The retrieval is delegated to the root node.
// Parameters:
// - key: The key to search for in the B-tree.
// Returns: The value associated with the key, a boolean indicating if the key was found, and an error if the operation fails.
func (bt *btree) get(key string) (string, bool, error) {
	value, err := bt.root.GetValue(key)
	if err != nil {
		return "", false, err
	}
	if value == "" {
		return "", false, nil
	}
	return value, true, nil
}

// setRootNode sets the root node of the B-tree.
// This is used when the root node changes due to splits or merges.
// Parameters:
// - n: The new root node.
func (bt *btree) setRootNode(n node) {
	bt.root = n
}

// del Deletes a key-value pair from the B-tree.
// The deletion is delegated to the root node, and the root may merge if necessary.
// Parameters:
// - key: The key to be Deleted from the B-tree.
// Returns: An error if the deletion fails.
func (bt *btree) del(key string) error {
	err := bt.root.Delete(key, bt)
	return err
}
