package db

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// DB represents a connection to a database, managing access to its B-tree structure.
// It provides methods for inserting, retrieving, and deleting key-value pairs in a thread-safe manner.
type DB struct {
	storage *btree       // The B-tree used for storing data.
	mu      sync.RWMutex // The read-write mutex to synchronize database operations.
}

// dbConnections manages multiple database instances, ensuring each instance is unique per file path.
// It uses a mutex to protect concurrent access to the database connections.
var dbConnections = struct {
	mu        sync.Mutex     // Mutex to lock access to dbConnections.
	instances map[string]*DB // A map of file paths to database instances.
}{
	instances: make(map[string]*DB), // Initializes the map of database instances.
}

// Open opens a new database connection at the specified file path.
// It ensures that the directory exists and creates it if necessary, and returns an existing connection if one already exists.
// Parameters:
// - filePath: The file path where the database should be stored or accessed.
// Returns: A pointer to the DB instance, and an error if the connection cannot be established.
func Open(filePath string) (*DB, error) {
	dbConnections.mu.Lock()
	defer dbConnections.mu.Unlock()

	// Ensure the directory for the database file exists, create it if not.
	dir := filepath.Dir(filePath)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		err := os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			fmt.Printf("Error creating directory: %v\n", err)
		}
		fmt.Printf("Directory created: %s\n", dir)
	}

	// Return the existing connection if one already exists for the given file path.
	if db, exists := dbConnections.instances[filePath]; exists {
		return db, nil
	}

	// Create a new connection and B-tree storage if no connection exists for the file path.
	storage, err := initializeBtree(filePath)
	if err != nil {
		return nil, err
	}
	db := &DB{
		storage: storage,
		mu:      sync.RWMutex{},
	}
	// Save the new DB instance to the map of database instances.
	dbConnections.instances[filePath] = db
	return db, nil
}

// Put inserts a key-value pair into the database, ensuring the pair is valid before insertion.
// The method locks the database for exclusive write access while inserting the pair.
// Parameters:
// - key: The key to be inserted.
// - value: The value associated with the key to be inserted.
// Returns: An error if the insertion fails.
func (db *DB) Put(key string, value string) error {
	pair := newPair(key, value)
	if err := pair.validate(); err != nil {
		return err
	}
	// Lock the database for exclusive write access while inserting.
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.storage.insert(pair)
}

// Get retrieves the value associated with a key from the database.
// The method locks the database for read access while retrieving the value.
// Parameters:
// - key: The key for which the value is to be retrieved.
// Returns: The value associated with the key, a boolean indicating if the key was found, and an error if the retrieval fails.
func (db *DB) Get(key string) (string, bool, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.storage.get(key)
}

// Del deletes the key-value pair associated with the specified key from the database.
// The method locks the database for exclusive write access while deleting the pair.
// Parameters:
// - key: The key to be deleted.
// Returns: An error if the deletion fails.
func (db *DB) Del(key string) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.storage.del(key)
}

// Close closes the database connection for the specified file path and releases associated resources.
// The method ensures the database is not already closed before proceeding with the closure.
// Parameters:
// - filePath: The file path of the database to be closed.
// Returns: An error if the database is already closed or if any issues arise during the closure.
func (db *DB) Close(filePath string) error {
	dbConnections.mu.Lock()
	defer dbConnections.mu.Unlock()

	// Return an error if the database is already closed.
	if db.storage == nil {
		return errors.New("database already closed")
	}

	// Lock the database for exclusive write access before closing it.
	db.mu.Lock()
	defer db.mu.Unlock()
	delete(dbConnections.instances, filePath)
	db.storage = nil // Mark the storage as closed
	return nil
}
