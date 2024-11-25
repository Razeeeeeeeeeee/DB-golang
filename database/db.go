package db

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// DB - Handle exported by the package
type DB struct {
	storage *btree
	mu      sync.RWMutex
}

var dbConnections = struct {
	mu        sync.Mutex
	instances map[string]*DB
}{
	instances: make(map[string]*DB),
}

// Open - Opens a new db connection at the file path
func Open(filePath string) (*DB, error) {
	dbConnections.mu.Lock()
	defer dbConnections.mu.Unlock()

	dir := filepath.Dir(filePath)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		// Create the directory if it doesn't exist
		err := os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			fmt.Printf("Error creating directory: %v\n", err)
		}
		fmt.Printf("Directory created: %s\n", dir)
	}
	// Return existing connection if it exists
	if db, exists := dbConnections.instances[filePath]; exists {
		return db, nil
	}

	//create a new connection
	storage, err := initializeBtree(filePath)
	if err != nil {
		return nil, err
	}
	db := &DB{
		storage: storage,
		mu:      sync.RWMutex{},
	}
	dbConnections.instances[filePath] = db
	return db, nil
}

// Put - Insert a key value pair in the database
func (db *DB) Put(key string, value string) error {
	pair := newPair(key, value)
	if err := pair.validate(); err != nil {
		return err
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.storage.insert(pair)
}

// Get - Get the stored value from the database for the respective key
func (db *DB) Get(key string) (string, bool, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.storage.get(key)
}

func (db *DB) Del(key string) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.storage.del(key)
}

func (db *DB) Close(filePath string) error {
	dbConnections.mu.Lock()
	defer dbConnections.mu.Unlock()

	if db.storage == nil {
		return errors.New("database already closed")
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	delete(dbConnections.instances, filePath)
	db.storage = nil // Mark the storage as closed
	return nil
}
