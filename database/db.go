package db

import "sync"

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

func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return nil
}
