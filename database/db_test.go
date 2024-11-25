package db

import (
	"os"
	"testing"
)

func TestDBOperations(t *testing.T) {
	// Create a temporary file path for testing
	testFilePath := "./db/test2.db"
	defer os.Remove(testFilePath)

	// Test Open
	db, err := Open(testFilePath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close(testFilePath)

	// Test Put
	testKey := "testKey"
	testValue := "testValue"
	err = db.Put(testKey, testValue)
	if err != nil {
		t.Errorf("Failed to put key-value pair: %v", err)
	}

	// Test Get
	retrievedValue, exists, err := db.Get(testKey)
	if err != nil {
		t.Errorf("Failed to get value: %v", err)
	}
	if !exists {
		t.Errorf("Key %s does not exist after put", testKey)
	}
	if retrievedValue != testValue {
		t.Errorf("Retrieved value %s does not match original value %s", retrievedValue, testValue)
	}

	// Test Delete
	err = db.Del(testKey)
	if err != nil {
		t.Errorf("Failed to delete key: %v", err)
	}

	// Verify deletion
	_, exists, err = db.Get(testKey)
	if err != nil {
		t.Errorf("Error checking deleted key: %v", err)
	}
	if exists {
		t.Errorf("Key %s still exists after deletion", testKey)
	}
}

func TestPutValidation(t *testing.T) {
	db, err := Open("/tmp/validationdb")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.Remove("/tmp/validationdb")
	defer db.Close("/tmp/validationdb")

	// Test empty key
	err = db.Put("", "value")
	if err == nil {
		t.Errorf("Put with empty key should fail")
	}

	// Test empty value
	err = db.Put("key", "")
	if err == nil {
		t.Errorf("Put with empty value should fail")
	}
}

func TestConcurrency(t *testing.T) {
	db, err := Open("/tmp/concurrencydb")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer os.Remove("/tmp/concurrencydb")
	defer db.Close("/tmp/concurrencydb")

	// Concurrent writes
	done := make(chan bool)
	for i := 0; i < 100; i++ {
		go func(id int) {
			key := "key" + string(rune(id))
			value := "value" + string(rune(id))
			err := db.Put(key, value)
			if err != nil {
				t.Errorf("Concurrent put failed: %v", err)
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < 100; i++ {
		<-done
	}
}
