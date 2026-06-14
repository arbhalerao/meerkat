package db

import (
	"os"
	"testing"
)

func setupTestDB(t *testing.T) *Database {
	t.Helper()
	dir, err := os.MkdirTemp("", "meerkat-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	db, err := NewDatabase(dir)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("failed to create database: %v", err)
	}

	t.Cleanup(func() {
		_ = db.Cleanup()
	})

	return db
}

func TestNewDatabase(t *testing.T) {
	db := setupTestDB(t)
	if db == nil {
		t.Fatal("expected non-nil database")
	}
}

func TestNewDatabase_InvalidPath(t *testing.T) {
	_, err := NewDatabase("/nonexistent/path/that/should/fail")
	if err == nil {
		t.Fatal("expected error for invalid path")
	}
}

func TestSetAndGetKey(t *testing.T) {
	db := setupTestDB(t)

	err := db.SetKey("user:1", "Alice")
	if err != nil {
		t.Fatalf("SetKey failed: %v", err)
	}

	val, err := db.GetKey("user:1")
	if err != nil {
		t.Fatalf("GetKey failed: %v", err)
	}
	if string(val) != "Alice" {
		t.Fatalf("expected 'Alice', got '%s'", string(val))
	}
}

func TestGetKey_NotFound(t *testing.T) {
	db := setupTestDB(t)

	_, err := db.GetKey("nonexistent")
	if err == nil {
		t.Fatal("expected error for non-existent key")
	}
}

func TestSetKey_Overwrite(t *testing.T) {
	db := setupTestDB(t)

	_ = db.SetKey("key", "value1")
	_ = db.SetKey("key", "value2")

	val, err := db.GetKey("key")
	if err != nil {
		t.Fatalf("GetKey failed: %v", err)
	}
	if string(val) != "value2" {
		t.Fatalf("expected 'value2', got '%s'", string(val))
	}
}

func TestDeleteKey(t *testing.T) {
	db := setupTestDB(t)

	_ = db.SetKey("key", "value")

	err := db.DeleteKey("key")
	if err != nil {
		t.Fatalf("DeleteKey failed: %v", err)
	}

	_, err = db.GetKey("key")
	if err == nil {
		t.Fatal("expected error after deleting key")
	}
}

func TestDeleteKey_NotFound(t *testing.T) {
	db := setupTestDB(t)

	err := db.DeleteKey("nonexistent")
	if err == nil {
		t.Fatal("expected error when deleting non-existent key")
	}
}

func TestIsHealthy(t *testing.T) {
	db := setupTestDB(t)

	if !db.IsHealthy() {
		t.Fatal("expected healthy database")
	}

	db.Close()
	if db.IsHealthy() {
		t.Fatal("expected unhealthy database after close")
	}
}

func TestCleanup(t *testing.T) {
	dir, _ := os.MkdirTemp("", "meerkat-cleanup-*")
	db, _ := NewDatabase(dir)

	err := db.Cleanup()
	if err != nil {
		t.Fatalf("Cleanup failed: %v", err)
	}

	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatal("expected directory to be removed after cleanup")
	}
}

func TestMultipleKeys(t *testing.T) {
	db := setupTestDB(t)

	keys := map[string]string{
		"user:1":    "Alice",
		"user:2":    "Bob",
		"user:3":    "Charlie",
		"config:db": "postgres",
		"session:x": "token123",
	}

	for k, v := range keys {
		if err := db.SetKey(k, v); err != nil {
			t.Fatalf("SetKey(%s) failed: %v", k, err)
		}
	}

	for k, expected := range keys {
		val, err := db.GetKey(k)
		if err != nil {
			t.Fatalf("GetKey(%s) failed: %v", k, err)
		}
		if string(val) != expected {
			t.Fatalf("GetKey(%s): expected %q, got %q", k, expected, string(val))
		}
	}
}

func TestEmptyKeyAndValue(t *testing.T) {
	db := setupTestDB(t)

	err := db.SetKey("empty-val", "")
	if err != nil {
		t.Fatalf("SetKey with empty value failed: %v", err)
	}

	val, err := db.GetKey("empty-val")
	if err != nil {
		t.Fatalf("GetKey failed: %v", err)
	}
	if string(val) != "" {
		t.Fatalf("expected empty value, got %q", string(val))
	}
}

func TestGetAllKeys_Empty(t *testing.T) {
	db := setupTestDB(t)

	pairs, err := db.GetAllKeys()
	if err != nil {
		t.Fatalf("GetAllKeys failed: %v", err)
	}
	if len(pairs) != 0 {
		t.Fatalf("expected 0 pairs, got %d", len(pairs))
	}
}

func TestGetAllKeys(t *testing.T) {
	db := setupTestDB(t)

	expected := map[string]string{
		"key-1": "value-1",
		"key-2": "value-2",
		"key-3": "value-3",
	}

	for k, v := range expected {
		_ = db.SetKey(k, v)
	}

	pairs, err := db.GetAllKeys()
	if err != nil {
		t.Fatalf("GetAllKeys failed: %v", err)
	}
	if len(pairs) != len(expected) {
		t.Fatalf("expected %d pairs, got %d", len(expected), len(pairs))
	}

	for _, p := range pairs {
		expectedVal, ok := expected[p.Key]
		if !ok {
			t.Fatalf("unexpected key %q in GetAllKeys result", p.Key)
		}
		if p.Value != expectedVal {
			t.Fatalf("GetAllKeys: key %q: expected %q, got %q", p.Key, expectedVal, p.Value)
		}
	}
}

func TestGetAllKeys_AfterDelete(t *testing.T) {
	db := setupTestDB(t)

	_ = db.SetKey("a", "1")
	_ = db.SetKey("b", "2")
	_ = db.SetKey("c", "3")
	_ = db.DeleteKey("b")

	pairs, err := db.GetAllKeys()
	if err != nil {
		t.Fatalf("GetAllKeys failed: %v", err)
	}
	if len(pairs) != 2 {
		t.Fatalf("expected 2 pairs after delete, got %d", len(pairs))
	}

	for _, p := range pairs {
		if p.Key == "b" {
			t.Fatal("deleted key 'b' should not appear in GetAllKeys")
		}
	}
}

func TestLargeValue(t *testing.T) {
	db := setupTestDB(t)

	largeVal := make([]byte, 1024*1024)
	for i := range largeVal {
		largeVal[i] = byte(i % 256)
	}

	err := db.SetKey("large", string(largeVal))
	if err != nil {
		t.Fatalf("SetKey with large value failed: %v", err)
	}

	val, err := db.GetKey("large")
	if err != nil {
		t.Fatalf("GetKey failed: %v", err)
	}
	if len(val) != len(largeVal) {
		t.Fatalf("expected %d bytes, got %d", len(largeVal), len(val))
	}
}
