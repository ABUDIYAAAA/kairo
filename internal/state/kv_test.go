package state

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
)

func TestKVStoreBasicPutGet(t *testing.T) {
	kv := NewKVStore()

	// Initial Get on non-existent key
	_, err := kv.Get("foo")
	if err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}

	// First Put: version should be 1, revision should be 1
	item1 := kv.Put("foo", "bar")
	if item1.Value != "bar" || item1.Version != 1 || item1.ModRevision != 1 {
		t.Fatalf("unexpected item1: %+v", item1)
	}

	// Verify Get
	get1, err := kv.Get("foo")
	if err != nil || get1 != item1 {
		t.Fatalf("expected %+v, got %+v, err: %v", item1, get1, err)
	}

	// Second Put on same key: version should increment to 2, revision to 2
	item2 := kv.Put("foo", "baz")
	if item2.Value != "baz" || item2.Version != 2 || item2.ModRevision != 2 {
		t.Fatalf("unexpected item2: %+v", item2)
	}

	// Put on a different key: version should be 1, revision should be 3
	item3 := kv.Put("alpha", "beta")
	if item3.Value != "beta" || item3.Version != 1 || item3.ModRevision != 3 {
		t.Fatalf("unexpected item3: %+v", item3)
	}

	if kv.Revision() != 3 {
		t.Fatalf("expected global revision 3, got %d", kv.Revision())
	}
}

func TestKVStoreClientDeduplication(t *testing.T) {
	kv := NewKVStore()

	clientID := "client-uuid-1"
	var seqNum uint64 = 100

	// First execution of client command
	res1 := kv.PutWithClient(clientID, seqNum, "counter", "1")
	if res1.Value != "1" || res1.Version != 1 || res1.ModRevision != 1 {
		t.Fatalf("unexpected res1: %+v", res1)
	}

	// Duplicate execution (client retries due to network timeout or leader failover)
	res2 := kv.PutWithClient(clientID, seqNum, "counter", "1")
	if res2.Value != "1" || res2.Version != 1 || res2.ModRevision != 1 {
		t.Fatalf("linearizability violation: duplicate request mutated state! res2: %+v", res2)
	}

	// Verify revision did NOT advance on duplicate execution
	if kv.Revision() != 1 {
		t.Fatalf("expected revision to remain 1, got %d", kv.Revision())
	}

	// New sequence number should execute normally
	res3 := kv.PutWithClient(clientID, seqNum+1, "counter", "2")
	if res3.Value != "2" || res3.Version != 2 || res3.ModRevision != 2 {
		t.Fatalf("unexpected res3: %+v", res3)
	}
}

func TestKVStoreApplyCommand(t *testing.T) {
	kv := NewKVStore()

	// 1. Apply PUT command
	putCmd, _ := json.Marshal(Command{
		Op:          OpPut,
		Key:         "user:100",
		Value:       "Alice",
		ClientID:    "c1",
		SequenceNum: 1,
	})

	item, err := kv.Apply(putCmd)
	if err != nil || item.Value != "Alice" || item.Version != 1 {
		t.Fatalf("failed to apply PUT: item=%+v, err=%v", item, err)
	}

	// 2. Apply GET command
	getCmd, _ := json.Marshal(Command{
		Op:  OpGet,
		Key: "user:100",
	})

	getItem, err := kv.Apply(getCmd)
	if err != nil || getItem.Value != "Alice" || getItem.Version != 1 {
		t.Fatalf("failed to apply GET: item=%+v, err=%v", getItem, err)
	}
}

func TestKVStoreConcurrentAccess(t *testing.T) {
	kv := NewKVStore()
	var wg sync.WaitGroup

	numGoroutines := 50
	opsPerGoroutine := 100

	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", id%5)
			for j := 0; j < opsPerGoroutine; j++ {
				val := fmt.Sprintf("val-%d-%d", id, j)
				kv.Put(key, val)
				_, _ = kv.Get(key)
			}
		}(i)
	}

	wg.Wait()
}

func TestKVStoreDelete(t *testing.T) {
	kv := NewKVStore()

	// Delete non-existent key
	_, err := kv.Delete("missing")
	if err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}

	kv.Put("key1", "val1")
	delItem, err := kv.Delete("key1")
	if err != nil || delItem.Value != "val1" || delItem.Version != 2 {
		t.Fatalf("unexpected delItem: %+v, err: %v", delItem, err)
	}

	// Verify key is gone
	_, err = kv.Get("key1")
	if err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound after delete, got %v", err)
	}

	// Test DeleteWithClient deduplication
	kv.Put("key2", "val2")
	del1, err := kv.DeleteWithClient("clientA", 1, "key2")
	if err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	// Duplicate delete request from same client
	del2, err := kv.DeleteWithClient("clientA", 1, "key2")
	if err != nil || del2 != del1 {
		t.Fatalf("expected cached delete result %+v, got %+v, err: %v", del1, del2, err)
	}
}

func TestKVStoreSnapshotRestore(t *testing.T) {
	kv1 := NewKVStore()

	kv1.PutWithClient("c1", 1, "k1", "v1")
	kv1.PutWithClient("c1", 2, "k1", "v2")
	kv1.PutWithClient("c2", 1, "k2", "v3")

	snapBytes, err := kv1.Snapshot()
	if err != nil {
		t.Fatalf("failed to create snapshot: %v", err)
	}

	// Restore into a brand new KVStore
	kv2 := NewKVStore()
	if err := kv2.Restore(snapBytes); err != nil {
		t.Fatalf("failed to restore snapshot: %v", err)
	}

	if kv2.Revision() != kv1.Revision() {
		t.Fatalf("expected revision %d, got %d", kv1.Revision(), kv2.Revision())
	}

	item1, err := kv2.Get("k1")
	if err != nil || item1.Value != "v2" || item1.Version != 2 {
		t.Fatalf("unexpected item1 after restore: %+v, err: %v", item1, err)
	}

	item2, err := kv2.Get("k2")
	if err != nil || item2.Value != "v3" || item2.Version != 1 {
		t.Fatalf("unexpected item2 after restore: %+v, err: %v", item2, err)
	}

	// Deduplication table should also be restored
	dup := kv2.PutWithClient("c1", 2, "k1", "v2")
	if dup.Version != 2 || kv2.Revision() != kv1.Revision() {
		t.Fatalf("restored deduplication table failed to catch duplicate: %+v", dup)
	}
}

func TestKVStoreCAS(t *testing.T) {
	kv := NewKVStore()

	// 1. Create with expected_version = 0 (should succeed if key doesn't exist)
	var ver0 uint64 = 0
	item1, err := kv.PutWithClientAndVersion("c1", 1, "test-cas", "val1", &ver0)
	if err != nil {
		t.Fatalf("expected success for ver0 create, got %v", err)
	}
	if item1.Version != 1 {
		t.Fatalf("expected version 1, got %d", item1.Version)
	}

	// 2. Create again with expected_version = 0 (should fail with ErrVersionMismatch)
	_, err = kv.PutWithClientAndVersion("c2", 1, "test-cas", "val2", &ver0)
	if err != ErrVersionMismatch {
		t.Fatalf("expected ErrVersionMismatch, got %v", err)
	}

	// 3. Mutate with wrong version (expected 2, but current is 1)
	var ver2 uint64 = 2
	_, err = kv.PutWithClientAndVersion("c2", 2, "test-cas", "val2", &ver2)
	if err != ErrVersionMismatch {
		t.Fatalf("expected ErrVersionMismatch, got %v", err)
	}

	// 4. Mutate with correct version (expected 1, current is 1)
	var ver1 uint64 = 1
	item2, err := kv.PutWithClientAndVersion("c2", 3, "test-cas", "val2", &ver1)
	if err != nil {
		t.Fatalf("expected success for ver1 mutate, got %v", err)
	}
	if item2.Version != 2 || item2.Value != "val2" {
		t.Fatalf("expected version 2, got %+v", item2)
	}

	// 5. Test via Apply()
	cmdBytes, _ := json.Marshal(Command{
		Op:              OpPut,
		Key:             "test-cas",
		Value:           "val3",
		ClientID:        "c3",
		SequenceNum:     1,
		ExpectedVersion: &ver1, // Stale! Current is 2
	})
	_, err = kv.Apply(cmdBytes)
	if err != ErrVersionMismatch {
		t.Fatalf("expected ErrVersionMismatch via Apply, got %v", err)
	}
}
