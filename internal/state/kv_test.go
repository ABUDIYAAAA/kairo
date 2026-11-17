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
