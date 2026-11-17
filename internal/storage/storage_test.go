package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestFileStorageAppendAndRead(t *testing.T) {
	dir := t.TempDir()

	fs, err := OpenFileStorage(dir)
	if err != nil {
		t.Fatalf("failed to open file storage: %v", err)
	}
	defer fs.Close()

	// Initial state check
	lastIdx, lastTerm, err := fs.LastLog()
	if err != nil || lastIdx != 0 || lastTerm != 0 {
		t.Fatalf("expected last log (0, 0), got (%d, %d, %v)", lastIdx, lastTerm, err)
	}

	term0, err := fs.Term(0)
	if err != nil || term0 != 0 {
		t.Fatalf("expected term 0 for index 0, got %d, err %v", term0, err)
	}

	entries := []LogEntry{
		{Index: 1, Term: 1, Data: []byte("set x 10")},
		{Index: 2, Term: 1, Data: []byte("set y 20")},
		{Index: 3, Term: 2, Data: []byte("set z 30")},
	}

	if err := fs.Append(entries...); err != nil {
		t.Fatalf("failed to append entries: %v", err)
	}

	if fs.LastIndex() != 3 {
		t.Fatalf("expected last index 3, got %d", fs.LastIndex())
	}

	lastIdx, lastTerm, err = fs.LastLog()
	if err != nil || lastIdx != 3 || lastTerm != 2 {
		t.Fatalf("expected (3, 2), got (%d, %d, %v)", lastIdx, lastTerm, err)
	}

	e2, err := fs.GetEntry(2)
	if err != nil {
		t.Fatalf("failed to get entry 2: %v", err)
	}
	if e2.Index != 2 || e2.Term != 1 || string(e2.Data) != "set y 20" {
		t.Fatalf("unexpected entry 2: %+v", e2)
	}

	slice, err := fs.GetEntries(1, 3)
	if err != nil {
		t.Fatalf("failed to get entries range: %v", err)
	}
	if len(slice) != 2 || slice[0].Index != 1 || slice[1].Index != 2 {
		t.Fatalf("unexpected slice: %+v", slice)
	}
}

func TestFileStorageTruncate(t *testing.T) {
	dir := t.TempDir()

	fs, err := OpenFileStorage(dir)
	if err != nil {
		t.Fatalf("failed to open file storage: %v", err)
	}
	defer fs.Close()

	entries := []LogEntry{
		{Index: 1, Term: 1, Data: []byte("cmd1")},
		{Index: 2, Term: 1, Data: []byte("cmd2")},
		{Index: 3, Term: 1, Data: []byte("cmd3")},
		{Index: 4, Term: 1, Data: []byte("cmd4")},
	}
	if err := fs.Append(entries...); err != nil {
		t.Fatalf("failed to append: %v", err)
	}

	// Truncate from index 3 onwards
	if err := fs.Truncate(3); err != nil {
		t.Fatalf("failed to truncate: %v", err)
	}

	if fs.LastIndex() != 2 {
		t.Fatalf("expected last index 2, got %d", fs.LastIndex())
	}

	if _, err := fs.GetEntry(3); err != ErrEntryNotFound {
		t.Fatalf("expected ErrEntryNotFound for index 3, got %v", err)
	}

	// Overwrite conflict test (Append with conflicting index <= lastIndex)
	newEntries := []LogEntry{
		{Index: 2, Term: 2, Data: []byte("cmd2_new")},
		{Index: 3, Term: 2, Data: []byte("cmd3_new")},
	}
	if err := fs.Append(newEntries...); err != nil {
		t.Fatalf("failed to append conflicting entries: %v", err)
	}

	if fs.LastIndex() != 3 {
		t.Fatalf("expected last index 3, got %d", fs.LastIndex())
	}

	e2, err := fs.GetEntry(2)
	if err != nil || e2.Term != 2 || string(e2.Data) != "cmd2_new" {
		t.Fatalf("unexpected entry 2 after conflict resolution: %+v", e2)
	}
}

func TestFileStorageCrashRecovery(t *testing.T) {
	dir := t.TempDir()

	fs1, err := OpenFileStorage(dir)
	if err != nil {
		t.Fatalf("failed to open file storage: %v", err)
	}

	if err := fs1.SaveState(5, 2); err != nil {
		t.Fatalf("failed to save state: %v", err)
	}

	entries := []LogEntry{
		{Index: 1, Term: 1, Data: []byte("op1")},
		{Index: 2, Term: 3, Data: []byte("op2")},
		{Index: 3, Term: 5, Data: []byte("op3")},
	}
	if err := fs1.Append(entries...); err != nil {
		t.Fatalf("failed to append: %v", err)
	}

	if err := fs1.Close(); err != nil {
		t.Fatalf("failed to close: %v", err)
	}

	// Re-open from disk (simulating restart after crash/shutdown)
	fs2, err := OpenFileStorage(dir)
	if err != nil {
		t.Fatalf("failed to reopen file storage: %v", err)
	}
	defer fs2.Close()

	term, votedFor, err := fs2.GetState()
	if err != nil || term != 5 || votedFor != 2 {
		t.Fatalf("expected term 5, votedFor 2, got term=%d, votedFor=%d, err=%v", term, votedFor, err)
	}

	if fs2.LastIndex() != 3 {
		t.Fatalf("expected last index 3, got %d", fs2.LastIndex())
	}

	for _, expected := range entries {
		actual, err := fs2.GetEntry(expected.Index)
		if err != nil {
			t.Fatalf("failed to get entry %d: %v", expected.Index, err)
		}
		if actual.Index != expected.Index || actual.Term != expected.Term || !bytes.Equal(actual.Data, expected.Data) {
			t.Fatalf("mismatch for entry %d: expected %+v, got %+v", expected.Index, expected, actual)
		}
	}
}

func TestFileStorageTornWriteRecovery(t *testing.T) {
	dir := t.TempDir()

	fs, err := OpenFileStorage(dir)
	if err != nil {
		t.Fatalf("failed to open file storage: %v", err)
	}

	entries := []LogEntry{
		{Index: 1, Term: 1, Data: []byte("op1")},
		{Index: 2, Term: 1, Data: []byte("op2")},
	}
	if err := fs.Append(entries...); err != nil {
		t.Fatalf("failed to append: %v", err)
	}
	_ = fs.Close()

	// Corrupt / append garbage bytes at the end of raft.log simulating a power cut mid-write
	logPath := filepath.Join(dir, logFileName)
	f, err := os.OpenFile(logPath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("failed to open log for corrupting: %v", err)
	}
	// Append 10 bytes of truncated garbage
	if _, err := f.Write([]byte{0, 0, 0, 50, 1, 2, 3, 4}); err != nil {
		t.Fatalf("failed to write garbage: %v", err)
	}
	_ = f.Close()

	// Reopening must recover cleanly by truncating the torn write
	recoveredFS, err := OpenFileStorage(dir)
	if err != nil {
		t.Fatalf("expected successful recovery despite torn write, got: %v", err)
	}
	defer recoveredFS.Close()

	if recoveredFS.LastIndex() != 2 {
		t.Fatalf("expected last index to remain 2, got %d", recoveredFS.LastIndex())
	}

	// Should be able to append new entries normally after recovery
	newEntry := LogEntry{Index: 3, Term: 2, Data: []byte("op3")}
	if err := recoveredFS.Append(newEntry); err != nil {
		t.Fatalf("failed to append after recovery: %v", err)
	}

	if recoveredFS.LastIndex() != 3 {
		t.Fatalf("expected last index 3, got %d", recoveredFS.LastIndex())
	}
}

func TestFileStorageSnapshotAndCompaction(t *testing.T) {
	dir := t.TempDir()

	fs, err := OpenFileStorage(dir)
	if err != nil {
		t.Fatalf("failed to open file storage: %v", err)
	}

	entries := []LogEntry{
		{Index: 1, Term: 1, Data: []byte("cmd1")},
		{Index: 2, Term: 1, Data: []byte("cmd2")},
		{Index: 3, Term: 2, Data: []byte("cmd3")},
		{Index: 4, Term: 2, Data: []byte("cmd4")},
		{Index: 5, Term: 3, Data: []byte("cmd5")},
	}
	if err := fs.Append(entries...); err != nil {
		t.Fatalf("failed to append: %v", err)
	}

	// Save snapshot at index 3, term 2
	meta := SnapshotMeta{LastIncludedIndex: 3, LastIncludedTerm: 2}
	snapData := []byte(`{"k1":"v1"}`)
	if err := fs.SaveSnapshot(meta, snapData); err != nil {
		t.Fatalf("failed to save snapshot: %v", err)
	}

	if fs.FirstIndex() != 4 {
		t.Fatalf("expected first index 4 after compaction, got %d", fs.FirstIndex())
	}
	if fs.LastIndex() != 5 {
		t.Fatalf("expected last index 5, got %d", fs.LastIndex())
	}

	// Indices 1, 2 should be gone
	if _, err := fs.GetEntry(2); err != ErrEntryNotFound {
		t.Fatalf("expected ErrEntryNotFound for compacted index 2, got %v", err)
	}

	// Index 3 term lookup should return baseline term
	term3, err := fs.Term(3)
	if err != nil || term3 != 2 {
		t.Fatalf("expected term 2 for baseline index 3, got %d, err %v", term3, err)
	}

	// Indices 4 and 5 should still be present
	e4, err := fs.GetEntry(4)
	if err != nil || string(e4.Data) != "cmd4" {
		t.Fatalf("unexpected entry 4: %+v, err %v", e4, err)
	}

	// Close and reopen from disk (recovery of snapshot + compacted log)
	_ = fs.Close()

	recoveredFS, err := OpenFileStorage(dir)
	if err != nil {
		t.Fatalf("failed to reopen compacted storage: %v", err)
	}
	defer recoveredFS.Close()

	if recoveredFS.FirstIndex() != 4 || recoveredFS.LastIndex() != 5 {
		t.Fatalf("unexpected bounds after recovery: first=%d, last=%d", recoveredFS.FirstIndex(), recoveredFS.LastIndex())
	}

	recoveredMeta, recoveredData, err := recoveredFS.GetSnapshot()
	if err != nil || recoveredMeta == nil || recoveredMeta.LastIncludedIndex != 3 || string(recoveredData) != `{"k1":"v1"}` {
		t.Fatalf("unexpected recovered snapshot: meta=%+v, data=%s, err=%v", recoveredMeta, string(recoveredData), err)
	}
}

func TestMemoryStorage(t *testing.T) {
	ms := NewMemoryStorage()

	if err := ms.SaveState(2, 1); err != nil {
		t.Fatalf("failed to save state: %v", err)
	}

	term, votedFor, _ := ms.GetState()
	if term != 2 || votedFor != 1 {
		t.Fatalf("expected term 2, votedFor 1, got %d, %d", term, votedFor)
	}

	entries := []LogEntry{
		{Index: 1, Term: 1, Data: []byte("a")},
		{Index: 2, Term: 2, Data: []byte("b")},
	}
	if err := ms.Append(entries...); err != nil {
		t.Fatalf("failed to append: %v", err)
	}

	if ms.LastIndex() != 2 {
		t.Fatalf("expected last index 2, got %d", ms.LastIndex())
	}

	e, err := ms.GetEntry(1)
	if err != nil || string(e.Data) != "a" {
		t.Fatalf("unexpected entry: %+v, err: %v", e, err)
	}
}
