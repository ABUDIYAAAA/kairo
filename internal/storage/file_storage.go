package storage

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const (
	logFileName          = "raft.log"
	stateFileName        = "state.json"
	snapshotMetaFileName = "snapshot.meta"
	snapshotDataFileName = "snapshot.dat"
)

// FileStorage is a disk-backed, crash-recoverable, synchronous implementation of Storage.
// All writes to the log, snapshot, and state files are immediately flushed to physical disk via fsync
// before returning, ensuring strict adherence to the Raft persistence invariants.
type FileStorage struct {
	mu sync.RWMutex

	dir     string
	logFile *os.File

	// entries stores in-memory cached entries for O(1) reads.
	// entries[0] is the sentinel / snapshot baseline entry (Index: baseIndex, Term: baseTerm).
	entries []LogEntry

	// offsets tracks the byte offset in logFile where each entry begins.
	offsets []int64

	// state tracks persistent Raft state (currentTerm, votedFor).
	state HardState

	// snapshot metadata
	snapshotMeta *SnapshotMeta
}

// OpenFileStorage creates or opens a FileStorage in the specified directory.
func OpenFileStorage(dir string) (*FileStorage, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create storage directory %s: %w", dir, err)
	}

	fs := &FileStorage{
		dir:     dir,
		entries: []LogEntry{{Index: 0, Term: 0, Data: nil}},
		offsets: []int64{0},
		state:   HardState{CurrentTerm: 0, VotedFor: NoVote},
	}

	// 1. Recover HardState (currentTerm, votedFor)
	if err := fs.recoverState(); err != nil {
		return nil, fmt.Errorf("failed to recover state: %w", err)
	}

	// 2. Recover Snapshot metadata if present
	if err := fs.recoverSnapshotMeta(); err != nil {
		return nil, fmt.Errorf("failed to recover snapshot meta: %w", err)
	}

	// 3. Open log file and recover entries
	logPath := filepath.Join(dir, logFileName)
	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file %s: %w", logPath, err)
	}
	fs.logFile = f

	if err := fs.recoverLog(); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("failed to recover log: %w", err)
	}

	return fs, nil
}

func (fs *FileStorage) recoverState() error {
	statePath := filepath.Join(fs.dir, stateFileName)
	data, err := os.ReadFile(statePath)
	if os.IsNotExist(err) {
		return fs.persistState(0, NoVote)
	}
	if err != nil {
		return err
	}

	var state HardState
	if err := json.Unmarshal(data, &state); err != nil {
		return fmt.Errorf("failed to unmarshal state: %w", err)
	}
	fs.state = state
	return nil
}

func (fs *FileStorage) recoverSnapshotMeta() error {
	metaPath := filepath.Join(fs.dir, snapshotMetaFileName)
	data, err := os.ReadFile(metaPath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}

	var meta SnapshotMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return fmt.Errorf("failed to unmarshal snapshot metadata: %w", err)
	}

	fs.snapshotMeta = &meta
	// Sentinel at entries[0] becomes the snapshot baseline
	fs.entries[0] = LogEntry{
		Index: meta.LastIncludedIndex,
		Term:  meta.LastIncludedTerm,
		Data:  nil,
	}
	return nil
}

func (fs *FileStorage) recoverLog() error {
	if _, err := fs.logFile.Seek(0, io.SeekStart); err != nil {
		return err
	}

	var validOffset int64 = 0
	lenBuf := make([]byte, 4)

	for {
		_, err := io.ReadFull(fs.logFile, lenBuf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fs.truncateFileTo(validOffset)
		}

		totalLen := binary.BigEndian.Uint32(lenBuf)
		if totalLen < 24 {
			return fs.truncateFileTo(validOffset)
		}

		payload := make([]byte, totalLen)
		_, err = io.ReadFull(fs.logFile, payload)
		if err != nil {
			return fs.truncateFileTo(validOffset)
		}

		storedCRC := binary.BigEndian.Uint32(payload[0:4])
		computedCRC := crc32.ChecksumIEEE(payload[4:])
		if storedCRC != computedCRC {
			return fs.truncateFileTo(validOffset)
		}

		index := binary.BigEndian.Uint64(payload[4:12])
		term := binary.BigEndian.Uint64(payload[12:20])
		dataLen := binary.BigEndian.Uint32(payload[20:24])

		if uint32(len(payload[24:])) != dataLen {
			return fs.truncateFileTo(validOffset)
		}

		expectedIndex := fs.entries[len(fs.entries)-1].Index + 1
		if index != expectedIndex {
			return fs.truncateFileTo(validOffset)
		}

		data := make([]byte, dataLen)
		copy(data, payload[24:24+dataLen])

		fs.offsets = append(fs.offsets, validOffset)
		fs.entries = append(fs.entries, LogEntry{
			Index: index,
			Term:  term,
			Data:  data,
		})

		validOffset += 4 + int64(totalLen)
	}

	if _, err := fs.logFile.Seek(validOffset, io.SeekStart); err != nil {
		return err
	}
	return nil
}

func (fs *FileStorage) truncateFileTo(offset int64) error {
	if err := fs.logFile.Truncate(offset); err != nil {
		return err
	}
	if _, err := fs.logFile.Seek(offset, io.SeekStart); err != nil {
		return err
	}
	return fs.logFile.Sync()
}

// Append synchronously writes one or more log entries to disk.
func (fs *FileStorage) Append(entries ...LogEntry) error {
	if len(entries) == 0 {
		return nil
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	baseIndex := fs.entries[0].Index

	for _, entry := range entries {
		lastIdx := fs.entries[len(fs.entries)-1].Index

		if entry.Index <= lastIdx {
			if entry.Index <= baseIndex {
				return fmt.Errorf("%w: cannot overwrite compacted index %d", ErrIndexOutOfBounds, entry.Index)
			}
			if err := fs.truncateLocked(entry.Index); err != nil {
				return err
			}
			lastIdx = fs.entries[len(fs.entries)-1].Index
		}

		if entry.Index != lastIdx+1 {
			return fmt.Errorf("%w: expected index %d, got %d", ErrIndexGap, lastIdx+1, entry.Index)
		}

		recordBytes := encodeEntry(entry)
		currentOffset, err := fs.logFile.Seek(0, io.SeekCurrent)
		if err != nil {
			return fmt.Errorf("failed to get current file offset: %w", err)
		}

		if _, err := fs.logFile.Write(recordBytes); err != nil {
			return fmt.Errorf("failed to write log entry %d: %w", entry.Index, err)
		}

		fs.offsets = append(fs.offsets, currentOffset)
		fs.entries = append(fs.entries, entry)
	}

	return fs.logFile.Sync()
}

// Truncate deletes all log entries from fromIndex onwards (inclusive) synchronously.
func (fs *FileStorage) Truncate(fromIndex uint64) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.truncateLocked(fromIndex)
}

func (fs *FileStorage) truncateLocked(fromIndex uint64) error {
	baseIndex := fs.entries[0].Index
	if fromIndex <= baseIndex {
		return fmt.Errorf("%w: cannot truncate at or before baseline index %d", ErrIndexOutOfBounds, baseIndex)
	}

	lastIdx := fs.entries[len(fs.entries)-1].Index
	if fromIndex > lastIdx {
		return nil
	}

	sliceIdx := fromIndex - baseIndex
	offset := fs.offsets[sliceIdx]
	if err := fs.logFile.Truncate(offset); err != nil {
		return fmt.Errorf("failed to truncate log file at offset %d: %w", offset, err)
	}
	if _, err := fs.logFile.Seek(offset, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek log file: %w", err)
	}
	if err := fs.logFile.Sync(); err != nil {
		return fmt.Errorf("failed to fsync log file after truncate: %w", err)
	}

	fs.entries = fs.entries[:sliceIdx]
	fs.offsets = fs.offsets[:sliceIdx]
	return nil
}

// GetEntry retrieves the log entry at the given 1-based index.
func (fs *FileStorage) GetEntry(index uint64) (*LogEntry, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	baseIndex := fs.entries[0].Index
	if index <= baseIndex || index >= baseIndex+uint64(len(fs.entries)) {
		return nil, ErrEntryNotFound
	}
	entry := fs.entries[index-baseIndex]
	return &entry, nil
}

// GetEntries returns entries in the half-open range [fromIndex, toIndex).
func (fs *FileStorage) GetEntries(fromIndex, toIndex uint64) ([]LogEntry, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	baseIndex := fs.entries[0].Index
	if fromIndex <= baseIndex || fromIndex > toIndex || toIndex > baseIndex+uint64(len(fs.entries)) {
		return nil, ErrIndexOutOfBounds
	}

	start := fromIndex - baseIndex
	end := toIndex - baseIndex
	res := make([]LogEntry, end-start)
	copy(res, fs.entries[start:end])
	return res, nil
}

// LastLog returns the index and term of the last entry in the log.
func (fs *FileStorage) LastLog() (uint64, uint64, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	last := fs.entries[len(fs.entries)-1]
	return last.Index, last.Term, nil
}

// Term returns the term of the entry at index.
func (fs *FileStorage) Term(index uint64) (uint64, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	baseIndex := fs.entries[0].Index
	if index < baseIndex || index >= baseIndex+uint64(len(fs.entries)) {
		return 0, ErrEntryNotFound
	}
	return fs.entries[index-baseIndex].Term, nil
}

// FirstIndex returns the first valid index available in the active log.
func (fs *FileStorage) FirstIndex() uint64 {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return fs.entries[0].Index + 1
}

// LastIndex returns the last valid index in the log.
func (fs *FileStorage) LastIndex() uint64 {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return fs.entries[len(fs.entries)-1].Index
}

// SaveState persists currentTerm and votedFor to stable storage synchronously.
func (fs *FileStorage) SaveState(currentTerm uint64, votedFor int) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if err := fs.persistState(currentTerm, votedFor); err != nil {
		return err
	}
	fs.state = HardState{CurrentTerm: currentTerm, VotedFor: votedFor}
	return nil
}

func (fs *FileStorage) persistState(currentTerm uint64, votedFor int) error {
	state := HardState{CurrentTerm: currentTerm, VotedFor: votedFor}
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}

	tmpPath := filepath.Join(fs.dir, stateFileName+".tmp")
	statePath := filepath.Join(fs.dir, stateFileName)

	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}

	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}

	if err := os.Rename(tmpPath, statePath); err != nil {
		return err
	}

	if dirFile, err := os.Open(fs.dir); err == nil {
		_ = dirFile.Sync()
		_ = dirFile.Close()
	}
	return nil
}

// GetState returns the current persisted term and votedFor.
func (fs *FileStorage) GetState() (uint64, int, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return fs.state.CurrentTerm, fs.state.VotedFor, nil
}

// SaveSnapshot persists snapshot metadata and data bytes to stable storage synchronously
// and triggers log compaction up to meta.LastIncludedIndex.
func (fs *FileStorage) SaveSnapshot(meta SnapshotMeta, data []byte) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// 1. Write snapshot metadata atomically
	metaBytes, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}
	tmpMeta := filepath.Join(fs.dir, snapshotMetaFileName+".tmp")
	finalMeta := filepath.Join(fs.dir, snapshotMetaFileName)
	if err := writeAndSync(tmpMeta, finalMeta, metaBytes); err != nil {
		return fmt.Errorf("failed to write snapshot meta: %w", err)
	}

	// 2. Write snapshot data atomically
	tmpData := filepath.Join(fs.dir, snapshotDataFileName+".tmp")
	finalData := filepath.Join(fs.dir, snapshotDataFileName)
	if err := writeAndSync(tmpData, finalData, data); err != nil {
		return fmt.Errorf("failed to write snapshot data: %w", err)
	}

	fs.snapshotMeta = &meta

	// 3. Compact log up to meta.LastIncludedIndex
	return fs.compactLocked(meta.LastIncludedIndex, meta.LastIncludedTerm)
}

func writeAndSync(tmpPath, finalPath string, data []byte) error {
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmpPath, finalPath)
}

// GetSnapshot returns the latest persisted snapshot metadata and data.
func (fs *FileStorage) GetSnapshot() (*SnapshotMeta, []byte, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	if fs.snapshotMeta == nil {
		return nil, nil, nil
	}

	dataPath := filepath.Join(fs.dir, snapshotDataFileName)
	data, err := os.ReadFile(dataPath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read snapshot data: %w", err)
	}

	metaCopy := *fs.snapshotMeta
	return &metaCopy, data, nil
}

// Compact discards all log entries up through compactIndex (inclusive).
func (fs *FileStorage) Compact(compactIndex uint64) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	baseIndex := fs.entries[0].Index
	if compactIndex <= baseIndex {
		return nil
	}
	lastIdx := fs.entries[len(fs.entries)-1].Index
	if compactIndex > lastIdx {
		return fmt.Errorf("%w: compact index %d exceeds last log index %d", ErrIndexOutOfBounds, compactIndex, lastIdx)
	}

	compactTerm := fs.entries[compactIndex-baseIndex].Term
	return fs.compactLocked(compactIndex, compactTerm)
}

func (fs *FileStorage) compactLocked(compactIndex, compactTerm uint64) error {
	baseIndex := fs.entries[0].Index
	if compactIndex <= baseIndex {
		return nil
	}

	lastIdx := fs.entries[len(fs.entries)-1].Index
	var retained []LogEntry
	if compactIndex < lastIdx {
		retained = fs.entries[compactIndex-baseIndex+1:]
	}

	// Create new compacted log file
	compactPath := filepath.Join(fs.dir, logFileName+".compact")
	f, err := os.OpenFile(compactPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to open compact log: %w", err)
	}

	newOffsets := []int64{0}
	var currentOffset int64 = 0

	for _, entry := range retained {
		recordBytes := encodeEntry(entry)
		if _, err := f.Write(recordBytes); err != nil {
			_ = f.Close()
			return err
		}
		newOffsets = append(newOffsets, currentOffset)
		currentOffset += int64(len(recordBytes))
	}

	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}

	// Close old log file and replace atomically
	if fs.logFile != nil {
		_ = fs.logFile.Close()
	}

	finalLogPath := filepath.Join(fs.dir, logFileName)
	if err := os.Rename(compactPath, finalLogPath); err != nil {
		return fmt.Errorf("failed to swap compacted log: %w", err)
	}

	// Reopen active log file
	reopened, err := os.OpenFile(finalLogPath, os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to reopen compacted log: %w", err)
	}
	fs.logFile = reopened

	// Update in-memory entries: entries[0] becomes the new baseline sentinel
	newEntries := make([]LogEntry, 1+len(retained))
	newEntries[0] = LogEntry{Index: compactIndex, Term: compactTerm}
	copy(newEntries[1:], retained)

	fs.entries = newEntries
	fs.offsets = newOffsets
	return nil
}

// Close closes the underlying log file handle.
func (fs *FileStorage) Close() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if fs.logFile != nil {
		_ = fs.logFile.Sync()
		err := fs.logFile.Close()
		fs.logFile = nil
		return err
	}
	return nil
}

func encodeEntry(e LogEntry) []byte {
	dataLen := len(e.Data)
	payloadLen := 4 + 8 + 8 + 4 + dataLen
	buf := make([]byte, 4+payloadLen)

	binary.BigEndian.PutUint32(buf[0:4], uint32(payloadLen))
	binary.BigEndian.PutUint64(buf[8:16], e.Index)
	binary.BigEndian.PutUint64(buf[16:24], e.Term)
	binary.BigEndian.PutUint32(buf[24:28], uint32(dataLen))
	copy(buf[28:], e.Data)

	checksum := crc32.ChecksumIEEE(buf[8:])
	binary.BigEndian.PutUint32(buf[4:8], checksum)
	return buf
}
