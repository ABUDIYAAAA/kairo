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
	logFileName   = "raft.log"
	stateFileName = "state.json"
)

// FileStorage is a disk-backed, crash-recoverable, synchronous implementation of Storage.
// All writes to the log and state files are immediately flushed to physical disk via fsync
// before returning, ensuring strict adherence to the Raft persistence invariants.
type FileStorage struct {
	mu sync.RWMutex

	dir     string
	logFile *os.File

	// entries stores in-memory cached entries for O(1) reads.
	// entries[0] is the sentinel entry (Index: 0, Term: 0) to align with 1-based indexing.
	entries []LogEntry

	// offsets tracks the byte offset in logFile where each entry begins.
	// offsets[0] is 0. For i >= 1, offsets[i] is the file position of entries[i].
	offsets []int64

	// state tracks the persistent Raft state (currentTerm, votedFor).
	state HardState
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

	// 2. Open log file and recover entries
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

// recoverState reads the persisted hard state from state.json if present.
func (fs *FileStorage) recoverState() error {
	statePath := filepath.Join(fs.dir, stateFileName)
	data, err := os.ReadFile(statePath)
	if os.IsNotExist(err) {
		// New node, initialize state
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

// recoverLog scans the log file from offset 0, validates CRC checksums,
// populates the in-memory log cache and offset index, and truncates torn writes.
func (fs *FileStorage) recoverLog() error {
	if _, err := fs.logFile.Seek(0, io.SeekStart); err != nil {
		return err
	}

	var validOffset int64 = 0
	lenBuf := make([]byte, 4)

	for {
		// Read record length (TotalLen)
		_, err := io.ReadFull(fs.logFile, lenBuf)
		if err == io.EOF {
			break
		}
		if err != nil {
			// Torn write or partial header at end of file; truncate to last valid offset
			return fs.truncateFileTo(validOffset)
		}

		totalLen := binary.BigEndian.Uint32(lenBuf)
		if totalLen < 24 { // 4 (CRC) + 8 (Index) + 8 (Term) + 4 (DataLen)
			return fs.truncateFileTo(validOffset)
		}

		payload := make([]byte, totalLen)
		_, err = io.ReadFull(fs.logFile, payload)
		if err != nil {
			// Incomplete record write; truncate
			return fs.truncateFileTo(validOffset)
		}

		storedCRC := binary.BigEndian.Uint32(payload[0:4])
		computedCRC := crc32.ChecksumIEEE(payload[4:])
		if storedCRC != computedCRC {
			// CRC mismatch: data corruption or torn write; truncate
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
			// Index gap or disorder detected; truncate
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

	// Seek to end of valid entries ready for appending
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
// If an entry's index conflicts with existing entries, the log is truncated
// at that index before appending the new entries (Raft Paper, Section 5.3).
func (fs *FileStorage) Append(entries ...LogEntry) error {
	if len(entries) == 0 {
		return nil
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	for _, entry := range entries {
		lastIdx := fs.entries[len(fs.entries)-1].Index

		// If entry index is less than or equal to lastIndex, we have a conflict.
		// Raft §5.3: "If an existing entry conflicts with a new one (same index but different terms),
		// delete the existing entry and all that follow it".
		if entry.Index <= lastIdx {
			if err := fs.truncateLocked(entry.Index); err != nil {
				return err
			}
			lastIdx = fs.entries[len(fs.entries)-1].Index
		}

		// Enforce contiguous log indices
		if entry.Index != lastIdx+1 {
			return fmt.Errorf("%w: expected index %d, got %d", ErrIndexGap, lastIdx+1, entry.Index)
		}

		// Encode record
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

	// Synchronous flush to stable storage
	if err := fs.logFile.Sync(); err != nil {
		return fmt.Errorf("failed to fsync log file: %w", err)
	}

	return nil
}

// Truncate deletes all log entries from fromIndex onwards (inclusive)
// both in memory and on disk synchronously.
func (fs *FileStorage) Truncate(fromIndex uint64) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.truncateLocked(fromIndex)
}

func (fs *FileStorage) truncateLocked(fromIndex uint64) error {
	if fromIndex == 0 {
		return fmt.Errorf("%w: cannot truncate index 0", ErrIndexOutOfBounds)
	}

	if fromIndex >= uint64(len(fs.entries)) {
		// Nothing to truncate
		return nil
	}

	offset := fs.offsets[fromIndex]
	if err := fs.logFile.Truncate(offset); err != nil {
		return fmt.Errorf("failed to truncate log file at offset %d: %w", offset, err)
	}
	if _, err := fs.logFile.Seek(offset, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek log file: %w", err)
	}
	if err := fs.logFile.Sync(); err != nil {
		return fmt.Errorf("failed to fsync log file after truncate: %w", err)
	}

	fs.entries = fs.entries[:fromIndex]
	fs.offsets = fs.offsets[:fromIndex]
	return nil
}

// GetEntry retrieves the log entry at the given 1-based index.
func (fs *FileStorage) GetEntry(index uint64) (*LogEntry, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	if index == 0 || index >= uint64(len(fs.entries)) {
		return nil, ErrEntryNotFound
	}
	entry := fs.entries[index]
	return &entry, nil
}

// GetEntries returns entries in the half-open range [fromIndex, toIndex).
func (fs *FileStorage) GetEntries(fromIndex, toIndex uint64) ([]LogEntry, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	if fromIndex == 0 || fromIndex > toIndex || toIndex > uint64(len(fs.entries)) {
		return nil, ErrIndexOutOfBounds
	}

	res := make([]LogEntry, toIndex-fromIndex)
	copy(res, fs.entries[fromIndex:toIndex])
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
// For index 0, it returns term 0.
func (fs *FileStorage) Term(index uint64) (uint64, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	if index >= uint64(len(fs.entries)) {
		return 0, ErrEntryNotFound
	}
	return fs.entries[index].Term, nil
}

// FirstIndex returns the first valid index (1 for an active log, or 1 if empty).
func (fs *FileStorage) FirstIndex() uint64 {
	return 1
}

// LastIndex returns the last valid index in the log (0 if empty).
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

	// Write atomically via temporary file and rename
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

	// Flush to disk
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

	// Sync directory to guarantee atomic rename persistence
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

// Close closes the underlying log file handle.
func (fs *FileStorage) Close() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if fs.logFile != nil {
		if err := fs.logFile.Sync(); err != nil {
			_ = fs.logFile.Close()
			return err
		}
		err := fs.logFile.Close()
		fs.logFile = nil
		return err
	}
	return nil
}

// encodeEntry serializes a LogEntry into:
// [TotalLen 4B] [CRC32 4B] [Index 8B] [Term 8B] [DataLen 4B] [Data N bytes]
func encodeEntry(e LogEntry) []byte {
	dataLen := len(e.Data)
	payloadLen := 4 + 8 + 8 + 4 + dataLen // CRC + Index + Term + DataLen + Data
	buf := make([]byte, 4+payloadLen)

	// TotalLen
	binary.BigEndian.PutUint32(buf[0:4], uint32(payloadLen))

	// Index, Term, DataLen
	binary.BigEndian.PutUint64(buf[8:16], e.Index)
	binary.BigEndian.PutUint64(buf[16:24], e.Term)
	binary.BigEndian.PutUint32(buf[24:28], uint32(dataLen))
	copy(buf[28:], e.Data)

	// CRC32 over payload starting at offset 8 (Index + Term + DataLen + Data)
	checksum := crc32.ChecksumIEEE(buf[8:])
	binary.BigEndian.PutUint32(buf[4:8], checksum)

	return buf
}
