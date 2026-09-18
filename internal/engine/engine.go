// Package engine ties the pieces together into a database.
//
// The shape is a log-structured merge tree, the same one behind LevelDB, RocksDB
// and Cassandra. Writes go to a log and an in-memory table; when the table is
// big enough it is written out as an immutable sorted file; files accumulate and
// are periodically merged. Reads consult memory first, then files newest to
// oldest, and stop at the first answer — including a tombstone.
//
// "Including a tombstone" is the sentence the whole design turns on. A delete
// does not remove anything: it writes a marker. Stopping at that marker is what
// keeps an older value, still sitting in an older file, from coming back.
package engine

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/umer-78/kv-store/internal/memtable"
	"github.com/umer-78/kv-store/internal/sstable"
	"github.com/umer-78/kv-store/internal/wal"
)

// ErrNotFound is returned by Get for a key that is absent or deleted.
var ErrNotFound = errors.New("key not found")

// Options configures a database.
type Options struct {
	Dir             string
	MemtableBytes   int  // flush once the memtable holds this much
	SyncWrites      bool // fsync on every write: slower, survives a power cut
	CompactionLimit int  // merge once this many files exist
}

// DB is an open database.
type DB struct {
	mu      sync.RWMutex
	options Options
	log     *wal.Log
	active  *memtable.Table
	tables  []*sstable.Table // newest first
	nextID  int
	stats   counters
	closed  bool
}

// Stats counts what the database has done.
type Stats struct {
	Puts         int64
	Deletes      int64
	Gets         int64
	Flushes      int64
	Compactions  int64
	FilterSkips  int64 // lookups a bloom filter answered without touching the file
	FileReads    int64
	Recovered    int64
	DiscardedWAL int64
}

// counters are atomic so a read path can record what it did without taking the
// write lock. Reaching for the write lock mid-read — releasing the read lock,
// taking the write lock, then re-acquiring the read lock — would let the set of
// files change underneath a lookup already in progress.
type counters struct {
	puts        atomic.Int64
	deletes     atomic.Int64
	gets        atomic.Int64
	flushes     atomic.Int64
	compactions atomic.Int64
	filterSkips atomic.Int64
	fileReads   atomic.Int64
	recovered   atomic.Int64
	discarded   atomic.Int64
}

// Open starts or reopens a database, replaying the log and loading the files.
func Open(options Options) (*DB, error) {
	if options.Dir == "" {
		return nil, errors.New("a directory is required")
	}
	if options.MemtableBytes <= 0 {
		options.MemtableBytes = 1 << 20
	}
	if options.CompactionLimit <= 1 {
		options.CompactionLimit = 4
	}
	if err := os.MkdirAll(options.Dir, 0o755); err != nil {
		return nil, err
	}

	db := &DB{options: options, active: memtable.New()}

	names, err := filepath.Glob(filepath.Join(options.Dir, "*.sst"))
	if err != nil {
		return nil, err
	}
	sort.Sort(sort.Reverse(sort.StringSlice(names))) // newest id first
	for _, name := range names {
		table, err := sstable.Open(name)
		if err != nil {
			db.closeTables()
			return nil, fmt.Errorf("opening %s: %w", filepath.Base(name), err)
		}
		db.tables = append(db.tables, table)
		if id := idFromName(name); id >= db.nextID {
			db.nextID = id + 1
		}
	}

	// Replay whatever the log holds before accepting new writes.
	entries, discarded, err := wal.Replay(db.walPath())
	if err != nil {
		db.closeTables()
		return nil, err
	}
	for _, entry := range entries {
		if entry.Op == wal.OpDelete {
			db.active.Delete(string(entry.Key))
		} else {
			db.active.Put(string(entry.Key), entry.Value)
		}
	}
	db.stats.recovered.Store(int64(len(entries)))
	db.stats.discarded.Store(discarded)

	db.log, err = wal.Open(db.walPath(), options.SyncWrites)
	if err != nil {
		db.closeTables()
		return nil, err
	}
	return db, nil
}

func (db *DB) walPath() string { return filepath.Join(db.options.Dir, "wal.log") }

// Put stores a value. It is durable once this returns, if SyncWrites is on.
func (db *DB) Put(key string, value []byte) error {
	if key == "" {
		return errors.New("a key cannot be empty")
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return errors.New("the database is closed")
	}

	// The log first, always. Updating memory first and crashing in between would
	// acknowledge a write that no longer exists anywhere.
	if err := db.log.Append(wal.Entry{Op: wal.OpPut, Key: []byte(key), Value: value}); err != nil {
		return err
	}
	db.active.Put(key, value)
	db.stats.puts.Add(1)
	return db.maybeFlush()
}

// Delete writes a tombstone.
func (db *DB) Delete(key string) error {
	if key == "" {
		return errors.New("a key cannot be empty")
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return errors.New("the database is closed")
	}

	if err := db.log.Append(wal.Entry{Op: wal.OpDelete, Key: []byte(key)}); err != nil {
		return err
	}
	db.active.Delete(key)
	db.stats.deletes.Add(1)
	return db.maybeFlush()
}

// Get reads a key, newest source first.
func (db *DB) Get(key string) ([]byte, error) {
	db.stats.gets.Add(1)

	db.mu.RLock()
	defer db.mu.RUnlock()

	if entry, ok := db.active.Get(key); ok {
		if entry.Deleted {
			return nil, ErrNotFound
		}
		return append([]byte(nil), entry.Value...), nil
	}

	for _, table := range db.tables {
		hasFilter := table.FilterBytes() > 0
		entry, found, err := table.Get(key)
		if err != nil {
			return nil, err
		}
		if !found && hasFilter {
			db.stats.filterSkips.Add(1)
		} else {
			db.stats.fileReads.Add(1)
		}

		if found {
			if entry.Deleted {
				// Stop here. An older file may still hold a value for this key,
				// and reading past the tombstone would resurrect it.
				return nil, ErrNotFound
			}
			return append([]byte(nil), entry.Value...), nil
		}
	}
	return nil, ErrNotFound
}

// Has reports whether a key has a live value.
func (db *DB) Has(key string) bool {
	_, err := db.Get(key)
	return err == nil
}

// Scan returns live keys in [from, to) in order. Empty bounds mean unbounded.
func (db *DB) Scan(from, to string) ([]memtable.Entry, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	// Merge newest-first so the first sighting of a key wins, tombstone or not.
	seen := make(map[string]memtable.Entry)
	for _, entry := range db.active.Range(from, to) {
		seen[entry.Key] = entry
	}
	for _, table := range db.tables {
		entries, err := table.All()
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			if from != "" && strings.Compare(entry.Key, from) < 0 {
				continue
			}
			if to != "" && strings.Compare(entry.Key, to) >= 0 {
				continue
			}
			if _, ok := seen[entry.Key]; !ok {
				seen[entry.Key] = entry
			}
		}
	}

	out := make([]memtable.Entry, 0, len(seen))
	for _, entry := range seen {
		if !entry.Deleted {
			out = append(out, entry)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

func (db *DB) maybeFlush() error {
	if db.active.Bytes() < db.options.MemtableBytes {
		return nil
	}
	return db.flushLocked()
}

// Flush writes the memtable out as a file and empties the log.
func (db *DB) Flush() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.flushLocked()
}

func (db *DB) flushLocked() error {
	if db.active.Len() == 0 {
		return nil
	}

	path := filepath.Join(db.options.Dir, fmt.Sprintf("%06d.sst", db.nextID))
	if _, err := sstable.Write(path, db.active.Entries()); err != nil {
		return err
	}
	table, err := sstable.Open(path)
	if err != nil {
		return err
	}

	// Order matters: the file exists and is durable before the log is cleared.
	// Truncating first would make a crash here lose everything in between.
	db.tables = append([]*sstable.Table{table}, db.tables...)
	db.nextID++
	db.active = memtable.New()
	db.stats.flushes.Add(1)

	if err := db.log.Truncate(); err != nil {
		return err
	}
	if len(db.tables) >= db.options.CompactionLimit {
		return db.compactLocked()
	}
	return nil
}

// Compact merges every file into one, dropping shadowed values and tombstones.
func (db *DB) Compact() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := db.flushLocked(); err != nil {
		return err
	}
	return db.compactLocked()
}

func (db *DB) compactLocked() error {
	if len(db.tables) < 2 {
		return nil
	}

	// Newest first, so the first sighting of a key is the one that survives.
	winner := make(map[string]memtable.Entry)
	for _, table := range db.tables {
		entries, err := table.All()
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if _, ok := winner[entry.Key]; !ok {
				winner[entry.Key] = entry
			}
		}
	}

	merged := make([]memtable.Entry, 0, len(winner))
	for _, entry := range winner {
		// A tombstone is only safe to drop once nothing older can contradict it,
		// and after a full merge nothing older exists.
		if entry.Deleted {
			continue
		}
		merged = append(merged, entry)
	}
	sort.Slice(merged, func(i, j int) bool { return merged[i].Key < merged[j].Key })

	path := filepath.Join(db.options.Dir, fmt.Sprintf("%06d.sst", db.nextID))
	if _, err := sstable.Write(path, merged); err != nil {
		return err
	}
	table, err := sstable.Open(path)
	if err != nil {
		return err
	}

	old := db.tables
	db.tables = []*sstable.Table{table}
	db.nextID++
	db.stats.compactions.Add(1)

	// The replacement is on disk and open before the originals go.
	for _, previous := range old {
		previous.Close()
		os.Remove(previous.Path())
	}
	return nil
}

// Stats returns a snapshot of the counters.
func (db *DB) Stats() Stats {
	return Stats{
		Puts:         db.stats.puts.Load(),
		Deletes:      db.stats.deletes.Load(),
		Gets:         db.stats.gets.Load(),
		Flushes:      db.stats.flushes.Load(),
		Compactions:  db.stats.compactions.Load(),
		FilterSkips:  db.stats.filterSkips.Load(),
		FileReads:    db.stats.fileReads.Load(),
		Recovered:    db.stats.recovered.Load(),
		DiscardedWAL: db.stats.discarded.Load(),
	}
}

// Files is how many sstables are on disk.
func (db *DB) Files() int {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return len(db.tables)
}

// MemtableLen is how many keys are held in memory.
func (db *DB) MemtableLen() int {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.active.Len()
}

// DiskBytes totals the sstable sizes.
func (db *DB) DiskBytes() int64 {
	db.mu.RLock()
	defer db.mu.RUnlock()
	var total int64
	for _, table := range db.tables {
		total += table.Size()
	}
	return total
}

// Close flushes and releases everything.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil
	}
	db.closed = true
	if err := db.flushLocked(); err != nil {
		return err
	}
	db.closeTables()
	return db.log.Close()
}

func (db *DB) closeTables() {
	for _, table := range db.tables {
		table.Close()
	}
	db.tables = nil
}

func idFromName(path string) int {
	base := filepath.Base(path)
	var id int
	if _, err := fmt.Sscanf(base, "%06d.sst", &id); err != nil {
		return -1
	}
	return id
}
