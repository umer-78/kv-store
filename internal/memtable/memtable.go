// Package memtable holds recent writes in memory, sorted, until there are
// enough of them to be worth writing out as a file.
package memtable

import (
	"sort"
	"strings"
	"sync"
)

// Entry is a key with its value, or a tombstone marking it deleted.
type Entry struct {
	Key     string
	Value   []byte
	Deleted bool
}

// Table is a sorted in-memory map guarded by a mutex.
//
// A delete stores a tombstone rather than removing the key. Removing it would
// be wrong: an older value for that key may still sit in a file on disk, and
// dropping the marker would let the old value come back the next time the table
// is consulted. Resurrection-after-delete is the classic LSM bug and a test
// here is built around it.
type Table struct {
	mu      sync.RWMutex
	entries map[string]Entry
	bytes   int
}

// New returns an empty table.
func New() *Table {
	return &Table{entries: make(map[string]Entry)}
}

// Put stores a value.
func (t *Table) Put(key string, value []byte) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.account(key, Entry{Key: key, Value: value})
}

// Delete stores a tombstone.
func (t *Table) Delete(key string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.account(key, Entry{Key: key, Deleted: true})
}

func (t *Table) account(key string, entry Entry) {
	if previous, ok := t.entries[key]; ok {
		t.bytes -= len(previous.Value)
	} else {
		t.bytes += len(key)
	}
	t.bytes += len(entry.Value)
	t.entries[key] = entry
}

// Get returns the entry and whether this table knows anything about the key.
// A found tombstone still counts as knowing: the caller must not look further.
func (t *Table) Get(key string) (Entry, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	entry, ok := t.entries[key]
	return entry, ok
}

// Entries returns every entry in key order, tombstones included.
func (t *Table) Entries() []Entry {
	t.mu.RLock()
	defer t.mu.RUnlock()

	out := make([]Entry, 0, len(t.entries))
	for _, entry := range t.entries {
		out = append(out, entry)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

// Range returns entries with keys in [from, to), or all of them when both are
// empty. Tombstones are included; the caller decides what they mean.
func (t *Table) Range(from, to string) []Entry {
	all := t.Entries()
	out := make([]Entry, 0, len(all))
	for _, entry := range all {
		if from != "" && strings.Compare(entry.Key, from) < 0 {
			continue
		}
		if to != "" && strings.Compare(entry.Key, to) >= 0 {
			continue
		}
		out = append(out, entry)
	}
	return out
}

// Len is the number of distinct keys, tombstones included.
func (t *Table) Len() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.entries)
}

// Bytes approximates the memory held, for deciding when to flush.
func (t *Table) Bytes() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.bytes
}
