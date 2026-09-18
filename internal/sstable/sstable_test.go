package sstable

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/umer-78/kv-store/internal/memtable"
)

func build(t *testing.T, count int) (string, []memtable.Entry) {
	t.Helper()
	entries := make([]memtable.Entry, count)
	for i := range entries {
		entries[i] = memtable.Entry{Key: fmt.Sprintf("key-%04d", i), Value: []byte(fmt.Sprintf("value-%d", i))}
	}
	path := filepath.Join(t.TempDir(), "table.sst")
	if _, err := Write(path, entries); err != nil {
		t.Fatal(err)
	}
	return path, entries
}

func TestEveryWrittenKeyReadsBack(t *testing.T) {
	path, entries := build(t, 500)
	table, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer table.Close()

	for _, want := range entries {
		got, found, err := table.Get(want.Key)
		if err != nil || !found {
			t.Fatalf("%s: found=%v err=%v", want.Key, found, err)
		}
		if string(got.Value) != string(want.Value) {
			t.Fatalf("%s = %q, want %q", want.Key, got.Value, want.Value)
		}
	}
}

func TestAKeyOutsideTheFileIsNotFound(t *testing.T) {
	path, _ := build(t, 200)
	table, _ := Open(path)
	defer table.Close()

	for _, key := range []string{"aaa", "zzz", "key-9999"} {
		if _, found, _ := table.Get(key); found {
			t.Fatalf("%s should not be here", key)
		}
	}
}

func TestUnsortedEntriesAreRefused(t *testing.T) {
	entries := []memtable.Entry{{Key: "b"}, {Key: "a"}}
	if _, err := Write(filepath.Join(t.TempDir(), "x.sst"), entries); err == nil {
		t.Fatal("expected an error: the format depends on the order")
	}
}

func TestTombstonesSurviveTheRoundTrip(t *testing.T) {
	entries := []memtable.Entry{
		{Key: "a", Value: []byte("1")},
		{Key: "b", Deleted: true},
		{Key: "c", Value: []byte("3")},
	}
	path := filepath.Join(t.TempDir(), "t.sst")
	if _, err := Write(path, entries); err != nil {
		t.Fatal(err)
	}
	table, _ := Open(path)
	defer table.Close()

	entry, found, _ := table.Get("b")
	if !found {
		t.Fatal("the tombstone must be found — dropping it would resurrect an older value")
	}
	if !entry.Deleted {
		t.Fatal("the deleted flag was lost")
	}
}

func TestAnEmptyTableIsValid(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.sst")
	if _, err := Write(path, nil); err != nil {
		t.Fatal(err)
	}
	table, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer table.Close()

	if table.Count() != 0 {
		t.Fatalf("count is %d", table.Count())
	}
	if _, found, _ := table.Get("anything"); found {
		t.Fatal("an empty table holds nothing")
	}
}

func TestAllReturnsEveryRecordInOrder(t *testing.T) {
	path, entries := build(t, 300)
	table, _ := Open(path)
	defer table.Close()

	got, err := table.All()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(entries) {
		t.Fatalf("got %d records, want %d", len(got), len(entries))
	}
	for i := 1; i < len(got); i++ {
		if got[i-1].Key >= got[i].Key {
			t.Fatalf("out of order at %d: %s then %s", i, got[i-1].Key, got[i].Key)
		}
	}
}

func TestAFileWithoutTheMarkerIsRejected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "junk.sst")
	os.WriteFile(path, make([]byte, 64), 0o644)

	if _, err := Open(path); err == nil {
		t.Fatal("expected the missing marker to be caught")
	}
}

func TestATruncatedFileIsRejected(t *testing.T) {
	path, _ := build(t, 100)
	info, _ := os.Stat(path)
	os.Truncate(path, info.Size()-10) // eats part of the footer

	if _, err := Open(path); err == nil {
		t.Fatal("expected an error opening a truncated table")
	}
}

func TestADamagedRecordFailsItsChecksumRatherThanReturningRubbish(t *testing.T) {
	path, _ := build(t, 50)

	file, _ := os.OpenFile(path, os.O_WRONLY, 0o644)
	file.WriteAt([]byte("XXXXXXXX"), 30)
	file.Close()

	table, err := Open(path)
	if err != nil {
		return // rejected at open, which is also fine
	}
	defer table.Close()

	if _, err := table.All(); err == nil {
		t.Fatal("expected a checksum failure on the damaged record")
	}
}

func TestTheSparseIndexHoldsFarFewerEntriesThanThereAreKeys(t *testing.T) {
	// The point of sampling: a million-key file should not need a million index
	// entries resident in memory.
	path, _ := build(t, 1000)
	table, _ := Open(path)
	defer table.Close()

	if len(table.index) > 1000/indexStride+2 {
		t.Fatalf("index holds %d entries for 1000 keys", len(table.index))
	}
	if len(table.index) < 2 {
		t.Fatalf("index is too sparse to be useful: %d entries", len(table.index))
	}
}
