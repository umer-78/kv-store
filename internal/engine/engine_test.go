package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/umer-78/kv-store/internal/wal"
)

func open(t *testing.T, dir string, tune ...func(*Options)) *DB {
	t.Helper()
	options := Options{Dir: dir, MemtableBytes: 4 << 10, CompactionLimit: 3}
	for _, apply := range tune {
		apply(&options)
	}
	db, err := Open(options)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func put(t *testing.T, db *DB, key, value string) {
	t.Helper()
	if err := db.Put(key, []byte(value)); err != nil {
		t.Fatalf("put %s: %v", key, err)
	}
}

func mustGet(t *testing.T, db *DB, key, want string) {
	t.Helper()
	got, err := db.Get(key)
	if err != nil {
		t.Fatalf("get %s: %v", key, err)
	}
	if string(got) != want {
		t.Fatalf("get %s = %q, want %q", key, got, want)
	}
}

func mustMiss(t *testing.T, db *DB, key string) {
	t.Helper()
	if _, err := db.Get(key); err != ErrNotFound {
		t.Fatalf("get %s: expected ErrNotFound, got %v", key, err)
	}
}

// ------------------------------------------------------------------ basics

func TestPutThenGet(t *testing.T) {
	db := open(t, t.TempDir())
	put(t, db, "alpha", "one")
	put(t, db, "beta", "two")

	mustGet(t, db, "alpha", "one")
	mustGet(t, db, "beta", "two")
	mustMiss(t, db, "gamma")
}

func TestTheLatestValueWins(t *testing.T) {
	db := open(t, t.TempDir())
	for i := 0; i < 5; i++ {
		put(t, db, "key", fmt.Sprintf("v%d", i))
	}
	mustGet(t, db, "key", "v4")
}

func TestAnEmptyKeyIsRefused(t *testing.T) {
	db := open(t, t.TempDir())
	if err := db.Put("", []byte("x")); err == nil {
		t.Fatal("expected an error for an empty key")
	}
	if err := db.Delete(""); err == nil {
		t.Fatal("expected an error for an empty key")
	}
}

func TestAnEmptyValueIsStoredNotTreatedAsADelete(t *testing.T) {
	db := open(t, t.TempDir())
	put(t, db, "key", "")

	value, err := db.Get("key")
	if err != nil {
		t.Fatalf("an empty value is still a value: %v", err)
	}
	if len(value) != 0 {
		t.Fatalf("got %q", value)
	}
}

// ------------------------------------------------------------------ deletes

func TestDeletingAKeyThatIsOnlyInAnOlderFileKeepsItDeleted(t *testing.T) {
	// The classic LSM bug. The value is flushed to a file, then deleted, then
	// the tombstone is flushed to a newer file. A reader that skips tombstones,
	// or reads oldest-first, brings the old value back from the dead.
	dir := t.TempDir()
	db := open(t, dir)

	put(t, db, "ghost", "alive")
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := db.Delete("ghost"); err != nil {
		t.Fatal(err)
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}

	if db.Files() < 2 {
		t.Fatalf("expected the value and the tombstone in separate files, got %d", db.Files())
	}
	mustMiss(t, db, "ghost")
}

func TestADeletedKeyStaysDeletedAcrossCompaction(t *testing.T) {
	dir := t.TempDir()
	db := open(t, dir)

	put(t, db, "ghost", "alive")
	db.Flush()
	db.Delete("ghost")
	db.Flush()

	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	mustMiss(t, db, "ghost")

	if db.Files() != 1 {
		t.Fatalf("compaction should leave one file, got %d", db.Files())
	}
}

func TestADeletedKeyStaysDeletedAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	db := open(t, dir)
	put(t, db, "ghost", "alive")
	db.Flush()
	db.Delete("ghost")
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened := open(t, dir)
	mustMiss(t, reopened, "ghost")
}

func TestWritingAKeyAgainAfterDeletingItBringsItBack(t *testing.T) {
	db := open(t, t.TempDir())
	put(t, db, "key", "first")
	db.Delete("key")
	mustMiss(t, db, "key")

	put(t, db, "key", "second")
	mustGet(t, db, "key", "second")
}

func TestCompactionDropsTombstonesOnceNothingOlderRemains(t *testing.T) {
	dir := t.TempDir()
	db := open(t, dir)
	put(t, db, "a", "1")
	put(t, db, "b", "2")
	db.Flush()
	db.Delete("a")
	db.Flush()
	db.Compact()

	entries, err := db.Scan("", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Key != "b" {
		t.Fatalf("after compaction: %+v", entries)
	}
}

// ------------------------------------------------------------------ durability

func TestReopeningReplaysWhatWasNeverFlushed(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, MemtableBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		if err := db.Put(fmt.Sprintf("key-%02d", i), []byte(fmt.Sprintf("value-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	// No Close: the memtable was never written out, so only the log holds this.
	if db.Files() != 0 {
		t.Fatalf("expected nothing flushed yet, got %d files", db.Files())
	}

	reopened, err := Open(Options{Dir: dir, MemtableBytes: 1 << 20})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	if got := reopened.Stats().Recovered; got != 50 {
		t.Fatalf("replayed %d entries, want 50", got)
	}
	for i := 0; i < 50; i++ {
		mustGet(t, reopened, fmt.Sprintf("key-%02d", i), fmt.Sprintf("value-%d", i))
	}
}

func TestATornRecordAtTheEndOfTheLogIsDiscardedNotFatal(t *testing.T) {
	// A process killed mid-append leaves a partial record. That write was never
	// acknowledged, so dropping it is correct — and refusing to open the
	// database over it would turn a crash into an outage.
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, MemtableBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		db.Put(fmt.Sprintf("key-%d", i), []byte("value"))
	}
	db.log.Close()

	path := filepath.Join(dir, "wal.log")
	info, _ := os.Stat(path)
	if err := os.Truncate(path, info.Size()-4); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(Options{Dir: dir, MemtableBytes: 1 << 20})
	if err != nil {
		t.Fatalf("a torn tail must not stop the database opening: %v", err)
	}
	defer reopened.Close()

	if got := reopened.Stats().Recovered; got != 9 {
		t.Fatalf("recovered %d complete records, want 9", got)
	}
	if reopened.Stats().DiscardedWAL == 0 {
		t.Fatal("the discarded byte count should say something was dropped")
	}
	mustGet(t, reopened, "key-0", "value")
	mustMiss(t, reopened, "key-9")
}

func TestCorruptionInTheMiddleOfTheLogIsReported(t *testing.T) {
	// Unlike a torn tail, a bad checksum with valid records after it means the
	// file was damaged rather than cut short, and silently continuing would lose
	// data without saying so.
	dir := t.TempDir()
	db, _ := Open(Options{Dir: dir, MemtableBytes: 1 << 20})
	for i := 0; i < 10; i++ {
		db.Put(fmt.Sprintf("key-%d", i), []byte("value"))
	}
	db.log.Close()

	path := filepath.Join(dir, "wal.log")
	file, err := os.OpenFile(path, os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	file.WriteAt([]byte{0xff, 0xff}, 60) // flip bytes inside an early record
	file.Close()

	if _, _, err := wal.Replay(path); err == nil {
		t.Fatal("expected a checksum error for mid-file damage")
	}
}

func TestClosingFlushesTheMemtable(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(Options{Dir: dir, MemtableBytes: 1 << 20})
	db.Put("key", []byte("value"))
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	names, _ := filepath.Glob(filepath.Join(dir, "*.sst"))
	if len(names) != 1 {
		t.Fatalf("expected one sstable after close, got %d", len(names))
	}

	reopened := open(t, dir)
	if got := reopened.Stats().Recovered; got != 0 {
		t.Fatalf("the log should be empty after a clean close, replayed %d", got)
	}
	mustGet(t, reopened, "key", "value")
}

// ------------------------------------------------------------------ files

func TestFillingTheMemtableWritesAFile(t *testing.T) {
	db := open(t, t.TempDir(), func(o *Options) { o.MemtableBytes = 512; o.CompactionLimit = 100 })
	for i := 0; i < 200; i++ {
		put(t, db, fmt.Sprintf("key-%03d", i), "0123456789")
	}
	if db.Files() == 0 {
		t.Fatal("expected at least one flush")
	}
	for i := 0; i < 200; i++ {
		mustGet(t, db, fmt.Sprintf("key-%03d", i), "0123456789")
	}
}

func TestCompactionRunsOnceEnoughFilesExist(t *testing.T) {
	db := open(t, t.TempDir(), func(o *Options) { o.MemtableBytes = 256; o.CompactionLimit = 3 })
	for i := 0; i < 400; i++ {
		put(t, db, fmt.Sprintf("key-%03d", i), "0123456789abcdef")
	}
	if db.Stats().Compactions == 0 {
		t.Fatal("expected compaction to have run")
	}
	if db.Files() >= 3 {
		t.Fatalf("compaction should keep the file count down, got %d", db.Files())
	}
	for i := 0; i < 400; i++ {
		mustGet(t, db, fmt.Sprintf("key-%03d", i), "0123456789abcdef")
	}
}

func TestCompactionShrinksTheDiskFootprintOfRewrittenKeys(t *testing.T) {
	db := open(t, t.TempDir(), func(o *Options) { o.MemtableBytes = 1 << 20; o.CompactionLimit = 100 })
	for round := 0; round < 6; round++ {
		for i := 0; i < 100; i++ {
			put(t, db, fmt.Sprintf("key-%03d", i), fmt.Sprintf("round-%d-padding-padding", round))
		}
		db.Flush()
	}
	before := db.DiskBytes()

	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	after := db.DiskBytes()

	if after >= before/2 {
		t.Fatalf("six copies of every key should collapse to one: %d -> %d", before, after)
	}
	for i := 0; i < 100; i++ {
		mustGet(t, db, fmt.Sprintf("key-%03d", i), "round-5-padding-padding")
	}
}

func TestTheBloomFilterAnswersMostMissesWithoutReadingTheFile(t *testing.T) {
	db := open(t, t.TempDir(), func(o *Options) { o.MemtableBytes = 1 << 20; o.CompactionLimit = 100 })
	for i := 0; i < 500; i++ {
		put(t, db, fmt.Sprintf("present-%03d", i), "value")
	}
	db.Flush()

	for i := 0; i < 500; i++ {
		db.Get(fmt.Sprintf("absent-%03d", i))
	}

	stats := db.Stats()
	if stats.FilterSkips < 450 {
		t.Fatalf("the filter skipped only %d of 500 misses", stats.FilterSkips)
	}
}

// ------------------------------------------------------------------ scans

func TestScanReturnsLiveKeysInOrder(t *testing.T) {
	db := open(t, t.TempDir())
	for _, key := range []string{"delta", "alpha", "charlie", "bravo"} {
		put(t, db, key, key)
	}
	db.Delete("charlie")

	entries, err := db.Scan("", "")
	if err != nil {
		t.Fatal(err)
	}
	var keys []string
	for _, entry := range entries {
		keys = append(keys, entry.Key)
	}
	want := []string{"alpha", "bravo", "delta"}
	if fmt.Sprint(keys) != fmt.Sprint(want) {
		t.Fatalf("got %v, want %v", keys, want)
	}
}

func TestScanRespectsItsBounds(t *testing.T) {
	db := open(t, t.TempDir())
	for i := 0; i < 20; i++ {
		put(t, db, fmt.Sprintf("key-%02d", i), "v")
	}

	entries, _ := db.Scan("key-05", "key-10")
	if len(entries) != 5 {
		t.Fatalf("got %d entries, want 5", len(entries))
	}
	if entries[0].Key != "key-05" || entries[4].Key != "key-09" {
		t.Fatalf("bounds are [from, to): %s..%s", entries[0].Key, entries[4].Key)
	}
}

func TestScanMergesMemoryAndFilesNewestFirst(t *testing.T) {
	db := open(t, t.TempDir(), func(o *Options) { o.MemtableBytes = 1 << 20 })
	put(t, db, "key", "old")
	db.Flush()
	put(t, db, "key", "new")

	entries, _ := db.Scan("", "")
	if len(entries) != 1 || string(entries[0].Value) != "new" {
		t.Fatalf("got %+v", entries)
	}
}

// ------------------------------------------------------------------ misc

func TestOpeningWithoutADirectoryIsRefused(t *testing.T) {
	if _, err := Open(Options{}); err == nil {
		t.Fatal("expected an error")
	}
}

func TestUsingAClosedDatabaseIsRefused(t *testing.T) {
	db, _ := Open(Options{Dir: t.TempDir()})
	db.Close()

	if err := db.Put("key", []byte("value")); err == nil {
		t.Fatal("expected an error writing to a closed database")
	}
}

func TestStatsCountWhatHappened(t *testing.T) {
	db := open(t, t.TempDir())
	put(t, db, "a", "1")
	put(t, db, "b", "2")
	db.Delete("a")
	db.Get("b")
	db.Get("missing")

	stats := db.Stats()
	if stats.Puts != 2 || stats.Deletes != 1 || stats.Gets != 2 {
		t.Fatalf("%+v", stats)
	}
}

func TestALargeValueSurvivesAFlushAndReopen(t *testing.T) {
	dir := t.TempDir()
	big := make([]byte, 300<<10)
	for i := range big {
		big[i] = byte(i % 251)
	}

	db, _ := Open(Options{Dir: dir, MemtableBytes: 4 << 10})
	if err := db.Put("big", big); err != nil {
		t.Fatal(err)
	}
	db.Close()

	reopened := open(t, dir)
	got, err := reopened.Get("big")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(big) {
		t.Fatalf("got %d bytes, want %d", len(got), len(big))
	}
	for i := range big {
		if got[i] != big[i] {
			t.Fatalf("byte %d differs", i)
		}
	}
}
