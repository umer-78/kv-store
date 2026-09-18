package wal

import (
	"os"
	"path/filepath"
	"testing"
)

func write(t *testing.T, path string, count int, sync bool) {
	t.Helper()
	log, err := Open(path, sync)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < count; i++ {
		if err := log.Append(Entry{Op: OpPut, Key: []byte{byte('a' + i%26)}, Value: []byte("value")}); err != nil {
			t.Fatal(err)
		}
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestEveryAppendedRecordComesBack(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	write(t, path, 100, false)

	entries, discarded, err := Replay(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 100 || discarded != 0 {
		t.Fatalf("replayed %d entries, discarded %d", len(entries), discarded)
	}
}

func TestReplayingAMissingFileIsNotAnError(t *testing.T) {
	entries, _, err := Replay(filepath.Join(t.TempDir(), "absent.log"))
	if err != nil || entries != nil {
		t.Fatalf("entries=%v err=%v", entries, err)
	}
}

func TestATornHeaderAtTheTailIsDiscarded(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	write(t, path, 10, false)

	// Each record here is 13 bytes of header plus a 1-byte key and a 5-byte
	// value: 19 in all. Cutting 14 off the end leaves five bytes of the last
	// record's header — a length field that was never completed.
	info, _ := os.Stat(path)
	if err := os.Truncate(path, info.Size()-14); err != nil {
		t.Fatal(err)
	}

	entries, discarded, err := Replay(path)
	if err != nil {
		t.Fatalf("a torn tail is expected after a crash, not an error: %v", err)
	}
	if len(entries) != 9 {
		t.Fatalf("recovered %d complete records, want 9", len(entries))
	}
	if discarded == 0 {
		t.Fatal("the discarded count should be non-zero")
	}
}

func TestATornBodyAtTheTailIsDiscarded(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	write(t, path, 10, false)

	info, _ := os.Stat(path)
	if err := os.Truncate(path, info.Size()-3); err != nil {
		t.Fatal(err)
	}

	entries, discarded, err := Replay(path)
	if err != nil {
		t.Fatalf("expected a clean recovery, got %v", err)
	}
	if len(entries) != 9 || discarded == 0 {
		t.Fatalf("entries=%d discarded=%d", len(entries), discarded)
	}
}

func TestDamageInTheMiddleIsReportedRatherThanIgnored(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	write(t, path, 20, false)

	file, err := os.OpenFile(path, os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	file.WriteAt([]byte{0x00, 0x00, 0x00}, 40)
	file.Close()

	if _, _, err := Replay(path); err == nil {
		t.Fatal("a bad checksum with valid records after it is corruption, not truncation")
	}
}

func TestAnImplausibleLengthIsCaughtBeforeAllocating(t *testing.T) {
	// The checksum covers the length fields for exactly this reason: a corrupted
	// length must not be acted on.
	path := filepath.Join(t.TempDir(), "wal.log")
	write(t, path, 3, false)

	file, _ := os.OpenFile(path, os.O_WRONLY, 0o644)
	file.WriteAt([]byte{0xff, 0xff, 0xff, 0x7f}, 5) // a 2GB key length
	file.Close()

	if _, _, err := Replay(path); err == nil {
		t.Fatal("expected the implausible length to be rejected")
	}
}

func TestAnEmptyKeyIsRefused(t *testing.T) {
	log, _ := Open(filepath.Join(t.TempDir(), "wal.log"), false)
	defer log.Close()

	if err := log.Append(Entry{Op: OpPut, Key: nil, Value: []byte("v")}); err == nil {
		t.Fatal("expected an error")
	}
}

func TestTruncateEmptiesTheLog(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	log, _ := Open(path, false)
	log.Append(Entry{Op: OpPut, Key: []byte("k"), Value: []byte("v")})

	if err := log.Truncate(); err != nil {
		t.Fatal(err)
	}
	if log.Size() != 0 {
		t.Fatalf("size is %d after truncate", log.Size())
	}
	log.Close()

	entries, _, _ := Replay(path)
	if len(entries) != 0 {
		t.Fatalf("replayed %d entries from a truncated log", len(entries))
	}
}

func TestDeletesAndPutsAreDistinguishedOnReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	log, _ := Open(path, false)
	log.Append(Entry{Op: OpPut, Key: []byte("k"), Value: []byte("v")})
	log.Append(Entry{Op: OpDelete, Key: []byte("k")})
	log.Close()

	entries, _, err := Replay(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || entries[0].Op != OpPut || entries[1].Op != OpDelete {
		t.Fatalf("got %+v", entries)
	}
}

func TestSyncedWritesStillReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	write(t, path, 25, true)

	entries, _, err := Replay(path)
	if err != nil || len(entries) != 25 {
		t.Fatalf("entries=%d err=%v", len(entries), err)
	}
}
