# kv: log-structured key-value store

[![CI](https://github.com/umer-78/kv-store/actions/workflows/ci.yml/badge.svg)](https://github.com/umer-78/kv-store/actions/workflows/ci.yml)

**Live demo:** https://umer-78.github.io/kv-store/

A log-structured key-value store in Go, written from scratch: write-ahead log,
in-memory table, immutable sorted files, bloom filters, compaction and crash
recovery. No dependencies outside the standard library.

```
$ kv -dir data bench 50000
50000 keys, 100-byte values, 3 sstable(s), 6.2 MB on disk

sequential writes         196459 ops/sec
random reads, present      38888 ops/sec   (50000 found)
random reads, absent     2107237 ops/sec   (50000 correctly absent)

bloom filters answered 82.3% of file lookups without a read
```

Reading a key that is *absent* is fifty times faster than reading one that is
present. That is the whole argument for bloom filters in one line: the filter
says "definitely not here" without touching the disk, and four fifths of file
lookups never become reads.

- **56 tests**, all passing under `-race`
- 80–100% coverage per package, single static binary, distroless image

## Quick start

```bash
git clone https://github.com/umer-78/kv-store.git
cd kv-store
make test                       # 56 tests
make bench

kv -dir data put city Lahore
kv -dir data get city           # Lahore
kv -dir data delete city
kv -dir data scan
kv -dir data stats
kv -dir data compact
```

## How it is put together

```
write  →  write-ahead log  →  memtable (sorted, in memory)
                                   ↓  full
                              sstable on disk (immutable, sorted, bloom-filtered)
                                   ↓  several
                              compaction merges them into one

read   →  memtable  →  newest sstable  →  …  →  oldest
          stop at the first answer, including a tombstone
```

That last line is the sentence the design turns on.

## A delete does not delete anything

It writes a tombstone. Removing the key would be wrong: an older value may still
be sitting in an older file, and dropping the marker lets it come back the next
time the key is read. Resurrection-after-delete is the classic LSM bug, and
three tests are built around it — the value flushed to one file, the tombstone
flushed to a newer one, then checked across a read, a compaction and a reopen.

A tombstone is only safe to discard once nothing older can contradict it. Here
that means after a full merge, and not before.

## What happens when the process is killed

Every change goes to the log first and, with `-sync`, is fsynced before the write
is acknowledged. A process killed mid-append leaves a partial record at the end
of the file: a length header with no body, or a body cut short.

That is normal, not corruption. The write was never acknowledged, so discarding
it is correct — and refusing to open the database over it would turn a crash into
an outage. Damage in the *middle* of the file is a different thing entirely, and
is reported rather than skipped:

```
wal wal.log: checksum mismatch at offset 152 — the file is corrupt, not merely truncated
```

Both paths are tested: truncate the tail and the database opens with the records
that completed; flip bytes in the middle and replay refuses.

```
$ kv -dir data get city
recovered 47 entries from the log, discarded 9 trailing bytes from an interrupted write
Lahore
```

The checksum covers the length fields as well as the payload, so a corrupted
length is caught rather than acted on — otherwise a flipped bit becomes a
two-gigabyte allocation.

## The bloom filter bug worth keeping

A bloom filter may say "maybe present" for a key that is absent. It may **never**
say "absent" for a key that is present: that would lose data silently, and no
round-trip test would catch it.

The first version derived its second hash by running FNV again over the same
bytes with a salt. That is different from the first hash but not independent of
it, and the k positions landed in a handful of patterns instead of spreading:

```
false negatives: 0        (correct, but it was always going to be)
false positives: 4.67%    against a 1% target
```

Passing the first hash through splitmix64's avalanche finaliser instead:

```
false negatives: 0
false positives: 1.04%    against a 1% target
```

A four-times worse hit rate, from a filter that looked entirely correct. There is
now a test that measures the rate against the target, so a regression shows up as
a failure rather than as a slow database.

## Files

Each sstable is records, then a sparse index, then the filter, then a fixed
footer. The index samples every sixteenth key, so a lookup seeks to a block and
scans forward — a million-key file does not need a million index entries in
memory, and a test asserts the index stays proportionally small.

Compaction merges every file newest-first, keeps the first sighting of each key
and drops tombstones. Rewriting the same hundred keys six times and then
compacting collapses the on-disk size by more than half; that is a test too.

## Ordering that matters

Two places where doing it the other way round loses data:

**Log before memory.** If the memtable were updated first and the process died
before the append, the write would have been acknowledged and then vanished.

**File before truncate.** A flush writes the sstable, fsyncs it and opens it
*before* clearing the log. Truncating first would make a crash in between lose
everything the memtable held.

## Commands

```
put <key> <value>      store a value
get <key>              read one (exit 1 if absent)
delete <key>           write a tombstone
scan [from] [to]       live keys in [from, to)
stats                  counters, file count, bloom hit rate
compact                merge every file into one
bench [n]              write and read n keys
shell                  read commands from standard input
```

```
-dir    where the data lives          (default "data")
-mem    flush past this many bytes    (default 1048576)
-sync   fsync every write             (default off)
-files  compact past this many files  (default 4)
```

## As a library

```go
db, err := engine.Open(engine.Options{Dir: "data", MemtableBytes: 4 << 20, SyncWrites: true})
defer db.Close()

db.Put("city", []byte("Lahore"))
value, err := db.Get("city")          // []byte("Lahore"), nil
db.Delete("city")
_, err = db.Get("city")               // engine.ErrNotFound

entries, _ := db.Scan("a", "n")       // live keys in [a, n), in order
db.Compact()
```

## Layout

```
internal/wal/        append, fsync, replay, torn-tail handling
internal/memtable/   the sorted in-memory table and its tombstones
internal/sstable/    the on-disk format: records, sparse index, filter, footer
internal/bloom/      the filter, and the hash independence it depends on
internal/engine/     the database: reads, flushes, compaction, recovery
cmd/kv/              the command
```

## Not included

Transactions, secondary indexes, levelled compaction, block compression, MVCC,
replication, a network protocol. One process, one directory, one lock. What is
here is the storage engine underneath all of that, small enough to read and
tested where it would actually lose your data.

## Licence

MIT — see [LICENSE](LICENSE).
