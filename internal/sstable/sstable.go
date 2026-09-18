// Package sstable writes and reads the immutable sorted files a memtable
// becomes.
//
// Layout, in order: the records, then a sparse index, then the bloom filter,
// then a fixed footer pointing at the other two. Reading it is one seek to the
// end for the footer, one read for the index and filter, and then at most one
// read per lookup — the index only samples every Nth key, so a lookup lands on
// a block and scans forward rather than holding every key in memory.
package sstable

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sort"

	"github.com/umer-78/kv-store/internal/bloom"
	"github.com/umer-78/kv-store/internal/memtable"
)

const (
	magic        uint32 = 0x53535442 // "SSTB"
	footerSize          = 28
	indexStride         = 16 // one index entry per 16 records
	recordHeader        = 13 // crc32 (4) | flags (1) | key len (4) | value len (4)
)

// Table is an open, read-only file.
type Table struct {
	path    string
	file    *os.File
	index   []indexEntry
	filter  *bloom.Filter
	count   int
	minKey  string
	maxKey  string
	fileLen int64
}

type indexEntry struct {
	key    string
	offset int64
}

// Write serialises entries (which must be sorted by key) to path.
func Write(path string, entries []memtable.Entry) (int, error) {
	if !sort.SliceIsSorted(entries, func(i, j int) bool { return entries[i].Key < entries[j].Key }) {
		return 0, errors.New("sstable entries must be sorted by key")
	}

	file, err := os.Create(path)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	writer := bufio.NewWriterSize(file, 64<<10)
	filter := bloom.New(max(len(entries), 1), 0.01)
	var index []indexEntry
	var offset int64

	for i, entry := range entries {
		if i%indexStride == 0 {
			index = append(index, indexEntry{key: entry.Key, offset: offset})
		}
		filter.Add([]byte(entry.Key))

		header := make([]byte, recordHeader)
		if entry.Deleted {
			header[4] = 1
		}
		binary.LittleEndian.PutUint32(header[5:], uint32(len(entry.Key)))
		binary.LittleEndian.PutUint32(header[9:], uint32(len(entry.Value)))

		sum := crc32.NewIEEE()
		sum.Write(header[4:])
		sum.Write([]byte(entry.Key))
		sum.Write(entry.Value)
		binary.LittleEndian.PutUint32(header, sum.Sum32())

		for _, chunk := range [][]byte{header, []byte(entry.Key), entry.Value} {
			if _, err := writer.Write(chunk); err != nil {
				return 0, err
			}
		}
		offset += int64(recordHeader + len(entry.Key) + len(entry.Value))
	}

	indexOffset := offset
	for _, item := range index {
		buffer := make([]byte, 4+len(item.key)+8)
		binary.LittleEndian.PutUint32(buffer, uint32(len(item.key)))
		copy(buffer[4:], item.key)
		binary.LittleEndian.PutUint64(buffer[4+len(item.key):], uint64(item.offset))
		if _, err := writer.Write(buffer); err != nil {
			return 0, err
		}
		offset += int64(len(buffer))
	}

	filterOffset := offset
	filterBytes := filter.Bytes()
	if _, err := writer.Write(filterBytes); err != nil {
		return 0, err
	}
	offset += int64(len(filterBytes))

	footer := make([]byte, footerSize)
	binary.LittleEndian.PutUint64(footer, uint64(indexOffset))
	binary.LittleEndian.PutUint64(footer[8:], uint64(filterOffset))
	binary.LittleEndian.PutUint64(footer[16:], uint64(len(entries)))
	binary.LittleEndian.PutUint32(footer[24:], magic)
	if _, err := writer.Write(footer); err != nil {
		return 0, err
	}

	if err := writer.Flush(); err != nil {
		return 0, err
	}
	// fsync before the file counts as written: a flush only reaches the OS.
	if err := file.Sync(); err != nil {
		return 0, err
	}
	return len(entries), nil
}

// Open reads the footer, index and filter into memory.
func Open(path string) (*Table, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	if info.Size() < footerSize {
		file.Close()
		return nil, fmt.Errorf("%s is too short to be an sstable", path)
	}

	footer := make([]byte, footerSize)
	if _, err := file.ReadAt(footer, info.Size()-footerSize); err != nil {
		file.Close()
		return nil, err
	}
	if binary.LittleEndian.Uint32(footer[24:]) != magic {
		file.Close()
		return nil, fmt.Errorf("%s does not carry the sstable marker", path)
	}

	indexOffset := int64(binary.LittleEndian.Uint64(footer))
	filterOffset := int64(binary.LittleEndian.Uint64(footer[8:]))
	count := int(binary.LittleEndian.Uint64(footer[16:]))

	table := &Table{path: path, file: file, count: count, fileLen: info.Size()}

	indexBytes := make([]byte, filterOffset-indexOffset)
	if _, err := file.ReadAt(indexBytes, indexOffset); err != nil {
		file.Close()
		return nil, err
	}
	for position := 0; position+4 <= len(indexBytes); {
		keyLen := int(binary.LittleEndian.Uint32(indexBytes[position:]))
		if position+4+keyLen+8 > len(indexBytes) {
			break
		}
		key := string(indexBytes[position+4 : position+4+keyLen])
		offset := int64(binary.LittleEndian.Uint64(indexBytes[position+4+keyLen:]))
		table.index = append(table.index, indexEntry{key: key, offset: offset})
		position += 4 + keyLen + 8
	}

	filterBytes := make([]byte, info.Size()-footerSize-filterOffset)
	if _, err := file.ReadAt(filterBytes, filterOffset); err != nil {
		file.Close()
		return nil, err
	}
	if filter, ok := bloom.Load(filterBytes); ok {
		table.filter = filter
	}

	if len(table.index) > 0 {
		table.minKey = table.index[0].key
	}
	if entries, err := table.All(); err == nil && len(entries) > 0 {
		table.maxKey = entries[len(entries)-1].Key
	}
	return table, nil
}

// Get finds a key. The second result is whether this file knows about it.
func (t *Table) Get(key string) (memtable.Entry, bool, error) {
	if t.filter != nil && !t.filter.MayContain([]byte(key)) {
		return memtable.Entry{}, false, nil // definitive: the file cannot hold it
	}
	if t.count == 0 || (t.minKey != "" && key < t.minKey) || (t.maxKey != "" && key > t.maxKey) {
		return memtable.Entry{}, false, nil
	}

	// The index samples every 16th key, so start at the last sampled key that is
	// not past the one wanted and scan forward from there.
	start := int64(0)
	for _, item := range t.index {
		if item.key > key {
			break
		}
		start = item.offset
	}

	reader := bufio.NewReaderSize(io.NewSectionReader(t.file, start, t.fileLen-start), 32<<10)
	for {
		entry, err := readRecord(reader)
		if errors.Is(err, io.EOF) {
			return memtable.Entry{}, false, nil
		}
		if err != nil {
			return memtable.Entry{}, false, err
		}
		if entry.Key == key {
			return entry, true, nil
		}
		if entry.Key > key {
			return memtable.Entry{}, false, nil // sorted, so it is not here
		}
	}
}

// All reads every record in key order.
func (t *Table) All() ([]memtable.Entry, error) {
	reader := bufio.NewReaderSize(io.NewSectionReader(t.file, 0, t.fileLen), 64<<10)
	out := make([]memtable.Entry, 0, t.count)
	for i := 0; i < t.count; i++ {
		entry, err := readRecord(reader)
		if err != nil {
			return out, err
		}
		out = append(out, entry)
	}
	return out, nil
}

func readRecord(reader io.Reader) (memtable.Entry, error) {
	header := make([]byte, recordHeader)
	if _, err := io.ReadFull(reader, header); err != nil {
		return memtable.Entry{}, err
	}
	keyLen := binary.LittleEndian.Uint32(header[5:])
	valueLen := binary.LittleEndian.Uint32(header[9:])

	payload := make([]byte, keyLen+valueLen)
	if _, err := io.ReadFull(reader, payload); err != nil {
		return memtable.Entry{}, err
	}

	sum := crc32.NewIEEE()
	sum.Write(header[4:])
	sum.Write(payload)
	if sum.Sum32() != binary.LittleEndian.Uint32(header) {
		return memtable.Entry{}, errors.New("sstable record failed its checksum")
	}

	return memtable.Entry{
		Key:     string(payload[:keyLen]),
		Value:   payload[keyLen:],
		Deleted: header[4] == 1,
	}, nil
}

// Count is how many records the file holds, tombstones included.
func (t *Table) Count() int { return t.count }

// Path is the file this table reads.
func (t *Table) Path() string { return t.path }

// Size is the file's length in bytes.
func (t *Table) Size() int64 { return t.fileLen }

// FilterBytes is how much memory the bloom filter occupies.
func (t *Table) FilterBytes() int {
	if t.filter == nil {
		return 0
	}
	return t.filter.SizeBytes()
}

// Close releases the file handle.
func (t *Table) Close() error { return t.file.Close() }

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
