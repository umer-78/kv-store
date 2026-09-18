// Package wal is the write-ahead log: every change is appended here and made
// durable before it is acknowledged, so a crash costs nothing that was reported
// as written.
//
// The part that decides whether this works is the tail. A process killed
// mid-append leaves a partial record at the end of the file — a length header
// with no body, or a body cut short. That is normal, not corruption: the write
// was never acknowledged, so discarding it is correct. What is *not* acceptable
// is refusing to open the database because of it, which is how a crash turns
// into an outage.
package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

// Record kinds.
const (
	OpPut    byte = 1
	OpDelete byte = 2
)

// header is: crc32 (4) | kind (1) | key length (4) | value length (4)
const headerSize = 13

// Entry is one logged change.
type Entry struct {
	Op    byte
	Key   []byte
	Value []byte
}

// Log appends entries to a file.
type Log struct {
	path   string
	file   *os.File
	writer *bufio.Writer
	bytes  int64
	sync   bool
}

// Open creates or reopens a log for appending. sync=true calls fsync on every
// append: slower, and the only setting under which a power cut is survivable.
func Open(path string, sync bool) (*Log, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	return &Log{path: path, file: file, writer: bufio.NewWriterSize(file, 64<<10),
		bytes: info.Size(), sync: sync}, nil
}

// Append writes one entry and, when sync is on, makes it durable.
func (l *Log) Append(entry Entry) error {
	if len(entry.Key) == 0 {
		return errors.New("a key cannot be empty")
	}

	header := make([]byte, headerSize)
	header[4] = entry.Op
	binary.LittleEndian.PutUint32(header[5:], uint32(len(entry.Key)))
	binary.LittleEndian.PutUint32(header[9:], uint32(len(entry.Value)))

	// The checksum covers the payload *and* the part of the header that
	// describes it, so a corrupted length is caught rather than acted on.
	sum := crc32.NewIEEE()
	sum.Write(header[4:])
	sum.Write(entry.Key)
	sum.Write(entry.Value)
	binary.LittleEndian.PutUint32(header, sum.Sum32())

	for _, chunk := range [][]byte{header, entry.Key, entry.Value} {
		if _, err := l.writer.Write(chunk); err != nil {
			return err
		}
	}
	l.bytes += int64(headerSize + len(entry.Key) + len(entry.Value))

	if err := l.writer.Flush(); err != nil {
		return err
	}
	if l.sync {
		return l.file.Sync()
	}
	return nil
}

// Size is how many bytes the log holds.
func (l *Log) Size() int64 { return l.bytes }

// Close flushes and closes the file.
func (l *Log) Close() error {
	if err := l.writer.Flush(); err != nil {
		l.file.Close()
		return err
	}
	return l.file.Close()
}

// Truncate empties the log, which is what happens once its contents are safely
// in an SSTable.
func (l *Log) Truncate() error {
	if err := l.writer.Flush(); err != nil {
		return err
	}
	if err := l.file.Truncate(0); err != nil {
		return err
	}
	if _, err := l.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	l.bytes = 0
	return l.file.Sync()
}

// Replay reads every complete record in order.
//
// It returns the entries it could read plus the number of trailing bytes it
// discarded. A partial record at the end is expected after a crash and is
// dropped silently; a bad checksum in the *middle* of the file is real
// corruption and is reported.
func Replay(path string) ([]Entry, int64, error) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, 0, nil
	}
	if err != nil {
		return nil, 0, err
	}
	defer file.Close()

	reader := bufio.NewReaderSize(file, 64<<10)
	var entries []Entry
	var offset int64

	for {
		header := make([]byte, headerSize)
		read, err := io.ReadFull(reader, header)
		if errors.Is(err, io.EOF) {
			return entries, 0, nil
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return entries, int64(read), nil // torn header: never acknowledged
		}
		if err != nil {
			return entries, 0, err
		}

		op := header[4]
		keyLen := binary.LittleEndian.Uint32(header[5:])
		valueLen := binary.LittleEndian.Uint32(header[9:])
		if op != OpPut && op != OpDelete {
			return entries, 0, fmt.Errorf("wal %s: unknown record kind %d at offset %d",
				filepath.Base(path), op, offset)
		}
		if keyLen == 0 || keyLen > 1<<24 || valueLen > 1<<28 {
			return entries, 0, fmt.Errorf("wal %s: implausible record at offset %d "+
				"(key %d bytes, value %d bytes)", filepath.Base(path), offset, keyLen, valueLen)
		}

		payload := make([]byte, keyLen+valueLen)
		read, err = io.ReadFull(reader, payload)
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return entries, int64(headerSize + read), nil // torn body
		}
		if err != nil {
			return entries, 0, err
		}

		sum := crc32.NewIEEE()
		sum.Write(header[4:])
		sum.Write(payload)
		if sum.Sum32() != binary.LittleEndian.Uint32(header) {
			return entries, 0, fmt.Errorf("wal %s: checksum mismatch at offset %d — "+
				"the file is corrupt, not merely truncated", filepath.Base(path), offset)
		}

		entries = append(entries, Entry{
			Op:    op,
			Key:   payload[:keyLen],
			Value: payload[keyLen:],
		})
		offset += int64(headerSize) + int64(keyLen) + int64(valueLen)
	}
}
