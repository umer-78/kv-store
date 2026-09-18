// Command kv is a shell over the store: put, get, delete, scan, and the
// maintenance commands that make the internals visible.
package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/umer-78/kv-store/internal/engine"
)

const usage = `kv — a log-structured key-value store

  kv [options] <command> [arguments]

Commands:
  put <key> <value>      store a value
  get <key>              read one
  delete <key>           write a tombstone
  scan [from] [to]       list live keys in [from, to)
  stats                  what the store has done and what is on disk
  compact                merge every file into one
  bench [n]              write and read n keys, reporting throughput
  shell                  read commands from standard input

Options:
  -dir string    where the data lives (default "data")
  -mem int       flush the memtable past this many bytes (default 1048576)
  -sync          fsync every write: slower, survives a power cut
  -files int     compact once this many files exist (default 4)
`

func main() {
	dir := flag.String("dir", "data", "")
	mem := flag.Int("mem", 1<<20, "")
	sync := flag.Bool("sync", false, "")
	files := flag.Int("files", 4, "")
	flag.Usage = func() { fmt.Fprint(os.Stderr, usage) }
	flag.Parse()

	if flag.NArg() == 0 {
		flag.Usage()
		os.Exit(2)
	}

	db, err := engine.Open(engine.Options{
		Dir: *dir, MemtableBytes: *mem, SyncWrites: *sync, CompactionLimit: *files,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "kv: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	if stats := db.Stats(); stats.Recovered > 0 || stats.DiscardedWAL > 0 {
		fmt.Fprintf(os.Stderr, "recovered %d entries from the log", stats.Recovered)
		if stats.DiscardedWAL > 0 {
			fmt.Fprintf(os.Stderr, ", discarded %d trailing bytes from an interrupted write",
				stats.DiscardedWAL)
		}
		fmt.Fprintln(os.Stderr)
	}

	if err := run(db, flag.Args()); err != nil {
		fmt.Fprintf(os.Stderr, "kv: %v\n", err)
		os.Exit(1)
	}
}

func run(db *engine.DB, args []string) error {
	switch args[0] {
	case "put":
		if len(args) < 3 {
			return errors.New("put needs a key and a value")
		}
		return db.Put(args[1], []byte(strings.Join(args[2:], " ")))

	case "get":
		if len(args) < 2 {
			return errors.New("get needs a key")
		}
		value, err := db.Get(args[1])
		if errors.Is(err, engine.ErrNotFound) {
			fmt.Fprintln(os.Stderr, "not found")
			os.Exit(1)
		}
		if err != nil {
			return err
		}
		fmt.Println(string(value))
		return nil

	case "delete":
		if len(args) < 2 {
			return errors.New("delete needs a key")
		}
		return db.Delete(args[1])

	case "scan":
		from, to := "", ""
		if len(args) > 1 {
			from = args[1]
		}
		if len(args) > 2 {
			to = args[2]
		}
		entries, err := db.Scan(from, to)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			fmt.Printf("%-24s %s\n", entry.Key, entry.Value)
		}
		fmt.Fprintf(os.Stderr, "%d keys\n", len(entries))
		return nil

	case "stats":
		return showStats(db)

	case "compact":
		before := db.Files()
		beforeBytes := db.DiskBytes()
		if err := db.Compact(); err != nil {
			return err
		}
		fmt.Printf("%d files (%s) -> %d file (%s)\n",
			before, human(beforeBytes), db.Files(), human(db.DiskBytes()))
		return nil

	case "bench":
		count := 20000
		if len(args) > 1 {
			fmt.Sscanf(args[1], "%d", &count)
		}
		return bench(db, count)

	case "shell":
		return shell(db)
	}
	return fmt.Errorf("unknown command %q — try -h", args[0])
}

func showStats(db *engine.DB) error {
	stats := db.Stats()
	fmt.Printf("%-16s %d\n", "sstables", db.Files())
	fmt.Printf("%-16s %s\n", "on disk", human(db.DiskBytes()))
	fmt.Printf("%-16s %d keys\n", "in memory", db.MemtableLen())
	fmt.Println()
	fmt.Printf("%-16s %d\n", "puts", stats.Puts)
	fmt.Printf("%-16s %d\n", "deletes", stats.Deletes)
	fmt.Printf("%-16s %d\n", "gets", stats.Gets)
	fmt.Printf("%-16s %d\n", "flushes", stats.Flushes)
	fmt.Printf("%-16s %d\n", "compactions", stats.Compactions)
	fmt.Println()
	total := stats.FilterSkips + stats.FileReads
	fmt.Printf("%-16s %d\n", "file lookups", total)
	if total > 0 {
		fmt.Printf("%-16s %d (%.1f%% answered without touching the file)\n",
			"bloom skips", stats.FilterSkips, float64(stats.FilterSkips)/float64(total)*100)
	}
	return nil
}

func bench(db *engine.DB, count int) error {
	value := []byte(strings.Repeat("x", 100))

	start := time.Now()
	for i := 0; i < count; i++ {
		if err := db.Put(fmt.Sprintf("bench-%08d", i), value); err != nil {
			return err
		}
	}
	writeSeconds := time.Since(start).Seconds()

	if err := db.Flush(); err != nil {
		return err
	}

	rng := rand.New(rand.NewSource(1))
	start = time.Now()
	hits := 0
	for i := 0; i < count; i++ {
		if _, err := db.Get(fmt.Sprintf("bench-%08d", rng.Intn(count))); err == nil {
			hits++
		}
	}
	readSeconds := time.Since(start).Seconds()

	start = time.Now()
	misses := 0
	for i := 0; i < count; i++ {
		if _, err := db.Get(fmt.Sprintf("absent-%08d", i)); errors.Is(err, engine.ErrNotFound) {
			misses++
		}
	}
	missSeconds := time.Since(start).Seconds()

	fmt.Printf("%d keys, 100-byte values, %d sstable(s), %s on disk\n\n",
		count, db.Files(), human(db.DiskBytes()))
	fmt.Printf("%-22s %9.0f ops/sec\n", "sequential writes", float64(count)/writeSeconds)
	fmt.Printf("%-22s %9.0f ops/sec   (%d found)\n", "random reads, present",
		float64(count)/readSeconds, hits)
	fmt.Printf("%-22s %9.0f ops/sec   (%d correctly absent)\n", "random reads, absent",
		float64(count)/missSeconds, misses)

	stats := db.Stats()
	if lookups := stats.FilterSkips + stats.FileReads; lookups > 0 {
		fmt.Printf("\nbloom filters answered %.1f%% of file lookups without a read\n",
			float64(stats.FilterSkips)/float64(lookups)*100)
	}
	return nil
}

func shell(db *engine.DB) error {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) == 0 {
			continue
		}
		if fields[0] == "quit" || fields[0] == "exit" {
			return nil
		}
		if err := run(db, fields); err != nil {
			fmt.Fprintf(os.Stderr, "kv: %v\n", err)
		}
	}
	return scanner.Err()
}

func human(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGT"[exp])
}
