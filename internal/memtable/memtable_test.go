package memtable

import "testing"

func TestEntriesComeBackInKeyOrder(t *testing.T) {
	table := New()
	for _, key := range []string{"delta", "alpha", "charlie", "bravo"} {
		table.Put(key, []byte(key))
	}

	entries := table.Entries()
	for i := 1; i < len(entries); i++ {
		if entries[i-1].Key >= entries[i].Key {
			t.Fatalf("out of order: %s then %s", entries[i-1].Key, entries[i].Key)
		}
	}
}

func TestADeleteStoresATombstoneRatherThanRemovingTheKey(t *testing.T) {
	// Removing it would be wrong: an older value may still sit in a file on
	// disk, and dropping the marker lets it come back.
	table := New()
	table.Put("key", []byte("value"))
	table.Delete("key")

	entry, ok := table.Get("key")
	if !ok {
		t.Fatal("the table must still know about a deleted key")
	}
	if !entry.Deleted {
		t.Fatal("expected a tombstone")
	}
	if table.Len() != 1 {
		t.Fatalf("len is %d", table.Len())
	}
}

func TestWritingOverAKeyReplacesItRatherThanAccumulating(t *testing.T) {
	table := New()
	for i := 0; i < 10; i++ {
		table.Put("key", []byte("0123456789"))
	}

	if table.Len() != 1 {
		t.Fatalf("len is %d", table.Len())
	}
	if table.Bytes() > 100 {
		t.Fatalf("accounted %d bytes for one key rewritten ten times", table.Bytes())
	}
}

func TestRangeRespectsItsBounds(t *testing.T) {
	table := New()
	for _, key := range []string{"a", "b", "c", "d", "e"} {
		table.Put(key, nil)
	}

	got := table.Range("b", "d")
	if len(got) != 2 || got[0].Key != "b" || got[1].Key != "c" {
		t.Fatalf("[from, to) should give b and c, got %+v", got)
	}
	if len(table.Range("", "")) != 5 {
		t.Fatal("empty bounds mean everything")
	}
}

func TestByteAccountingGrowsAndShrinksWithValues(t *testing.T) {
	table := New()
	table.Put("key", make([]byte, 1000))
	big := table.Bytes()

	table.Put("key", make([]byte, 10))
	if table.Bytes() >= big {
		t.Fatalf("replacing a large value with a small one should shrink: %d -> %d",
			big, table.Bytes())
	}
}
