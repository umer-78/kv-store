package bloom

import (
	"fmt"
	"testing"
)

func TestAFilterNeverSaysAbsentForAKeyItHolds(t *testing.T) {
	// The only property that must hold without exception. A false positive costs
	// one wasted disk read; a false negative loses data, silently.
	filter := New(5000, 0.01)
	for i := 0; i < 5000; i++ {
		filter.Add([]byte(fmt.Sprintf("key-%d", i)))
	}
	for i := 0; i < 5000; i++ {
		if !filter.MayContain([]byte(fmt.Sprintf("key-%d", i))) {
			t.Fatalf("false negative on key-%d", i)
		}
	}
}

func TestTheMeasuredFalsePositiveRateMatchesTheTargetedOne(t *testing.T) {
	// The first version derived the second hash by running FNV again with a
	// salt. That keeps almost all the correlation with the first, and the
	// measured rate came out at 4.7% against a 1% target. Passing the first hash
	// through splitmix64's finaliser fixed it. This test is what would catch
	// that regression.
	const keys, target = 2000, 0.01
	filter := New(keys, target)
	for i := 0; i < keys; i++ {
		filter.Add([]byte(fmt.Sprintf("key-%d", i)))
	}

	falsePositives := 0
	const probes = 20000
	for i := keys; i < keys+probes; i++ {
		if filter.MayContain([]byte(fmt.Sprintf("key-%d", i))) {
			falsePositives++
		}
	}

	rate := float64(falsePositives) / probes
	if rate > target*2 {
		t.Fatalf("false positive rate %.3f%% against a %.1f%% target — the two hashes "+
			"are not independent enough", rate*100, target*100)
	}
}

func TestATighterTargetCostsMoreSpace(t *testing.T) {
	loose := New(1000, 0.1)
	tight := New(1000, 0.001)

	if tight.SizeBytes() <= loose.SizeBytes() {
		t.Fatalf("1%% in 1000: loose %d bytes, tight %d", loose.SizeBytes(), tight.SizeBytes())
	}
}

func TestAnEmptyFilterContainsNothing(t *testing.T) {
	filter := New(100, 0.01)
	if filter.MayContain([]byte("anything")) {
		t.Fatal("an empty filter should answer no")
	}
}

func TestAFilterSurvivesARoundTripThroughBytes(t *testing.T) {
	filter := New(500, 0.01)
	for i := 0; i < 500; i++ {
		filter.Add([]byte(fmt.Sprintf("key-%d", i)))
	}

	restored, ok := Load(filter.Bytes())
	if !ok {
		t.Fatal("failed to load a filter it just wrote")
	}
	for i := 0; i < 500; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		if !restored.MayContain(key) {
			t.Fatalf("false negative after reload on key-%d", i)
		}
	}
}

func TestATruncatedFilterIsRejectedRatherThanMisread(t *testing.T) {
	filter := New(100, 0.01)
	filter.Add([]byte("key"))
	data := filter.Bytes()

	for _, short := range [][]byte{nil, data[:2], data[:4]} {
		if _, ok := Load(short); ok {
			t.Fatalf("loaded a %d-byte filter", len(short))
		}
	}
}

func TestAbsurdParametersAreClampedRatherThanPanicking(t *testing.T) {
	for _, filter := range []*Filter{New(0, 0.01), New(-5, 0.01), New(100, 0), New(100, 2)} {
		filter.Add([]byte("key"))
		if !filter.MayContain([]byte("key")) {
			t.Fatal("a clamped filter must still work")
		}
	}
}
