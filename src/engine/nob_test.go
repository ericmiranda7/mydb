package engine

import (
	"maps"
	"os"
	"path"
	"regexp"
	"testing"
)

// working dir of file engine
func getNob(dir string) *Nob {
	dbfile := setupDbFile(dir)
	return NewNob(dbfile, dir)
}
func getCustomNob(dbfile *os.File, dir string) *Nob {
	return NewNob(dbfile, dir)
}

func setupDbFile(dir string) *os.File {
	dbfile, _ := os.Create(path.Join(dir, "db"))
	return dbfile
}

func TestSetAndGet(t *testing.T) {
	key := "foo"
	val := "23"
	nob := getNob(t.TempDir())

	nob.Set(key, val)
	res, _ := nob.Get(key)

	if res != val {
		t.Fatalf("got %v want %v", res, val)
	}

}

func TestSetAndGetFail(t *testing.T) {
	key := "foo"
	val := "23"
	nob := getNob(t.TempDir())

	nob.Set(key, val)
	_, err := nob.Get("bar")

	if err == nil {
		t.Fatal("was expecting err")
	}

}

func TestPopulateIndex(t *testing.T) {
	nob := getNob(t.TempDir())
	nob.Set("foo", "24")
	nob.Set("bar", "knob")
	nob.Set("foo", "42")

	indx := nob.populateIndex()

	if indx["foo"] != 16 || indx["bar"] != 7 {
		t.Fatalf("got %v want %v", indx, "15,7")
	}
}

func FuzzGetSet(f *testing.F) {
	keys := []string{"foo", "bar", "baz", "memtable", "cart-v4", "eric"}
	vals := []string{"foo", "23", "986 423 124", "mip map", "kladsf;921##$$", "#23 clown drive california"}

	for i := 0; i < len(keys); i++ {
		f.Add(keys[i], vals[i])
	}

	f.Fuzz(func(t *testing.T, key string, val string) {
		nob := getNob(t.TempDir())

		nob.Set(key, val)
		res, _ := nob.Get(key)

		if res != val {
			t.Fatalf("got %v want %v", res, val)
		}
	})
}

func TestSegmentation(t *testing.T) {
	// 20 bytes
	key := "ottff"
	val := "stkerjfnxkfalgktxa"
	dir := t.TempDir()
	nob := getNob(dir)

	for _ = range 5 {
		nob.Set(key, val)
	}

	s, _ := nob.dbfile.Stat()

	if s.Size() != 0 {
		t.Fatal("should've been 0")
	}

	dirs, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}

	c := 0
	for _, dir := range dirs {
		println(dir.Name())
		rxp, _ := regexp.Compile("^seg.*")
		if rxp.MatchString(dir.Name()) {
			c += 1
		}
	}

	println("c is ", c)
	if c != 1 {
		t.Fatal("shouldve been a segment file")
	}
}

//
//func TestMerge(t *testing.T) {
//	f1, _ := os.Open("test-data/seg1")
//	f2, _ := os.Open("test-data/seg2")
//	merge(f1, f2)
//}

func TestCompact(t *testing.T) {
	f1, _ := os.Open("test-data/seg1")
	f2, _ := os.Open("test-data/seg2")
	tdir := t.TempDir()
	nob := getNob(tdir)

	res := nob.compact(f1, f2)
	exp := map[string]string{
		"foo":     "latest",
		"baz":     "asolatest",
		"finbean": "82",
	}

	if !maps.Equal(res, exp) {
		t.Fatalf("got %v want %v", res, exp)
	}
}

//func TestMergeCompact(t *testing.T) {
//	f1 := open(seg1)
//	f2 := open(seg2)
//
//	mergeCompact(f1, f2)
//
//	// expect dir contains compacted_file, compacted_indx
//	// expect compacted_file == {foo:42, bar:23}
//	// expect compacted_indx to be {foo: boff, bar: boff}
//	// expect f1, f2 to be deleted
//}
