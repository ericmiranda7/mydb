package engine

import (
	"fmt"
	"io"
	"log"
	"maps"
	"os"
	"path"
	"regexp"
	"slices"
	"strings"
	"testing"
)

func TestSet(t *testing.T) {
	key := "foo"
	val := "23"
	nob := getNob(t.TempDir())

	nob.Set(key, val)
	_, err := nob.dbfile.Seek(0, 0)
	ce(err)
	got, err := io.ReadAll(nob.dbfile)
	ce(err)

	want := "foo 23\n"

	if string(got) != want {
		t.Fatalf("got %v want %v", got, want)
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

	indx := getIndexFrom(nob.dbfile)

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
		rxp, _ := regexp.Compile("^seg.*")
		if rxp.MatchString(dir.Name()) {
			c += 1
		}
	}

	if c != 1 {
		t.Fatal("shouldve been a segment file")
	}
}

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

func TestMergeCompact(t *testing.T) {
	tdir := t.TempDir()
	setupTestFile("test-data", tdir)
	nob := getNob(tdir)

	nob.mergeCompact()

	files, _ := os.ReadDir(tdir)

	// expect dir contains compacted_file, compacted_indx
	var containsFile bool = false
	for _, f := range files {
		if strings.Contains(f.Name(), "compacted_file") {
			containsFile = true
		}
	}
	if containsFile != true {
		t.Fatalf("no compacted_file")
	}
	cf, err := os.Open(path.Join(tdir, "compacted_file"))
	ce(err)
	rdr, err := io.ReadAll(cf)
	ce(err)
	got := convStrToMap(string(rdr))
	want := map[string]string{
		"baz":     "asolatest",
		"finbean": "82",
		"foo":     "latest",
	}
	if !maps.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}

	// todo(): index file creation, tests
	// expect compacted_indx to be {foo: boff, bar: boff}
	fmt.Println("file is", string(rdr))
	ci, err := os.Open(path.Join(tdir, "indx_compacted"))
	ce(err)
	b, err := io.ReadAll(ci)
	got = convStrToMap(string(b))
	want = map[string]string{
		"baz":     "92",
		"foo":     "85",
		"finbean": "42",
	}
	//if !maps.Equal(got, want) {
	//	// todo(FIRST): figure out how to validate essentially application-duplicated code (getIndexFrom)
	//	// should i write the identical app function as a test func? what do people do normally?
	//	t.Fatalf("got %v, want %v", got, want)
	//}
	// expect f1, f2 to be deleted
}

func convStrToMap(str string) map[string]string {
	res := map[string]string{}
	kvs := strings.Split(strings.Trim(str, "\n"), "\n")
	for _, kv := range kvs {
		skv := strings.Split(kv, " ")
		res[skv[0]] = skv[1]
	}

	return res
}

func TestGetOrderedFiles(t *testing.T) {
	inpDir := "test-data"
	nob := getNob(inpDir)

	res := nob.getOrderedSegFiles()
	exp := []string{path.Join(inpDir, "seg1"), path.Join(inpDir, "seg2")}

	if !slices.Equal(res, exp) {
		t.Fatalf("got %v want %v", res, exp)
	}
}

// test helpers
func setupTestFile(srcdir, tdir string) {
	files, _ := os.ReadDir(srcdir)
	for _, f := range files {
		if strings.Contains(f.Name(), "seg") {
			copyFile(path.Join(tdir, f.Name()), path.Join(srcdir, f.Name()))
		}
	}
}

func copyFile(dst, src string) {
	sf, _ := os.Open(src)
	data, err := io.ReadAll(sf)
	ce(err)
	err = os.WriteFile(dst, data, 0644)
	ce(err)
}

func ce(err error) {
	if err != nil {
		log.Fatalln(err)
	}
}

func getNob(dir string) *Nob {
	dbfile := setupDbFile(dir)
	return NewNob(dbfile, dir)
}

func setupDbFile(dir string) *os.File {
	dbfile, _ := os.Create(path.Join(dir, "db"))
	return dbfile
}
