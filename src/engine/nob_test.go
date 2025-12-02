package engine

import (
	"io"
	"log"
	"maps"
	"os"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func TestPopulateIndex(t *testing.T) {
	nob := getNob(t.TempDir())
	nob.Set("foo", "24")
	nob.Set("bar", "knob")
	nob.Set("foo", "42")

	indx := buildIndexOf(nob.dbfile)

	if indx["foo"] != 16 || indx["bar"] != 7 {
		t.Fatalf("got %v want %v", indx, "15,7")
	}
}

// INTEGRATION TEST
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

func TestSetSegmentation(t *testing.T) {
	// 20 bytes
	key := "ottff"
	val := "stkerjfnxkfalgktxadjklxad"
	nob := getNob(t.TempDir())

	// act
	for _ = range 5 {
		nob.Set(key, val)
	}

	// dbfile is empty
	s, _ := nob.dbfile.Stat()
	if s.Size() != 0 {
		t.Fatal("should've been 0")
	}

	// todo(): fix
	// memtable is cleared
	//if len(nob.memtable) != 0 {
	//	t.Fatal("memtable should be emptied")
	//}

	dirs, err := os.ReadDir(nob.rootDir)
	if err != nil {
		t.Fatal(err)
	}
	var segCreated bool
	for _, dir := range dirs {
		rxp, _ := regexp.Compile("^seg.*")
		if rxp.MatchString(dir.Name()) {
			segCreated = true
		}
	}

	if !segCreated {
		t.Fatal("shouldve been a segment file")
	}
}

func TestCompact(t *testing.T) {
	f1, _ := os.Open("test-data/seg_1")
	f2, _ := os.Open("test-data/seg_2")
	tdir := t.TempDir()
	nob := getNob(tdir)

	res, ok := nob.compact(f1, f2)
	exp := map[string]string{
		"foo":     "latest",
		"baz":     "asolatest",
		"finbean": "82",
	}

	if !ok {
		t.Fatalf("wanted files in dir")
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
	var containsCompactedSeg, containsCompactedIndx = false, false
	var compactedSeg *os.File
	var compactedIndx *os.File
	//var compactedIndx *os.File
	for _, f := range files {
		matchedSeg, err := regexp.MatchString("^compacted_\\d+", f.Name())
		if err != nil {
			t.Fatal(err)
		}
		if matchedSeg {
			containsCompactedSeg = true
			compactedSeg, err = os.Open(path.Join(tdir, f.Name()))
			if err != nil {
				t.Fatal(err)
			}
		}
		matchdIndx, err := regexp.MatchString("^indx_compacted_\\d+", f.Name())
		if matchdIndx {
			containsCompactedIndx = true
			compactedIndx, err = os.Open(path.Join(nob.rootDir, f.Name()))
			if err != nil {
				log.Fatalln(err)
			}
		}
	}

	if (containsCompactedSeg && containsCompactedIndx) != true {
		t.Fatalf("no compacted file")
	}

	// expect compacted seg is correct
	rdr, err := io.ReadAll(compactedSeg)
	if err != nil {
		log.Fatalln(err)
	}
	got := convStrToMap(string(rdr))
	want := map[string]string{
		"baz":     "asolatest",
		"finbean": "82",
		"foo":     "latest",
	}
	if !maps.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}

	// expect compacted memtable is correct
	b, err := io.ReadAll(compactedIndx)
	if err != nil {
		log.Fatalln(err)
	}
	got = convStrToMap(string(b))
	for k, v := range got {
		offst, err := strconv.Atoi(v)
		if err != nil {
			t.Fatal(err)
		}
		_, err = compactedSeg.Seek(int64(offst), 0)
		if err != nil {
			t.Fatal(err)
		}
		readUpto := len(k)
		b := make([]byte, readUpto)
		_, err = compactedSeg.Read(b)
		if err != nil {
			t.Fatal(err)
		}
		if string(b) != k {
			t.Fatal(string(b))
		}

	}

	// expect f1, f2 to be deleted
	dfiles, err := os.ReadDir(tdir)
	if err != nil {
		log.Fatalln(err)
	}
	for _, f := range dfiles {
		if strings.Contains(f.Name(), "seg") {
			t.Fatal("seg:", f.Name(), "shouldnt be here")
		}
	}
}

func TestGetOrderedFiles(t *testing.T) {
	inpDir := "test-data"
	nob := getNob(inpDir)

	res := nob.getOrderedSegFiles()
	exp := []string{path.Join(inpDir, "seg_1"), path.Join(inpDir, "seg_2")}

	if !slices.Equal(res, exp) {
		t.Fatalf("got %v want %v", res, exp)
	}
}

func TestOldKeyFromSegment(t *testing.T) {
	nob := getNob(t.TempDir())
	treasureKey := "x"
	treasureValue := "marksTheSpot"

	junkKey := "junk"
	junkValue := "values"

	// plant
	nob.Set(treasureKey, treasureValue)
	// distract
	for _ = range 503 {
		nob.Set(junkKey, junkValue)
	}

	got, err := nob.Get(treasureKey)
	if err != nil {
		t.Fatal(err)
	}

	if got != treasureValue {
		t.Fatalf("got %v want %v", got, treasureValue)
	}
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
	if err != nil {
		log.Fatalln(err)
	}
	err = os.WriteFile(dst, data, 0644)
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
