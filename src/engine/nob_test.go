package engine

import (
	"fmt"
	"os"
	"testing"
)

func setupDbFile(t *testing.T) *os.File {
	dbfile, err := os.Create(fmt.Sprintf("%v/%v", t.TempDir(), "dbfile"))
	if err != nil {
		t.Fatal(err)
	}
	return dbfile
}

func TestSetAndGet(t *testing.T) {
	key := "foo"
	val := "23"
	dbfile := setupDbFile(t)
	nob := NewNob(dbfile)

	nob.Set(key, val)
	res, _ := nob.Get(key)

	if res != val {
		t.Fatalf("got %v want %v", res, val)
	}

}

func TestSetAndGetFail(t *testing.T) {
	key := "foo"
	val := "23"
	dbfile := setupDbFile(t)
	nob := NewNob(dbfile)

	nob.Set(key, val)
	_, err := nob.Get("bar")

	if err == nil {
		t.Fatal("was expecting err")
	}

}

func TestPopulateIndex(t *testing.T) {
	dbfile := setupDbFile(t)
	nob := NewNob(dbfile)
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
		dbfile := setupDbFile(t)
		nob := NewNob(dbfile)

		nob.Set(key, val)
		res, _ := nob.Get(key)

		if res != val {
			t.Fatalf("got %v want %v", res, val)
		}
	})
}

//func TestSegmentation(t *testing.T) {
//	key := "ottff"
//	val := "stkerjfnxkfalgktxa"
//
//	for i := range 5 {
//
//	}
//}
//
// segmentation test
// 1. populate k,v of 25bytes each
// expect load / 5 == ls("seg") | count
