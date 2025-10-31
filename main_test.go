package main

import (
	"bytes"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
)

func TestSet(t *testing.T) {
	key := "foo"
	val := "23"
	dbfile := bytes.Buffer{}

	Persist(&dbfile, key, val)

	if strings.Compare(dbfile.String(), "foo,23\n") != 0 {
		fmt.Println(dbfile.String())
		t.Fail()
	}
}

func TestGetSuccess(t *testing.T) {
	key := "foo"
	exp := "24"
	dbfile := bytes.NewReader([]byte("foo,24\nbar,knob\n"))

	res, _ := Get(dbfile, key, 0)

	if strings.Compare(res, exp) != 0 {
		t.Fatalf("got %v want %v", res, exp)
	}
}

func TestOffsetSuccess(t *testing.T) {
	key := "foo"
	indx := map[string]int64{"foo": 42}

	_, err := OffsetOf(key, indx)
	if err != nil {
		t.Fail()
	}
}

func TestOffsetFail(t *testing.T) {
	key := "foos"
	indx := map[string]int64{"foo": 42}

	_, err := OffsetOf(key, indx)
	if err == nil {
		t.Fail()
	}
}

func TestPopulateIndex(t *testing.T) {
	dbfile := bytes.NewReader([]byte("foo,24\nbar,knob\nfoo,42\n"))

	indx := populateIndex(dbfile)

	if indx["foo"] != 16 || indx["bar"] != 7 {
		t.Fatalf("got %v want %v", indx, "15,7")
	}
}

func TestGetLatestIntegration(t *testing.T) {
	key := "foo"
	exp := "42"
	buf := bytes.Buffer{}
	_ = Persist(&buf, key, "24")
	_ = Persist(&buf, key, exp)

	rdr := bytes.NewReader(buf.Bytes())
	res, _ := Get(rdr, key, 7)

	if strings.Compare(res, exp) != 0 {
		t.Fatalf("got %v want %v", res, exp)
	}
}

func FuzzSet(f *testing.F) {
	keys := []string{"foo", "bar", "baz", "memtable", "cart-v4", "eric"}
	vals := []string{"foo", "23", "986 423 124", "mip map", "kladsf;921##$$", "#23 clown drive california"}

	for i := 0; i < len(keys); i++ {
		f.Add(keys[i], vals[i])
	}

	f.Fuzz(func(t *testing.T, key string, val string) {
		dbfile := bytes.Buffer{}

		Persist(&dbfile, key, val)

		if strings.Compare(dbfile.String(), fmt.Sprintf("%v,%v\n", key, val)) != 0 {
			fmt.Println(dbfile.String())
			t.Fail()
		}
	})
}

func FuzzGetSet(f *testing.F) {
	keys := []string{"foo", "bar", "baz", "memtable", "cart-v4", "eric"}
	vals := []string{"foo", "23", "986 423 124", "mip map", "kladsf;921##$$", "#23 clown drive california"}

	for i := 0; i < len(keys); i++ {
		f.Add(keys[i], vals[i])
	}

	f.Fuzz(func(t *testing.T, key string, val string) {
		myKv := map[string]string{}
		indx := map[string]int64{}
		dbfile, err := os.CreateTemp("", "testdb")
		if err != nil {
			t.Fatal("cannot create temp file")
		}
		defer dbfile.Close()
		defer os.Remove(dbfile.Name())

		r, _ := regexp.Compile("[\\n\\r]")

		if !r.MatchString(key) && !r.MatchString(val) {
			myKv[key] = val
			Set(dbfile, key, val, indx)
		}

		// test all keys
		for k, v := range myKv {
			got, err := Get(dbfile, k, indx[k])
			if err != nil {
				t.Fatal("br", err)
			}

			if got != v {
				t.Fatalf("got %v want %v", got, v)
			}
		}
	})
}
