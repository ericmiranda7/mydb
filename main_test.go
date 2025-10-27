package main

import (
	"bytes"
	"fmt"
	"log"
	"strings"
	"testing"
)

func TestSet(t *testing.T) {
	key := "foo"
	val := "23"
	dbfile := bytes.Buffer{}

	Set(&dbfile, key, val)

	if strings.Compare(dbfile.String(), "foo,23\n") != 0 {
		fmt.Println(dbfile.String())
		t.Fail()
	}
}

func TestGetSuccess(t *testing.T) {
	key := "foo"
	exp := "24"
	dbfile := bytes.Buffer{}
	dbfile.WriteString("foo,24\nbar,knob\n")

	res, _ := Get(&dbfile, key)

	if strings.Compare(res, exp) != 0 {
		t.Fatalf("got %v want %v", res, exp)
	}
}

func TestGetFail(t *testing.T) {
	key := "baz"
	dbfile := bytes.Buffer{}
	dbfile.WriteString("foo,24\nbar,knob\n")

	_, err := Get(&dbfile, key)

	if err == nil {
		log.Fatalln("expected error!")
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

		Set(&dbfile, key, val)

		if strings.Compare(dbfile.String(), fmt.Sprintf("%v,%v\n", key, val)) != 0 {
			fmt.Println(dbfile.String())
			t.Fail()
		}
	})
}
