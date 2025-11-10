package main

import (
	"fmt"
	"log"
	"os"

	"git.target.com/eric.miranda/mydb/v2/src/engine"
)

// todo(): support newlines in key/val?
// todo(): handle concurrency
func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	cmd := os.Args[1]
	dbfile, err := os.OpenFile("db", os.O_CREATE|os.O_APPEND|os.O_RDWR, 0646)
	if err != nil {
		log.Fatalln(err)
	}
	nob := engine.NewNob(dbfile)

	switch cmd {
	case "set":
		{
			key := os.Args[2]
			val := os.Args[3]
			println(val)
			nob.Set(key, val)
		}
	case "get":
		{
			key := os.Args[2]
			ofst, err := nob.OffsetOf(key)
			if err != nil {
				fmt.Println("no such key")
				return
			}

			val, err := nob.Get(key, ofst)
			if err != nil {
				log.Fatalln(err)
			}
			fmt.Println("val: ", val)
		}
	case "http":
		{
			run(nob)
		}
	}
}
