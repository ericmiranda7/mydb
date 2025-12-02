package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"git.target.com/eric.miranda/mydb/v2/src/engine"
)

// todo(): support newlines in key/val?
// todo(): handle concurrency
func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	rootDir := "./output"
	for _, kv := range os.Environ() {
		kva := strings.Split(kv, "=")

		if kva[0] == "ROOT_DIR" {
			rootDir = kva[1]
		}
	}
	nob := engine.NewNob(rootDir)

	cmd := os.Args[1]
	switch cmd {
	case "set":
		{
			key := os.Args[2]
			val := os.Args[3]
			fmt.Printf("SET %v %v\n", key, val)
			nob.Set(key, val)
		}
	case "get":
		{
			key := os.Args[2]

			val, err := nob.Get(key)
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
