package main

import (
	"bufio"
	"errors"
	"io"
	"log"
	"os"
	"strings"
)

func main() {
	cmd := os.Args[1]
	key := os.Args[2]
	dbfile, err := os.OpenFile("db", os.O_CREATE|os.O_APPEND|os.O_RDWR, 0646)
	if err != nil {
		log.Fatalln(err)
	}

	switch cmd {
	case "set":
		{
			val := os.Args[3]
			Set(dbfile, key, val)
		}
	case "get":
		{
			// get
			Get(dbfile, key)
		}
	}
}

func Set(dbfile io.Writer, key string, val string) {
	// set
	line := key + "," + val + "\n"
	_, err := dbfile.Write([]byte(line))
	if err != nil {
		log.Fatalln("brew", err)
	}
}

func Get(dbfile io.Reader, key string) (string, error) {
	sc := bufio.NewScanner(dbfile)
	for sc.Scan() {
		line := sc.Text()
		ind := strings.Index(line, key)
		if ind != -1 {
			return line[ind+len(key)+1:], nil
		}
	}

	return "", errors.New("key does not exist")
}
