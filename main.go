package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	cmd := os.Args[1]
	dbfile, err := os.OpenFile("db", os.O_CREATE|os.O_APPEND|os.O_RDWR, 0646)
	if err != nil {
		log.Fatalln(err)
	}

	switch cmd {
	case "set":
		{
			key := os.Args[2]
			val := os.Args[3]
			Set(dbfile, key, val)
		}
	case "get":
		{
			// get
			key := os.Args[2]
			val, err := Get(dbfile, key)
			if err != nil {
				log.Fatalln(err)
			}
			fmt.Println("val: ", val)
		}
	case "http":
		{
			// run in server mode
			http.HandleFunc("/get/", GetHandler(dbfile))

			middlewared := LoggingMiddleware(http.DefaultServeMux)

			err := http.ListenAndServe(":8090", middlewared)
			if err != nil {
				log.Fatalln(err)
			}
		}
	}
}

func LoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Println("Received request: ", r.URL)
		next.ServeHTTP(w, r)
	})
}

func GetHandler(dbfile io.ReadSeeker) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		key, _ := strings.CutPrefix(req.URL.String(), "/get/")

		val, err := Get(dbfile, key)
		if err != nil {
			log.Fatalln(err)
		}

		_, err = fmt.Fprintf(w, "val for %v is %v", key, val)
		if err != nil {
			log.Fatalln(err)
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

func Get(dbfile io.ReadSeeker, key string) (string, error) {
	_, err := dbfile.Seek(0, 0)
	if err != nil {
		return "", err
	}
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
