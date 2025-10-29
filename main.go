package main

import (
	"bufio"
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
	indx := populateIndex(dbfile)

	switch cmd {
	case "set":
		{
			key := os.Args[2]
			val := os.Args[3]
			wrote := Set(dbfile, key, val)
			dbStat, err := dbfile.Stat()
			if err != nil {
				log.Fatalln(err)
			}
			indx[key] = dbStat.Size() - wrote - 1
		}
	case "get":
		{
			key := os.Args[2]
			val, err := Get(dbfile, key, indx[key])
			if err != nil {
				log.Fatalln(err)
			}
			fmt.Println("val: ", val)
		}
	case "http":
		{
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

		// todo(proper offset)
		val, err := Get(dbfile, key, 0)
		if err != nil {
			log.Fatalln(err)
		}

		_, err = fmt.Fprintf(w, "val for %v is %v", key, val)
		if err != nil {
			log.Fatalln(err)
		}
	}
}

func Set(dbfile io.Writer, key string, val string) int64 {
	// set
	line := key + "," + val + "\n"
	written, err := dbfile.Write([]byte(line))
	if err != nil {
		log.Fatalln("brew", err)
	}

	return int64(written)
}

func Get(dbfile io.ReadSeeker, key string, offset int64) (string, error) {
	_, err := dbfile.Seek(offset, io.SeekStart)
	if err != nil {
		return "", err
	}
	sc := bufio.NewScanner(dbfile)
	sc.Scan()
	line := sc.Text()
	return line[len(key)+1:], nil
}

func populateIndex(dbfile io.ReadSeeker) map[string]int64 {
	res := map[string]int64{}
	_, err := dbfile.Seek(0, io.SeekStart)
	if err != nil {
		log.Fatalln(err)
	}

	sc := bufio.NewScanner(dbfile)
	var offset int64 = 0
	for sc.Scan() {
		line := sc.Text()

		key := line[0:strings.Index(line, ",")]
		res[key] = offset
		offset += int64(len(line) + 1)
	}
	return res
}
