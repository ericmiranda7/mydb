package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
)

func run(dbfile io.ReadSeeker, indx map[string]int64) {
	http.HandleFunc("/get/", GetHandler(dbfile, indx))

	middlewared := LoggingMiddleware(http.DefaultServeMux)

	err := http.ListenAndServe(":8090", middlewared)
	if err != nil {
		log.Fatalln(err)
	}
}

// todo(): sethandler
func GetHandler(dbfile io.ReadSeeker, indx map[string]int64) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		key, _ := strings.CutPrefix(req.URL.String(), "/get/")

		ofst, err := OffsetOf(key, indx)
		if err == nil {
			w.WriteHeader(http.StatusNotFound)
		}
		val, err := Get(dbfile, key, ofst)
		if err != nil {
			log.Fatalln(err)
		}

		_, err = fmt.Fprintf(w, "val for %v is %v", key, val)
		if err != nil {
			log.Fatalln(err)
		}
	}
}

func LoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Println("Received request: ", r.URL)
		next.ServeHTTP(w, r)
	})
}
