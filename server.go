package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	"git.target.com/eric.miranda/mydb/v2/src/engine"
)

func run(nob *engine.Nob) {
	http.HandleFunc("/get/", GetHandler(nob))
	http.HandleFunc("/set/", SetHandler(nob))

	middlewared := LoggingMiddleware(http.DefaultServeMux)

	err := http.ListenAndServe(":8090", middlewared)
	if err != nil {
		log.Fatalln(err)
	}
}

func SetHandler(nob *engine.Nob) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key, _ := strings.CutPrefix(r.URL.String(), "/set/")
		bb, err := io.ReadAll(r.Body)
		if err != nil {
			log.Fatalln(err)
		}
		val := string(bb)
		nob.Set(key, val)
		w.WriteHeader(http.StatusCreated)
	}
}

func GetHandler(nob *engine.Nob) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		key, _ := strings.CutPrefix(req.URL.String(), "/get/")

		ofst, err := nob.OffsetOf(key)
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		val, err := nob.Get(key, ofst)
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
