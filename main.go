package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"
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
	indx := populateIndex(dbfile)

	switch cmd {
	case "set":
		{
			key := os.Args[2]
			val := os.Args[3]
			println(val)
			Set(dbfile, key, val, indx)
		}
	case "get":
		{
			key := os.Args[2]
			ofst, err := OffsetOf(key, indx)
			if err != nil {
				fmt.Println("no such key")
				return
			}

			val, err := Get(dbfile, key, ofst)
			if err != nil {
				log.Fatalln(err)
			}
			fmt.Println("val: ", val)
		}
	case "http":
		{
			run(dbfile, indx)
		}
	}
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

func Set(dbfile *os.File, key string, val string, indx map[string]int64) {
	wrote := Persist(dbfile, key, val)

	dbStat, err := dbfile.Stat()
	if err != nil {
		log.Fatalln(err)
	}
	latestOffset := dbStat.Size() - wrote
	indx[key] = latestOffset

	println("latest offset: ", latestOffset)
	if latestOffset >= 100 {
		// write segment to disk
		CreateSegment(dbfile, indx)
	}
}

func CreateSegment(dbfile *os.File, indx map[string]int64) {
	// write out old log
	nowTime := time.Now()
	segname := fmt.Sprintf("seg%v%v%v%v%v%v",
		nowTime.Year(), int(nowTime.Month()), nowTime.Day(), nowTime.Hour(), nowTime.Minute(), nowTime.Second())
	newSeg, err := os.Create(segname)

	_, err = dbfile.Seek(0, 0)
	if err != nil {
		log.Fatalln("cant seek ", err)
	}

	_, err = io.Copy(newSeg, dbfile)
	if err != nil {
		log.Fatalln("cant copy, ", err)
	}

	newDb, err := os.Create("db")
	if err != nil {
		log.Fatalln("cant create, ", err)
	}
	// write to new segment
	*dbfile = *newDb

	// write out segment index
	ifile, err := os.Create(fmt.Sprintf("indx_%v", segname))
	if err != nil {
		log.Fatalln(err)
	}
	for k, v := range indx {
		_, err = ifile.WriteString(fmt.Sprintf("%v %v\n", k, v))
		if err != nil {
			log.Fatalln(err)
		}
	}
}

func OffsetOf(key string, indx map[string]int64) (int64, error) {
	ofst, exists := indx[key]
	if !exists {
		return -1, errors.New("key does not exist")
	}

	return ofst, nil
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

func Persist(dbfile io.Writer, key string, val string) int64 {
	line := key + "," + val + "\n"
	written, err := dbfile.Write([]byte(line))
	if err != nil {
		log.Fatalln("brew", err)
	}

	return int64(written)
}
