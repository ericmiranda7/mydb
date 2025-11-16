package engine

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"regexp"
	"sort"
	"strings"
	"time"
)

type Nob struct {
	dbfile  *os.File
	indx    map[string]int64
	rootDir string
}

func NewNob(dbfile *os.File, rootDir string) *Nob {
	n := Nob{dbfile: dbfile, indx: make(map[string]int64), rootDir: rootDir}
	n.populateIndex()
	return &n
}

func (nob *Nob) Set(key string, val string) {
	wrote := nob.persist(key, val)

	dbStat, err := nob.dbfile.Stat()
	if err != nil {
		log.Fatalln(err)
	}
	latestOffset := dbStat.Size() - wrote
	nob.indx[key] = latestOffset

	println("latest offset: ", latestOffset)
	if latestOffset >= 100 {
		// write segment to disk
		nob.createSegment()
	}
}

func (nob *Nob) Get(key string) (string, error) {
	ofst, err := nob.offsetOf(key)
	if err != nil {
		return "", err
	}
	_, err = nob.dbfile.Seek(ofst, io.SeekStart)
	if err != nil {
		return "", err
	}
	sc := bufio.NewScanner(nob.dbfile)
	sc.Scan()
	line := sc.Text()
	return line[len(key)+1:], nil
}

func (nob *Nob) mergeCompact() {
	stringFiles := nob.getOrderedSegFiles()
	var files []*os.File
	for _, f := range stringFiles {
		of, _ := os.Open(f)
		files = append(files, of)
	}
	compact := nob.compact(files...)
	writePath := path.Join(nob.rootDir, "compacted_file")
	wf, err := os.Create(writePath)
	if err != nil {
		log.Fatalln(err)
	}
	for k, v := range compact {
		_, _ = io.WriteString(wf, fmt.Sprintf("%v %v\n", k, v))
	}
	//	todo(): writeFile(indexOf(compact))
}

func (nob *Nob) getOrderedSegFiles() []string {
	dirFiles, _ := os.ReadDir(nob.rootDir)
	sort.Slice(dirFiles, func(i, j int) bool {
		return dirFiles[i].Name() < dirFiles[j].Name()
	})

	var res []string
	for _, file := range dirFiles {
		if ok, _ := regexp.MatchString("^seg", file.Name()); ok {
			res = append(res, path.Join(nob.rootDir, file.Name()))
		}
	}
	return res
}

func (nob *Nob) compact(files ...*os.File) map[string]string {
	res := map[string]string{}

	for _, f := range files {
		sc := bufio.NewScanner(f)

		for sc.Scan() {
			kv := strings.Split(sc.Text(), " ")
			res[kv[0]] = kv[1]
		}
	}

	return res
}

func (nob *Nob) persist(key string, val string) int64 {
	line := key + "," + val + "\n"
	written, err := nob.dbfile.Write([]byte(line))
	if err != nil {
		log.Fatalln("brew", err)
	}

	return int64(written)
}

func (nob *Nob) createSegment() {
	// write out old log
	nowTime := time.Now()
	segname := fmt.Sprintf("seg%v%v%v%v%v%v",
		nowTime.Year(), int(nowTime.Month()), nowTime.Day(), nowTime.Hour(), nowTime.Minute(), nowTime.Second())
	newSeg, err := os.Create(path.Join(nob.rootDir, segname))

	_, err = nob.dbfile.Seek(0, 0)
	if err != nil {
		log.Fatalln("cant seek ", err)
	}

	_, err = io.Copy(newSeg, nob.dbfile)
	if err != nil {
		log.Fatalln("cant copy, ", err)
	}

	newDb, err := os.Create(path.Join(nob.rootDir, "db"))
	if err != nil {
		log.Fatalln("cant create, ", err)
	}
	// write to new segment
	nob.dbfile = newDb

	// write out segment index
	ifile, err := os.Create(path.Join(nob.rootDir, fmt.Sprintf("indx_%v", segname)))
	if err != nil {
		log.Fatalln(err)
	}
	for k, v := range nob.indx {
		_, err = ifile.WriteString(fmt.Sprintf("%v %v\n", k, v))
		if err != nil {
			log.Fatalln(err)
		}
	}
}

func (nob *Nob) offsetOf(key string) (int64, error) {
	ofst, exists := nob.indx[key]
	if !exists {
		return -1, errors.New("key does not exist")
	}

	return ofst, nil
}

func (nob *Nob) populateIndex() map[string]int64 {
	res := map[string]int64{}
	_, err := nob.dbfile.Seek(0, io.SeekStart)
	if err != nil {
		log.Fatalln(err)
	}

	sc := bufio.NewScanner(nob.dbfile)
	var offset int64 = 0
	for sc.Scan() {
		line := sc.Text()

		key := line[0:strings.Index(line, ",")]
		res[key] = offset
		offset += int64(len(line) + 1)
	}
	return res
}
