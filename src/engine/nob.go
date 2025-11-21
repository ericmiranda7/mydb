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
	n := Nob{dbfile: dbfile, indx: nil, rootDir: rootDir}
	n.indx = getIndexFrom(dbfile)
	ticker := time.NewTicker(time.Second * 10)
	go func() {
		for {
			select {
			case <-ticker.C:
				n.mergeCompact()
			}
		}
	}()
	return &n
}

func (nob *Nob) Set(key string, val string) {
	wrote := nob.persist(key, val)

	latestOffset := nob.updateOffset(key, wrote)

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
	orderedFileNames := nob.getOrderedSegFiles()
	var files []*os.File
	for _, f := range orderedFileNames {
		of, _ := os.Open(f)
		files = append(files, of)
	}
	compactedMap := nob.compact(files...)

	// todo(): runtimestamped compacted_file

	compactedFileWritePath := path.Join(nob.rootDir, "compacted_file")
	compactedFile, err := os.Create(compactedFileWritePath)
	if err != nil {
		log.Fatalln(err)
	}

	for k, v := range compactedMap {
		_, _ = io.WriteString(compactedFile, fmt.Sprintf("%v %v\n", k, v))
	}

	//	todo(): writeFile(indexOf(compactedMap))
	compactedIndx := getIndexFrom(compactedFile)
	writeSegmentIndex(nob.rootDir, "compacted", compactedIndx)

	// delete files
	for _, oldSeg := range orderedFileNames {
		err = os.Remove(oldSeg)
		if err != nil {
			log.Fatalln(err)
		}
	}
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

	writeSegmentIndex(nob.rootDir, segname, nob.indx)
}

func writeSegmentIndex(rootDir, segName string, indx map[string]int64) {
	// write out segment index
	ifile, err := os.Create(path.Join(rootDir, fmt.Sprintf("indx_%v", segName)))
	if err != nil {
		log.Fatalln(err)
	}
	for k, v := range indx {
		_, err = ifile.WriteString(fmt.Sprintf("%v %v\n", k, v))
		if err != nil {
			log.Fatalln(err)
		}
	}
	println(ifile.Name())
}

func (nob *Nob) offsetOf(key string) (int64, error) {
	ofst, exists := nob.indx[key]
	if !exists {
		return -1, errors.New("key does not exist")
	}

	return ofst, nil
}

func getIndexFrom(f *os.File) map[string]int64 {
	res := map[string]int64{}
	_, err := f.Seek(0, io.SeekStart)
	if err != nil {
		log.Fatalln(err)
	}

	sc := bufio.NewScanner(f)
	var offset int64 = 0
	for sc.Scan() {
		line := sc.Text()

		key := line[0:strings.Index(line, " ")]
		res[key] = offset
		offset += int64(len(line) + 1)
	}
	return res
}

func (nob *Nob) persist(key string, val string) int64 {
	line := key + " " + val + "\n"
	written, err := nob.dbfile.Write([]byte(line))
	if err != nil {
		log.Fatalln("brew", err)
	}

	return int64(written)
}

func (nob *Nob) updateOffset(key string, wrote int64) int64 {
	dbStat, err := nob.dbfile.Stat()
	if err != nil {
		log.Fatalln(err)
	}
	latestOffset := dbStat.Size() - wrote
	nob.indx[key] = latestOffset
	return latestOffset
}
