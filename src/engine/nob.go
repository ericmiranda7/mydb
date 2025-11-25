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
	dbfile      *os.File
	indx        map[string]int64
	rootDir     string
	segmentSize int64
	segNo       int
}

func NewNob(dbfile *os.File, rootDir string) *Nob {
	n := Nob{dbfile: dbfile, indx: nil, rootDir: rootDir, segmentSize: 100, segNo: 0}
	n.indx = getIndexFrom(dbfile)
	ticker := time.NewTicker(time.Second * 10)
	go func() {
		for {
			select {
			case <-ticker.C:
				// TODO(first): why are there 2 indx files created? haw?
				n.mergeCompact()
			}
		}
	}()
	return &n
}

func (nob *Nob) Set(key string, val string) {
	wrote := nob.persist(key, val)

	latestOffset := nob.updateOffset(key, wrote)

	if latestOffset >= nob.segmentSize {
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
	compactedMap, ok := nob.compact(files...)
	if !ok {
		log.Println("no files to compact")
		return
	}

	nob.segNo = 0
	compctFileName := fmt.Sprintf("compacted_%v", nob.allocateSeg())
	compactedFileWritePath := path.Join(nob.rootDir, compctFileName)
	compactedFile, err := os.Create(compactedFileWritePath)
	if err != nil {
		log.Fatalln(err)
	}

	for k, v := range compactedMap {
		_, _ = io.WriteString(compactedFile, fmt.Sprintf("%v %v\n", k, v))
	}

	compactedIndx := getIndexFrom(compactedFile)
	nob.writeSegmentIndex(fmt.Sprintf("indx_%v", compctFileName), compactedIndx)

	// delete files
	for _, oldSeg := range orderedFileNames {
		err = os.Remove(oldSeg)
		if err != nil {
			log.Fatalln(err)
		}
		err = os.Remove(fmt.Sprintf("indx_%v", oldSeg))
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

// compact returns false if no segment files exist
func (nob *Nob) compact(files ...*os.File) (map[string]string, bool) {
	if len(files) == 0 {
		return nil, false
	}
	res := map[string]string{}

	for _, f := range files {
		sc := bufio.NewScanner(f)

		for sc.Scan() {
			kv := strings.Split(sc.Text(), " ")
			res[kv[0]] = kv[1]
		}
	}

	return res, true
}

func (nob *Nob) createSegment() {
	// write out old log
	segname := fmt.Sprintf("seg_%v", nob.allocateSeg())

	// rename dbfile to segfile
	err := os.Rename(nob.dbfile.Name(), path.Join(nob.rootDir, segname))
	if err != nil {
		log.Fatalln("cant rename", err)
	}

	newDb, err := os.Create(path.Join(nob.rootDir, "db"))
	if err != nil {
		log.Fatalln("cant create, ", err)
	}
	// start write to new segment
	nob.dbfile = newDb

	nob.writeSegmentIndex(segname, nob.indx)
}

func (nob *Nob) allocateSeg() int {
	nob.segNo += 1
	return nob.segNo
}

func (nob *Nob) writeSegmentIndex(segName string, indx map[string]int64) {
	// write out segment index
	ifile, err := os.Create(path.Join(nob.rootDir, fmt.Sprintf("indx_%v", segName)))
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
