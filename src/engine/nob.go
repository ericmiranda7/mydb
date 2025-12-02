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
	"strconv"
	"strings"
	"time"

	"git.target.com/eric.miranda/mydb/v2/src/util"
)

type Nob struct {
	memtable    *util.TreeMap
	rootDir     string
	segmentSize int64
	segNo       int
}

func NewNob(rootDir string) *Nob {
	n := Nob{memtable: nil, rootDir: rootDir, segmentSize: 100, segNo: 0}
	// todo(): build
	//n.memtable = buildIndexOf(dbfile)
	n.memtable = util.NewTreeMap()
	//ticker := time.NewTicker(time.Second * 10)
	ticker := time.NewTicker(time.Hour * 10)
	go func() {
		for {
			select {
			case <-ticker.C:
				n.mergeCompact()
				fmt.Println("compact complete")
			}
		}
	}()
	return &n
}

func (nob *Nob) Set(key string, val string) {
	nob.memtable.Insert(key, val)
	if nob.memtable.GetSize() > 150 {
		nob.createSegment()
	}
}

func (nob *Nob) Get(key string) (string, error) {
	val, exists := nob.memtable.Get(key)
	if exists {
		return val, nil
	}

	// todo() check seg-1

	return "", errors.New("nokey")
}

func (nob *Nob) mergeCompact() {
	orderedSegFileNames := nob.getOrderedSegFiles()
	log.Println("segnames", orderedSegFileNames)
	var files []*os.File
	for _, f := range orderedSegFileNames {
		of, _ := os.Open(f)
		files = append(files, of)
	}
	compactedMap, ok := nob.compact(files...)
	if !ok {
		log.Println("no files to compact")
		return
	}

	nob.segNo = 0
	// todo(bug): using seg number will rewrite compacted files when its reset
	// can look into level / size-tiered compaction
	compctFileName := fmt.Sprintf("compacted_%v", nob.allocateSeg())
	compactedFileWritePath := path.Join(nob.rootDir, compctFileName)
	compactedFile, err := os.Create(compactedFileWritePath)
	if err != nil {
		log.Fatalln(err)
	}
	log.Println("cf is", compactedFile.Name())

	for k, v := range compactedMap {
		_, _ = io.WriteString(compactedFile, fmt.Sprintf("%v %v\n", k, v))
	}

	compactedIndx := buildIndexOf(compactedFile)
	nob.writeSegmentIndex(compctFileName, compactedIndx)

	// delete files
	for _, oldSeg := range orderedSegFileNames {
		err = os.Remove(oldSeg)
		if err != nil {
			log.Fatalln(err)
		}
		err = os.Remove(path.Join(nob.rootDir, fmt.Sprintf("indx_%v", path.Base(oldSeg))))
		if err != nil {
			log.Fatalln(err)
		}
	}
}

func (nob *Nob) getOrderedSegFiles() []string {
	dirFiles, _ := os.ReadDir(nob.rootDir)

	var res []string
	for _, file := range dirFiles {
		if ok, _ := regexp.MatchString("^seg", file.Name()); ok {
			res = append(res, path.Join(nob.rootDir, file.Name()))
		}
	}

	sort.Slice(res, func(i, j int) bool {
		f1name, f2name := path.Base(res[i]), path.Base(res[j])
		log.Println(f1name, f2name)
		f1no, err := strconv.Atoi(strings.Split(f1name, "_")[1])
		if err != nil {
			log.Fatalln(err)
		}
		f2no, err := strconv.Atoi(strings.Split(f2name, "_")[1])
		if err != nil {
			log.Fatalln(err)
		}
		return f1no < f2no
	})
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
	// get segname
	segname := fmt.Sprintf("seg_%v", nob.allocateSeg())

	// write to segment
	segfile, err := os.Create(path.Join(nob.rootDir, segname))
	defer func(segfile *os.File) {
		err := segfile.Close()
		if err != nil {
			log.Fatalln(err)
		}
	}(segfile)
	if err != nil {
		log.Fatalln(err)
	}

	segmentIndx := map[string]int64{}
	orderedKv := nob.memtable.GetInorder()
	log.Println(orderedKv)
	for _, kv := range orderedKv {
		offset, err := segfile.Seek(0, io.SeekCurrent)
		segmentIndx[kv.Key] = offset
		_, err = segfile.WriteString(fmt.Sprintf("%v %v\n", kv.Key, kv.Value))
		if err != nil {
			log.Fatalln(err)
		}
	}

	// write to indx
	indxfile, err := os.Create(path.Join(nob.rootDir, fmt.Sprintf("indx_%v", segname)))
	defer func(indxfile *os.File) {
		err := indxfile.Close()
		if err != nil {
			log.Fatalln()
		}
	}(indxfile)
	if err != nil {
		log.Fatalln(err)
	}

	for k, v := range segmentIndx {
		_, err = indxfile.WriteString(fmt.Sprintf("%v %v\n", k, v))
		if err != nil {
			log.Fatalln(err)
		}
	}

	// start write to new memtable
	nob.memtable = util.NewTreeMap()
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
}

// getLocation returns the offset, the containing segment file and whether the key exists in any file
//func (nob *Nob) getLocation(key string) (int64, *os.File, bool) {
//	offset, exists := nob.memtable[key]
//	if !exists {
//		// get old memtable seggies
//		oldIndexes := getOldIndexes(nob.rootDir)
//		// check em
//		for _, dirEntry := range oldIndexes {
//			file, err := os.Open(path.Join(nob.rootDir, dirEntry.Name()))
//			if err != nil {
//				log.Fatalln(err)
//			}
//			log.Println("indxfile", file.Name())
//			indx := loadIndexFrom(file)
//			offset, ok := indx[key]
//			if ok {
//				fileName := strings.ReplaceAll(file.Name(), "indx_", "")
//				segfile, err := os.Open(fileName)
//				if err != nil {
//					log.Fatalln(err)
//				}
//				log.Println("containing file:", segfile.Name(), "memtable", indx)
//				return offset, segfile, ok
//			}
//		}
//
//		// no old segment has it
//		return 0, nil, false
//	} else {
//		return offset, nob.dbfile, true
//	}
//}

func getOldIndexes(dir string) []os.DirEntry {
	files, err := os.ReadDir(dir)
	if err != nil {
		log.Fatalln(err)
	}

	var res []os.DirEntry
	for _, file := range files {
		matched, err := regexp.MatchString("^memtable", file.Name())
		if err != nil {
			log.Fatalln(err)
		}
		if matched {
			res = append(res, file)
		}
	}

	return res
}

func loadIndexFrom(f *os.File) map[string]int64 {
	res := map[string]int64{}
	_, err := f.Seek(0, 0)
	if err != nil {
		log.Fatalln(err)
	}

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		kvparts := strings.Split(sc.Text(), " ")
		k, v := kvparts[0], kvparts[1]
		i, err := strconv.Atoi(v)
		if err != nil {
			log.Fatalln("offset is NaN")
		}
		res[k] = int64(i)
	}

	return res
}

func buildIndexOf(f *os.File) map[string]int64 {
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

//func (nob *Nob) updateOffset(key string, wrote int64) int64 {
//	dbStat, err := nob.dbfile.Stat()
//	if err != nil {
//		log.Fatalln(err)
//	}
//	latestOffset := dbStat.Size() - wrote
//	nob.memtable[key] = latestOffset
//	return latestOffset
//}
