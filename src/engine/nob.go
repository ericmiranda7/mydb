package engine

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"git.target.com/eric.miranda/mydb/v2/src/util"
)

const COMPACTED_PREFIX = "^compacted"
const SEGMENT_PREFIX = "^seg"

type Nob struct {
	memtable    *util.TreeMap
	rootDir     string
	segmentSize int64
	segNo       int
	blockSize   int64
}

type Anchor struct {
	key    string
	offset int64
}

func NewNob(rootDir string) *Nob {
	n := Nob{memtable: nil, rootDir: rootDir, segmentSize: 100, segNo: 0, blockSize: 10}
	// todo(): build
	//n.memtable = buildIndexOf(dbfile)
	n.memtable = util.NewTreeMap()
	//ticker := time.NewTicker(time.Second * 10)
	ticker := time.NewTicker(time.Hour * 10)
	go func() {
		for {
			select {
			case <-ticker.C:
				fmt.Println("====beginning compaction====")
				n.mergeCompact()
				fmt.Println("====compact complete====")
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

// Get(key) searches in the following steps
//
// 1. check in memory
//
// 2. get segfiles / compactedfiles in order of creation
//
// 3. search latest, latest-1, latest-2...
func (nob *Nob) Get(key string) (string, error) {
	val, exists := nob.memtable.Get(key)
	if exists {
		log.Println("found key", key, "value: ", val)
		return val, nil
	}

	segFiles := nob.getOrderedSegFiles(SEGMENT_PREFIX, false)
	log.Println("segfiles: ", segFiles)

	// i'm thinking if compaction ran, any existing segfile would be newer
	val, err := nob.searchSegments(key, segFiles)
	if err != nil {
		return "", errors.New("nokey")
	}

	return val, nil
}

func (nob *Nob) searchSegments(key string, segFiles []string) (string, error) {
	for _, segFile := range segFiles {
		// todo(first)
		// get indx file
		indexFile, err := os.Open(
			path.Join(nob.rootDir, fmt.Sprintf("indx_%v", filepath.Base(segFile))),
		)
		if err != nil {
			log.Fatalln(err)
		}
		// get lower offset
		segFile, err := os.Open(segFile)
		if err != nil {
			log.Fatalln(err)
		}
		fileInfo, err := segFile.Stat()
		if err != nil {
			log.Fatalln(err)
		}

		lowerOffset, upperOffset := getOffsets(key, indexFile, fileInfo.Size())
		fmt.Println("Offsets", lowerOffset, upperOffset)
		// search segFileName from loweroffset .. upperOffset
		val, err := searchFile(key, lowerOffset, upperOffset, segFile)
		if err == nil {
			return val, nil
		}
	}

	return "", errors.New("no key in segfiles found")
}

// getOffsets() returns lowerbound & upperbound to search within
func getOffsets(key string, indexFile *os.File, segFileSize int64) (int64, int64) {
	// todo
	indxSlice := loadSparseIndex(indexFile)
	s := 0
	e := len(indxSlice)

	for s < e {
		m := ((e - s) / 2) + s

		if indxSlice[m].key > key {
			e = m
		} else {
			s = m + 1
		}
	}

	if e == 0 {
		return 0, indxSlice[0].offset
	} else if e == len(indxSlice) {
		return indxSlice[e-1].offset, segFileSize
	} else {
		return indxSlice[e-1].offset, indxSlice[e].offset
	}
}

func searchFile(needle string, lowerOffset, upperOffset int64, segFile *os.File) (string, error) {
	currentOffset, err := segFile.Seek(lowerOffset, 0)
	if err != nil {
		log.Fatalln(err)
	}
	reader := bufio.NewReader(segFile)

	for currentOffset < upperOffset {
		line, err := reader.ReadString('\n')
		if err != nil {
			log.Fatalln(err)
		}
		log.Println("line is", line)
		kvpair := strings.Split(line, " ")
		key, val := kvpair[0], kvpair[1]
		if key == needle {
			return val, nil
		}

		currentOffset += int64(len(line))
	}
	return "", nil
}

func (nob *Nob) mergeCompact() {
	orderedSegFileNames := nob.getOrderedSegFiles(SEGMENT_PREFIX, true)
	log.Println("segnames", orderedSegFileNames)
	var segFiles []*os.File
	for _, f := range orderedSegFileNames {
		of, _ := os.Open(f)
		segFiles = append(segFiles, of)
	}
	compactedKeyValues, ok := nob.compact(segFiles...)
	if !ok {
		log.Println("no segFiles to compact")
		return
	}

	nob.segNo = 0
	// todo(bug): using seg number will rewrite compacted segFiles when its reset
	// todo(can look into level / size-tiered compaction)
	compactedSegName := fmt.Sprintf("compacted_%v", nob.allocateSeg())
	compactedSegWritePath := path.Join(nob.rootDir, compactedSegName)
	compactedSegFile, err := os.Create(compactedSegWritePath)
	if err != nil {
		log.Fatalln(err)
	}
	defer func(f *os.File) {
		err := f.Close()
		if err != nil {
			log.Fatalln(err)
		}
	}(compactedSegFile)

	for k, v := range compactedKeyValues {
		_, _ = io.WriteString(compactedSegFile, fmt.Sprintf("%v %v\n", k, v))
	}

	nob.createFileAndSparseIndex(compactedSegFile)

	// delete segFiles
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

// getOrderedSegFiles(pattern string, asc bool) returns absolute filepaths matching pattern sorted by asc
func (nob *Nob) getOrderedSegFiles(pattern string, asc bool) []string {
	dirFiles, _ := os.ReadDir(nob.rootDir)

	var res []string
	for _, file := range dirFiles {
		if ok, _ := regexp.MatchString(pattern, file.Name()); ok {
			res = append(res, path.Join(nob.rootDir, file.Name()))
		}
	}

	sort.Slice(res, func(i, j int) bool {
		f1name, f2name := path.Base(res[i]), path.Base(res[j])
		f1no, err := strconv.Atoi(strings.Split(f1name, "_")[1])
		if err != nil {
			log.Fatalln(err)
		}
		f2no, err := strconv.Atoi(strings.Split(f2name, "_")[1])
		if err != nil {
			log.Fatalln(err)
		}
		if asc {
			return f1no < f2no
		} else {
			return f2no < f1no
		}
	})
	return res
}

// compact returns false if no segment files exist
func (nob *Nob) compact(files ...*os.File) (map[string]string, bool) {
	if len(files) == 0 {
		return nil, false
	}
	hashMap := map[string]string{}

	for _, file := range files {
		sc := bufio.NewScanner(file)

		for sc.Scan() {
			kv := strings.Split(sc.Text(), " ")
			key, value := kv[0], kv[1]
			hashMap[key] = value
		}
	}

	return hashMap, true
}

// createSegment() creates a segment file with seg_{segNo} format
func (nob *Nob) createSegment() {
	// get segName
	segName := fmt.Sprintf("seg_%v", nob.allocateSeg())

	// write to segment
	segFile, err := os.Create(path.Join(nob.rootDir, segName))
	defer func(segfile *os.File) {
		err := segfile.Close()
		if err != nil {
			log.Fatalln(err)
		}
	}(segFile)
	if err != nil {
		log.Fatalln(err)
	}

	nob.createFileAndSparseIndex(segFile)

	// start write to new memtable
	nob.memtable = util.NewTreeMap()
}

// createSparseIndx(segFile) creates an index file with indx_{segFile} format
func (nob *Nob) createFileAndSparseIndex(segFile *os.File) {
	sparseIndx := map[string]int64{}
	orderedKv := nob.memtable.GetInorder()
	currentBlock := int64(0)
	for _, kv := range orderedKv {
		offset, err := segFile.Seek(0, io.SeekCurrent)
		if err != nil {
			log.Fatalln(err)
		}
		if offset/nob.blockSize > currentBlock {
			sparseIndx[kv.Key] = offset
			currentBlock = offset
		}
		_, err = segFile.WriteString(fmt.Sprintf("%v %v\n", kv.Key, kv.Value))
		if err != nil {
			log.Fatalln(err)
		}
	}

	// write to indx
	sparseIndxFile, err := os.Create(path.Join(nob.rootDir, fmt.Sprintf("indx_%v", filepath.Base(segFile.Name()))))
	if err != nil {
		log.Fatalln(err)
	}
	defer func(indxFile *os.File) {
		err := indxFile.Close()
		if err != nil {
			log.Fatalln()
		}
	}(sparseIndxFile)

	for k, v := range sparseIndx {
		_, err = sparseIndxFile.WriteString(fmt.Sprintf("%v %v\n", k, v))
		if err != nil {
			log.Fatalln(err)
		}
	}
}

func (nob *Nob) allocateSeg() int {
	nob.segNo += 1
	return nob.segNo
}

func (nob *Nob) writeSegmentIndex(segName string, indx map[string]int64) {
	// write out segment index
	indxFile, err := os.Create(path.Join(nob.rootDir, fmt.Sprintf("indx_%v", segName)))
	if err != nil {
		log.Fatalln(err)
	}
	defer func(f *os.File) {
		err := f.Close()
		if err != nil {
			log.Fatalln(err)
		}
	}(indxFile)

	for k, v := range indx {
		_, err = indxFile.WriteString(fmt.Sprintf("%v %v\n", k, v))
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

// returns an array of anchors sorted by key asc
func loadSparseIndex(f *os.File) []*Anchor {
	res := []*Anchor{}
	sc := bufio.NewScanner(f)

	for sc.Scan() {
		line := sc.Text()
		kvpair := strings.Split(line, " ")
		key, offsetString := kvpair[0], kvpair[1]
		offset, err := strconv.ParseInt(offsetString, 10, 64)
		if err != nil {
			log.Fatalln(err)
		}
		anchor := &Anchor{
			key:    key,
			offset: offset,
		}
		res = append(res, anchor)
	}

	return res
}

// loadIndexFrom(file) loads an index from disk into memory
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
	indx := map[string]int64{}
	_, err := f.Seek(0, io.SeekStart)
	if err != nil {
		log.Fatalln(err)
	}

	sc := bufio.NewScanner(f)
	var offset int64 = 0
	for sc.Scan() {
		line := sc.Text()

		key := line[0:strings.Index(line, " ")]
		indx[key] = offset
		offset += int64(len(line) + 1)
	}
	return indx
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
