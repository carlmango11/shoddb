package files

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
)

type Files struct {
	dataDir string
}

func New(dataDir string) *Files {
	return &Files{
		dataDir: dataDir,
	}
}

func (f *Files) New(sk []byte, n int) *os.File {
	fileName := fmt.Sprintf("%v/%v.%v", f.dataDir, getKeyHash(sk), n)

	file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Panicf("error opening file '%v': %v", fileName, err)
	}

	return file
}

func (f *Files) Load(sk []byte) []*os.File {
	fis, err := ioutil.ReadDir(f.dataDir)
	if err != nil {
		log.Panicf("cannot read data dir '%v': %v", f.dataDir, err)
	}

	hash := getKeyHash(sk)

	files := map[int]*os.File{}

	for _, fi := range fis {
		bits := strings.Split(fi.Name(), ".")

		if bits[0] != hash {
			continue
		}

		n, err := strconv.ParseInt(bits[1], 10, 64)
		if err != nil {
			panic(err)
		}

		path := f.dataDir + fi.Name()

		file, err := os.Open(path)
		if err != nil {
			log.Panicf("cannot read data dir '%v': %v", file, err)
		}

		files[int(n)] = file
	}

	var ordered []*os.File
	for i := 0; i < len(files); i++ {
		ordered = append(ordered, files[i])
	}

	return ordered
}

func getKeyHash(sk []byte) string {
	hasher := sha1.New()
	hasher.Write(sk)

	return hex.EncodeToString(hasher.Sum(nil))
}
