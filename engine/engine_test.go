package engine

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"testing"
)

const testDataDir = "shoddb-tmp-test-data"

func setUp() {
	os.Mkdir(testDataDir, 0766)
}

func cleanUp() {
	err := os.RemoveAll(testDataDir)
	if err != nil {
		log.Printf("error cleaning up: %v", err)
	}
}

func TestEngine(t *testing.T) {
	setUp()
	defer cleanUp()

	eng := New[int](testDataDir)

	for i := 0; i < maxLen*5; i++ {
		eng.Write(fmt.Sprintf("%v", i), i)
	}

	val, ok := eng.Read("0")
	assert.True(t, ok)
	assert.Equal(t, 0, val)
}

func TestJSON(t *testing.T) {

}
