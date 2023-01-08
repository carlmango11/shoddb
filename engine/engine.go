package engine

import (
	"bytes"
	"github.com/igrmk/treemap/v2"
	"io"
	"os"
	"shoddb/codec"
	"shoddb/files"
	"sync"
)

const maxLen = 2 // TODO: should be expressed as disk space

type SegKey string

type Segment struct {
	mu    sync.RWMutex
	files []*os.File
}

type Engine[V any] struct {
	files *files.Files

	segsMu sync.Mutex
	segs   map[SegKey]*Segment

	memtableMu sync.RWMutex                           // TODO make normal lock or use RW features
	memtable   map[SegKey]*treemap.TreeMap[string, V] // TODO implement that myself
}

func New[V any](dataDir string) *Engine[V] {
	e := &Engine[V]{
		files:    files.New(dataDir),
		segs:     map[SegKey]*Segment{},
		memtable: map[SegKey]*treemap.TreeMap[string, V]{},
	}

	go e.periodicCompact()

	return e
}

func (e *Engine[V]) Write(key string, val V) {
	sk := segKey(key)

	mt := e.getMemtable(sk)
	mt.Set(key, val)

	if mt.Len() > maxLen {
		e.resetMemtable(sk)

		// TODO: this should be async, but it raises the issue of what happens if a read comes in while the write to
		// disk is happening. Ideally reads and writes should be able to continue but I think that means holding onto
		// N memtables and only binning them once the write to disk completes.
		e.writeMemtable(sk, mt)
	}
}

func (e *Engine[V]) Read(key string) (V, bool) {
	sk := segKey(key)
	mt := e.getMemtable(sk)

	v, ok := mt.Get(key)
	if ok {
		return v, true
	}

	return e.readSegmentFiles(e.getSegment(sk), key)
}

func (e *Engine[V]) writeMemtable(sk SegKey, mt *treemap.TreeMap[string, V]) {
	segData := e.toBytes(mt)
	seg := e.getSegment(sk)

	e.writeSegData(sk, seg, segData)
}

func (e *Engine[V]) resetMemtable(sk SegKey) {
	e.memtableMu.Lock()
	defer e.memtableMu.Unlock()

	e.memtable[sk] = treemap.New[string, V]()
}

func (e *Engine[V]) getMemtable(sk SegKey) *treemap.TreeMap[string, V] {
	e.memtableMu.Lock()
	defer e.memtableMu.Unlock()

	mt, ok := e.memtable[sk]
	if !ok {
		mt = treemap.New[string, V]()
		e.memtable[sk] = mt
	}

	return mt
}

func (e *Engine[V]) getSegment(sk SegKey) *Segment {
	e.segsMu.Lock()
	defer e.segsMu.Unlock()

	seg, ok := e.segs[sk]
	if !ok {
		seg = &Segment{
			files: e.files.Load([]byte(sk)),
		}
		e.segs[sk] = seg
	}

	return seg
}

func (e *Engine[V]) writeSegData(sk SegKey, seg *Segment, data []byte) {
	// take write lock in case another memtable fills up in the meantime and tries to write
	seg.mu.Lock()
	defer seg.mu.Unlock()

	f := e.files.New([]byte(sk), len(seg.files))
	seg.files = append(seg.files, f)

	_, err := f.Write(data)
	if err != nil {
		panic(err)
	}
}

func (e *Engine[V]) toBytes(mt *treemap.TreeMap[string, V]) []byte {
	var buf bytes.Buffer
	enc := codec.NewEncoder(&buf)

	for it := mt.Iterator(); it.Valid(); it.Next() {
		enc.WriteString(it.Key())
		enc.WritePayload(it.Value())
	}

	return buf.Bytes()
}

func (e *Engine[V]) periodicCompact() {

}

func (e *Engine[V]) readSegmentFiles(seg *Segment, key string) (V, bool) {
	seg.mu.RLock()
	defer seg.mu.RUnlock()

	// reverse chronological
	for i := len(seg.files) - 1; i >= 0; i-- {
		data := readFile[V](seg.files[i])

		val, ok := data[key]
		if ok {
			return val, true
		}
	}

	var n V
	return n, false
}

func readFile[V any](f *os.File) map[string]V {
	_, err := f.Seek(0, io.SeekStart)
	if err != nil {
		panic(err)
	}

	dec := codec.NewDecoder[V](f)

	data := map[string]V{}

	for {
		key, err := dec.ReadString()
		if err != nil {
			if err == io.EOF {
				break
			}

			panic(err)
		}

		data[key] = dec.ReadPayload()
	}

	return data
}

func segKey(key string) SegKey {
	return SegKey(key[0:1])
}
