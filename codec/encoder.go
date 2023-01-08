package codec

import (
	"encoding/binary"
	"encoding/json"
	"io"
)

type Encoder struct {
	w io.Writer
}

func NewEncoder(writer io.Writer) *Encoder {
	return &Encoder{
		w: writer,
	}
}

func (e *Encoder) WriteString(s string) {
	b := []byte(s)

	e.writeLen(len(b))
	e.w.Write(b)
}

func (e *Encoder) WritePayload(v any) {
	valueBytes, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}

	e.writeLen(len(valueBytes))
	e.w.Write(valueBytes)
}

func (e *Encoder) writeLen(n int) {
	l := make([]byte, 4) // 32 bits
	binary.LittleEndian.PutUint32(l, uint32(n))

	e.w.Write(l)
}
