package codec

import (
	"encoding/binary"
	"encoding/json"
	"io"
	"log"
)

type Decoder[V any] struct {
	r io.Reader
}

func NewDecoder[V any](reader io.Reader) *Decoder[V] {
	return &Decoder[V]{
		r: reader,
	}
}

func (d *Decoder[V]) ReadString() (string, error) {
	strBytes, err := d.readNext()
	if err != nil {
		return "", err
	}

	return string(strBytes), nil
}

func (d *Decoder[V]) ReadPayload() V {
	bytes, err := d.readNext()
	if err != nil {
		log.Printf("could not read payload. file corrupted or decoder misused: %v", err)
	}

	var v V
	err = json.Unmarshal(bytes, &v)
	if err != nil {
		panic(err)
	}

	return v
}

func (d *Decoder[V]) readNext() ([]byte, error) {
	payloadLen, err := d.readLen()
	if err != nil {
		return nil, err
	}

	s := make([]byte, payloadLen)

	_, err = d.r.Read(s)
	if err != nil {
		log.Panicf("could not read %v bytes as expected: %v", payloadLen, err)
	}

	return s, nil
}

func (d *Decoder[V]) readLen() (uint32, error) {
	l := make([]byte, 4) // 32 bits

	_, err := d.r.Read(l)
	if err != nil {
		return 0, err
	}

	return binary.LittleEndian.Uint32(l), nil
}
