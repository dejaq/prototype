package pooler

import "sync"

type FlatBufferBuilder struct {
	sync.Pool
}

type ByteSlice struct {
	sync.Pool
}
