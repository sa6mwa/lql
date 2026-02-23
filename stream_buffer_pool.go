package lql

import "sync"

var streamJSONBytePool = newStreamBytePool()

type streamBytePool struct {
	classCaps []int
	classes   []sync.Pool
	wrappers  sync.Pool
}

type streamPooledBytes struct {
	buf []byte
}

func newStreamBytePool() *streamBytePool {
	caps := []int{
		256, 512, 1024, 2048, 4096,
		8192, 16384, 32768, 65536,
		131072, 262144, 524288, 1048576,
		2097152, 3145728, 4194304, 5242880, 8388608,
	}
	return &streamBytePool{
		classCaps: caps,
		classes:   make([]sync.Pool, len(caps)),
	}
}

func (p *streamBytePool) acquire(hint int) []byte {
	class := p.classIndex(hint)
	if class < 0 {
		if hint < 0 {
			hint = 0
		}
		return make([]byte, 0, hint)
	}
	if value := p.classes[class].Get(); value != nil {
		wrapper := value.(*streamPooledBytes)
		buf := wrapper.buf[:0]
		wrapper.buf = nil
		p.wrappers.Put(wrapper)
		return buf
	}
	return make([]byte, 0, p.classCaps[class])
}

func (p *streamBytePool) release(buf []byte) {
	class := p.classIndex(cap(buf))
	if class < 0 {
		return
	}
	var wrapper *streamPooledBytes
	if value := p.wrappers.Get(); value != nil {
		wrapper = value.(*streamPooledBytes)
	} else {
		wrapper = &streamPooledBytes{}
	}
	wrapper.buf = buf[:0]
	p.classes[class].Put(wrapper)
}

func (p *streamBytePool) classIndex(size int) int {
	if size <= 0 {
		return 0
	}
	for i, cap := range p.classCaps {
		if size <= cap {
			return i
		}
	}
	return -1
}
