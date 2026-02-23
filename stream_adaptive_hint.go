package lql

import "sync/atomic"

type streamAdaptiveHint struct {
	floor int
	ceil  int
	value atomic.Int64
}

func newStreamAdaptiveHint(floor, initial, ceil int) *streamAdaptiveHint {
	if floor < 1 {
		floor = 1
	}
	if ceil > 0 && ceil < floor {
		ceil = floor
	}
	h := &streamAdaptiveHint{
		floor: floor,
		ceil:  ceil,
	}
	h.value.Store(int64(h.clamp(initial)))
	return h
}

func (h *streamAdaptiveHint) Load() int {
	if h == nil {
		return 0
	}
	return h.clamp(int(h.value.Load()))
}

func (h *streamAdaptiveHint) Observe(size int) {
	if h == nil || size <= 0 {
		return
	}
	size = h.clamp(size)
	for {
		current := int(h.value.Load())
		if current <= 0 {
			current = h.floor
		}
		next := size
		if size < current {
			// Decay slowly on shrink to avoid oscillation around rare large payloads.
			next = current - (current-size)/8
		}
		next = h.clamp(next)
		if h.value.CompareAndSwap(int64(current), int64(next)) {
			return
		}
	}
}

func (h *streamAdaptiveHint) clamp(size int) int {
	if size < h.floor {
		return h.floor
	}
	if h.ceil > 0 && size > h.ceil {
		return h.ceil
	}
	return size
}
