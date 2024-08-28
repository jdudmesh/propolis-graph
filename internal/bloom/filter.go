package bloom

import (
	"fmt"

	"github.com/OneOfOne/xxhash"
	"github.com/btcsuite/btcutil/base58"
)

const FilterLen = 256
const base58Ver = 1

type Filter struct {
	value []byte
}

func New() *Filter {
	return &Filter{
		value: make([]byte, FilterLen/8),
	}
}

func (f *Filter) Set(val []byte) {
	h := xxhash.New32()
	h.Write(val)
	hval := h.Sum32() % FilterLen
	ix := hval / 8
	iy := hval % 8
	f.value[ix] = f.value[ix] | (1 << iy)
}

func (f *Filter) Unset(val []byte) {
	h := xxhash.New32()
	h.Write(val)
	hval := h.Sum32() % FilterLen
	ix := hval / 8
	iy := hval % 8
	f.value[ix] = f.value[ix] & ^(1 << iy)
}

func (f *Filter) Intersects(val []byte) bool {
	h := xxhash.New32()
	h.Write(val)
	hval := h.Sum32() % FilterLen
	ix := hval / 8
	iy := hval % 8
	return f.value[ix]&(1<<iy) > 0
}

func (f *Filter) IntersectsAny(val ...[]byte) bool {
	intersects := false
	for _, v := range val {
		h := xxhash.New32()
		h.Write(v)
		hval := h.Sum32() % FilterLen
		ix := hval / 8
		iy := hval % 8
		if f.value[ix]&(1<<iy) > 0 {
			intersects = true
			break
		}
	}
	return intersects
}

func (f *Filter) String() string {
	return base58.CheckEncode(f.value, base58Ver)
}

func (f *Filter) Parse(value string) error {
	b, v, err := base58.CheckDecode(value)
	if err != nil {
		return fmt.Errorf("invalid filter value: %w", err)
	}

	if v != base58Ver {
		return fmt.Errorf("invalid encoding version: %w", err)
	}

	if len(b) != len(f.value) {
		return fmt.Errorf("invalid filter value length")
	}

	copy(f.value, b)

	return nil
}
