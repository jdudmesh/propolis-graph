package bloom

import (
	"bytes"
	"fmt"

	"github.com/OneOfOne/xxhash"
	"github.com/bits-and-blooms/bitset"
	"github.com/btcsuite/btcutil/base58"
)

const FilterLen = 256
const base58Ver = 1

type Filter struct {
	value bitset.BitSet
}

func New() *Filter {
	return &Filter{
		value: bitset.BitSet{},
	}
}

func (f *Filter) pos(val []byte) uint {
	h := xxhash.New32()
	h.Write(val)
	return uint(h.Sum32() % FilterLen)
}

func (f *Filter) Set(val []byte) {
	f.value.Set(f.pos(val))
}

func (f *Filter) Unset(val []byte) {
	f.value.Clear(f.pos(val))
}

func (f *Filter) Intersects(val []byte) bool {
	return f.value.Test(f.pos(val))
}

func (f *Filter) IntersectsAny(val ...[]byte) bool {
	for _, v := range val {
		if f.value.Test(f.pos(v)) {
			return true
		}
	}
	return false
}

func (f *Filter) String() string {
	buf := bytes.NewBuffer(nil)
	f.value.WriteTo(buf)
	return base58.CheckEncode(buf.Bytes(), base58Ver)
}

func (f *Filter) Parse(value string) error {
	b, v, err := base58.CheckDecode(value)
	if err != nil {
		return fmt.Errorf("invalid filter value: %w", err)
	}
	if v != base58Ver {
		return fmt.Errorf("invalid encoding version: %w", err)
	}
	buf := bytes.NewBuffer(b)
	f.value.ReadFrom(buf)
	return nil
}
