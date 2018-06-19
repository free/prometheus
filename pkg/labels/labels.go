// Copyright 2017 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package labels

import (
	//	"bytes"
	"encoding/json"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"unsafe"

	"github.com/cespare/xxhash"
)

const sep = '\xff'

// Well-known label names used by Prometheus components.
const (
	MetricName   = "__name__"
	AlertName    = "alertname"
	BucketLabel  = "le"
	InstanceName = "instance"
)

// Label is a key/value pair of strings.
type Label struct {
	Name, Value string
}

type Labels struct {
	s string
}

type labels struct {
	hash    uint64
	isPrint bool
	nlabels uint16
	offsets [65536]uint16 // actually [<1 + nlabels*2>]uint16
	// buf  [<offsets[nlabels*2] - 1>]byte
}

var (
	labelsPointer *labels
	zeroLabels    = New()
)

const (
	sizeofLabels        = int(unsafe.Sizeof(*labelsPointer))
	offsetLabelsOffsets = int(unsafe.Offsetof(labelsPointer.offsets))
	sizeofLabelsOffset  = int(unsafe.Sizeof(labelsPointer.offsets[0]))
)

type labelset []Label

func (ls labelset) Len() int           { return len(ls) }
func (ls labelset) Swap(i, j int)      { ls[i], ls[j] = ls[j], ls[i] }
func (ls labelset) Less(i, j int) bool { return ls[i].Name < ls[j].Name }

func isPrint(s string) bool {
	buf := make([]byte, 0, 1024)
	buf = strconv.AppendQuote(buf, s)
	return s == string(buf[1:len(buf)-1])
}

func New(ls ...Label) Labels {
	if len(ls) > math.MaxInt16 {
		panic("More than 32k labels")
	}

	var set labelset
	if len(ls) > 0 {
		set = make(labelset, len(ls))
		copy(set, ls)
		sort.Sort(set)
	}

	size := offsetLabelsOffsets + (len(ls)*2+1)*sizeofLabelsOffset
	for _, l := range set {
		size += len(l.Name) + len(l.Value) + 4 // equals, 2 quotes, comma
	}
	if len(set) > 0 {
		size += 1 // opening brace only, closing brace replaces a comma
	} else {
		size += 2 // opening and closing braces
	}
	if size > math.MaxUint16 {
		panic("Labels longer than 64k")
	}

	b := make([]byte, size)
	bdata := (*reflect.SliceHeader)(unsafe.Pointer(&b)).Data

	ll := (*labels)(unsafe.Pointer(bdata))
	ll.isPrint = true
	ll.nlabels = uint16(len(set))

	b = b[:offsetLabelsOffsets+(2*len(set)+1)*sizeofLabelsOffset]
	b = append(b, '{')
	for i, l := range set {
		ll.offsets[i*2] = uint16(len(b))
		b = append(b, l.Name...)

		if isPrint(l.Value) {
			b = append(b, '=')
		} else {
			b = append(b, '\x00')
			ll.isPrint = false
		}
		b = append(b, '"')
		ll.offsets[i*2+1] = uint16(len(b))
		b = append(b, l.Value...)
		b = append(b, '"', ',')
	}
	if len(set) > 0 {
		b = b[:len(b)-1]
	}
	b = append(b, '}')
	ll.offsets[len(set)*2] = uint16(len(b))

	var hdr reflect.StringHeader
	hdr.Data = bdata
	hdr.Len = size
	return Labels{*(*string)(unsafe.Pointer(&hdr))}
}

func (ls *Labels) labels() *labels {
	if ls.s == "" {
		ls.s = zeroLabels.s
	}
	return (*labels)(unsafe.Pointer((*reflect.StringHeader)(unsafe.Pointer(&ls.s)).Data))
}

func (ls Labels) Hash() uint64 {
	l := ls.labels()
	if l.hash == 0 {
		var hdr reflect.SliceHeader
		hdr.Data = (*reflect.StringHeader)(unsafe.Pointer(&ls.s)).Data
		hdr.Len = len(ls.s)
		hdr.Cap = len(ls.s)
		buf := *(*[]byte)(unsafe.Pointer(&hdr))

		l.hash = xxhash.Sum64(buf[offsetLabelsOffsets+int(ls.labels().nlabels*2+1)*sizeofLabelsOffset:])
	}
	return l.hash
}

func (ls Labels) isPrint() bool {
	return ls.labels().isPrint
}

func (ls Labels) Len() int {
	return int(ls.labels().nlabels)
}

func (ls Labels) offset(i int) uint16 {
	return ls.labels().offsets[i]
}

func (ls Labels) Label(i int) string {
	start := ls.offset(i * 2)
	end := ls.offset(i*2 + 2)
	return ls.s[start : end-1]
}

func (ls Labels) LabelName(i int) string {
	start := ls.offset(i * 2)
	end := ls.offset(i*2 + 1)
	return ls.s[start : end-2]
}

func (ls Labels) LabelValue(i int) string {
	start := ls.offset(i*2 + 1)
	end := ls.offset(i*2 + 2)
	return ls.s[start : end-2]
}

func (ls Labels) String() string {
	l := ls.labels()
	if l.isPrint {
		offset := int(offsetLabelsOffsets + int(ls.labels().nlabels*2+1)*sizeofLabelsOffset)
		return ls.s[offset:]
	} else {
		buf := make([]byte, 0, 1024)
		buf = append(buf, '{')
		for i := 0; i < int(l.nlabels); i++ {
			if ls.s[ls.offset(i*2+1)-2] == '=' {
				buf = append(buf, ls.Label(i)...)
			} else {
				buf = append(buf, ls.LabelName(i)...)
				buf = append(buf, '=')
				buf = strconv.AppendQuote(buf, ls.LabelValue(i))
			}
			buf = append(buf, ',')
		}
		buf[len(buf)-1] = '}'
		return string(buf)
	}
}

//type labels []Label
//
//func (ls labels) Len() int           { return len(ls) }
//func (ls labels) Swap(i, j int)      { ls[i], ls[j] = ls[j], ls[i] }
//func (ls labels) Less(i, j int) bool { return ls[i].Name < ls[j].Name }
//
//func equal(ls, o labels) bool {
//	if len(ls) != len(o) {
//		return false
//	}
//	for i, l := range ls {
//		if l.Name != o[i].Name || l.Value != o[i].Value {
//			return false
//		}
//	}
//	return true
//}
//
//// Labels is a sorted set of labels. Order has to be guaranteed upon
//// instantiation.
//type Labels struct {
//	L    []Label
//	hash uint64
//}
//
//func (ls Labels) String() string {
//	var b bytes.Buffer
//
//	b.WriteByte('{')
//	for i, l := range ls.L {
//		if i > 0 {
//			b.WriteByte(',')
//			b.WriteByte(' ')
//		}
//		b.WriteString(l.Name)
//		b.WriteByte('=')
//		b.WriteString(strconv.Quote(l.Value))
//	}
//	b.WriteByte('}')
//
//	return b.String()
//}

// MarshalJSON implements json.Marshaler.
func (ls *Labels) MarshalJSON() ([]byte, error) {
	return json.Marshal(ls.Map())
}

// UnmarshalJSON implements json.Unmarshaler.
func (ls *Labels) UnmarshalJSON(b []byte) error {
	var m map[string]string

	if err := json.Unmarshal(b, &m); err != nil {
		return err
	}

	*ls = FromMap(m)
	return nil
}

//
//// Hash returns a hash value for the label set.
//func (ls *Labels) Hash() uint64 {
//	if ls.hash != 0 {
//		return ls.hash
//	}
//	b := make([]byte, 0, 1024)
//
//	for _, v := range ls.L {
//		b = append(b, v.Name...)
//		b = append(b, sep)
//		b = append(b, v.Value...)
//		b = append(b, sep)
//	}
//	ls.hash = xxhash.Sum64(b)
//	return ls.hash
//}

// Get returns the value for the label with the given name.
// Returns an empty string if the label doesn't exist.
func (ls Labels) Get(name string) string {
	for i := 0; i < ls.Len(); i++ {
		if ls.LabelName(i) == name {
			return ls.LabelValue(i)
		}
	}
	return ""
}

// Has returns true if the label with the given name is present.
func (ls Labels) Has(name string) bool {
	for i := 0; i < ls.Len(); i++ {
		if ls.LabelName(i) == name {
			return true
		}
	}
	return false
}

// Equal returns whether the two label sets are equal.
func Equal(ls, o Labels) bool {
	if ls.s == "" || o.s == "" {
		return ls.Len() == 0 && o.Len() == 0
	}
	return ls.s[8:] == o.s[8:]
}

// Map returns a string map of the labels.
func (ls Labels) Map() map[string]string {
	m := make(map[string]string, ls.Len())
	for i := 0; i < ls.Len(); i++ {
		m[ls.LabelName(i)] = ls.LabelValue(i)
	}
	return m
}

//// New returns a sorted Labels from the given labels.
//// The caller has to guarantee that all label names are unique.
//func New(ls ...Label) Labels {
//	if len(ls) == 0 {
//		return Labels{}
//	}
//	set := make(labels, len(ls))
//	copy(set, ls)
//	sort.Sort(set)
//
//	return Labels{L: set}
//}

// FromMap returns new sorted Labels from the given map.
func FromMap(m map[string]string) Labels {
	l := make([]Label, 0, len(m))
	for k, v := range m {
		l = append(l, Label{Name: k, Value: v})
	}
	return New(l...)
}

// FromStrings creates new labels from pairs of strings.
func FromStrings(ss ...string) Labels {
	if len(ss)%2 != 0 {
		panic("invalid number of strings")
	}
	var res labelset
	for i := 0; i < len(ss); i += 2 {
		res = append(res, Label{Name: ss[i], Value: ss[i+1]})
	}
	sort.Sort(res)

	return New(res...)
}

// Compare compares the two label sets.
// The result will be 0 if a==b, <0 if a < b, and >0 if a > b.
func Compare(a, b Labels) int {
	l := a.Len()
	if b.Len() < l {
		l = b.Len()
	}

	for i := 0; i < l; i++ {
		if d := strings.Compare(a.LabelName(i), b.LabelName(i)); d != 0 {
			return d
		}
		if d := strings.Compare(a.LabelValue(i), b.LabelValue(i)); d != 0 {
			return d
		}
	}
	// If all labels so far were in common, the set with fewer labels comes first.
	return a.Len() - b.Len()
}

// Builder allows modifiying Labels.
type Builder struct {
	base Labels
	del  []string
	add  []Label
}

// NewBuilder returns a new LabelsBuilder
func NewBuilder(base Labels) *Builder {
	return &Builder{
		base: base,
		del:  make([]string, 0, 5),
		add:  make([]Label, 0, 5),
	}
}

// Del deletes the label of the given name.
func (b *Builder) Del(ns ...string) *Builder {
	for _, n := range ns {
		for i, a := range b.add {
			if a.Name == n {
				b.add = append(b.add[:i], b.add[i+1:]...)
			}
		}
		b.del = append(b.del, n)
	}
	return b
}

// Set the name/value pair as a label.
func (b *Builder) Set(n, v string) *Builder {
	for i, a := range b.add {
		if a.Name == n {
			b.add[i].Value = v
			return b
		}
	}
	b.add = append(b.add, Label{Name: n, Value: v})

	return b
}

// Labels returns the labels from the builder. If no modifications
// were made, the original labels are returned.
func (b *Builder) Labels() Labels {
	if len(b.del) == 0 && len(b.add) == 0 {
		return b.base
	}

	// In the general case, labels are removed, modified or moved
	// rather than added.
	res := make(labelset, 0, b.base.Len()+len(b.add))
Outer:
	for i := 0; i < b.base.Len(); i++ {
		for _, n := range b.del {
			if b.base.LabelName(i) == n {
				continue Outer
			}
		}
		for _, la := range b.add {
			if b.base.LabelName(i) == la.Name {
				continue Outer
			}
		}
		res = append(res, Label{b.base.LabelName(i), b.base.LabelValue(i)})
	}
	res = append(res, b.add...)
	sort.Sort(res)

	return New(res...)
}
