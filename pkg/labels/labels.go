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
	"bytes"
	"encoding/json"
	"sort"
	"strconv"
	"strings"

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

type labels []Label

func (ls labels) Len() int           { return len(ls) }
func (ls labels) Swap(i, j int)      { ls[i], ls[j] = ls[j], ls[i] }
func (ls labels) Less(i, j int) bool { return ls[i].Name < ls[j].Name }

func equal(ls, o labels) bool {
	if len(ls) != len(o) {
		return false
	}
	for i, l := range ls {
		if l.Name != o[i].Name || l.Value != o[i].Value {
			return false
		}
	}
	return true
}

// Labels is a sorted set of labels. Order has to be guaranteed upon
// instantiation.
type Labels struct {
	L    []Label
	hash uint64
}

func (ls Labels) String() string {
	var b bytes.Buffer

	b.WriteByte('{')
	for i, l := range ls.L {
		if i > 0 {
			b.WriteByte(',')
			b.WriteByte(' ')
		}
		b.WriteString(l.Name)
		b.WriteByte('=')
		b.WriteString(strconv.Quote(l.Value))
	}
	b.WriteByte('}')

	return b.String()
}

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

// Hash returns a hash value for the label set.
func (ls *Labels) Hash() uint64 {
	if ls.hash != 0 {
		return ls.hash
	}
	b := make([]byte, 0, 1024)

	for _, v := range ls.L {
		b = append(b, v.Name...)
		b = append(b, sep)
		b = append(b, v.Value...)
		b = append(b, sep)
	}
	ls.hash = xxhash.Sum64(b)
	return ls.hash
}

// Get returns the value for the label with the given name.
// Returns an empty string if the label doesn't exist.
func (ls *Labels) Get(name string) string {
	for _, l := range ls.L {
		if l.Name == name {
			return l.Value
		}
	}
	return ""
}

// Has returns true if the label with the given name is present.
func (ls *Labels) Has(name string) bool {
	for _, l := range ls.L {
		if l.Name == name {
			return true
		}
	}
	return false
}

// Equal returns whether the two label sets are equal.
func Equal(ls, o Labels) bool {
	return equal(ls.L, o.L)
}

// Map returns a string map of the labels.
func (ls *Labels) Map() map[string]string {
	m := make(map[string]string, len(ls.L))
	for _, l := range ls.L {
		m[l.Name] = l.Value
	}
	return m
}

// New returns a sorted Labels from the given labels.
// The caller has to guarantee that all label names are unique.
func New(ls ...Label) Labels {
	if len(ls) == 0 {
		return Labels{}
	}
	set := make(labels, len(ls))
	copy(set, ls)
	sort.Sort(set)

	return Labels{L: set}
}

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
	var res labels
	for i := 0; i < len(ss); i += 2 {
		res = append(res, Label{Name: ss[i], Value: ss[i+1]})
	}

	sort.Sort(res)
	return Labels{L: res}
}

// Compare compares the two label sets.
// The result will be 0 if a==b, <0 if a < b, and >0 if a > b.
func Compare(a, b Labels) int {
	l := len(a.L)
	if len(b.L) < l {
		l = len(b.L)
	}

	for i := 0; i < l; i++ {
		if d := strings.Compare(a.L[i].Name, b.L[i].Name); d != 0 {
			return d
		}
		if d := strings.Compare(a.L[i].Value, b.L[i].Value); d != 0 {
			return d
		}
	}
	// If all labels so far were in common, the set with fewer labels comes first.
	return len(a.L) - len(b.L)
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
	res := make(labels, 0, len(b.base.L)+len(b.add))
Outer:
	for _, l := range b.base.L {
		for _, n := range b.del {
			if l.Name == n {
				continue Outer
			}
		}
		for _, la := range b.add {
			if l.Name == la.Name {
				continue Outer
			}
		}
		res = append(res, l)
	}
	res = append(res, b.add...)
	sort.Sort(res)

	return Labels{L: res}
}
