package table

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/stefanbethge/gseq/option"
	"github.com/stefanbethge/gseq/slice"
)

// Struct mapping converts between Go structs and tables. Each exported field
// maps to one column; the gseq struct tag controls the mapping:
//
//	type Person struct {
//	    Name  string                 `gseq:"name"`
//	    Age   int                    `gseq:"age"`
//	    Email option.Option[string]  `gseq:"email"`
//	    Born  *time.Time             `gseq:"born"`
//	    Note  string                 `gseq:",omitempty"` // column "Note"
//	    Temp  string                 `gseq:"-"`          // skipped
//	}
//
// Without a tag the field name is the column name. Unexported fields are
// skipped and fields of embedded structs are flattened into the parent.
//
// Supported field types are the table.Value types, types whose underlying type
// is a built-in integer, float, string or bool (e.g. type Status string),
// pointers to them, and option.Option[T] for T in table.Value. Cells are parsed
// and formatted with the same rules as the typed methods (GetAs, ColAs, ...).
//
// Pointer and Option fields represent optional values: nil and None are
// written as empty cells, and empty cells are read back as nil and None.
//
// The omitempty option writes the zero value as an empty cell and, when
// reading, leaves the field at its zero value if the cell is empty or the
// column is missing.

// structCodec converts one field value to and from a cell string. The
// reflect.Value passed in is always addressable.
type structCodec struct {
	format func(reflect.Value) string
	parse  func(raw string, dst reflect.Value) error
	empty  func(raw string) bool // reports whether raw is a missing value
}

// structField is one column of a struct mapping.
type structField struct {
	name      string
	index     []int
	omitEmpty bool
	codec     structCodec
}

// structMapping is the cached column layout of a struct type.
type structMapping struct {
	fields []structField
	err    error
}

var structMappings sync.Map // reflect.Type -> *structMapping

// structCodecs holds the codecs for the table.Value types and option.Option
// of them.
var structCodecs = func() map[reflect.Type]structCodec {
	m := make(map[reflect.Type]structCodec)
	registerStructCodec[int](m)
	registerStructCodec[int8](m)
	registerStructCodec[int16](m)
	registerStructCodec[int32](m)
	registerStructCodec[int64](m)
	registerStructCodec[uint](m)
	registerStructCodec[uint8](m)
	registerStructCodec[uint16](m)
	registerStructCodec[uint32](m)
	registerStructCodec[uint64](m)
	registerStructCodec[float32](m)
	registerStructCodec[float64](m)
	registerStructCodec[string](m)
	registerStructCodec[bool](m)
	registerStructCodec[time.Time](m)
	return m
}()

func registerStructCodec[T Value](m map[reflect.Type]structCodec) {
	m[reflect.TypeFor[T]()] = structCodec{
		format: func(v reflect.Value) string {
			return formatValue(*v.Addr().Interface().(*T))
		},
		parse: func(raw string, dst reflect.Value) error {
			x, err := parseValueErr[T](raw)
			if err != nil {
				return err
			}
			*dst.Addr().Interface().(*T) = x
			return nil
		},
		empty: isEmptyCell[T],
	}
	m[reflect.TypeFor[option.Option[T]]()] = structCodec{
		format: func(v reflect.Value) string {
			if x, ok := v.Addr().Interface().(*option.Option[T]).Get(); ok {
				return formatValue(x)
			}
			return ""
		},
		parse: func(raw string, dst reflect.Value) error {
			p := dst.Addr().Interface().(*option.Option[T])
			if isEmptyCell[T](raw) {
				*p = option.None[T]()
				return nil
			}
			x, err := parseValueErr[T](raw)
			if err != nil {
				return err
			}
			*p = option.Some(x)
			return nil
		},
		empty: isEmptyCell[T],
	}
}

// isEmptyCell reports whether raw counts as a missing value for T. Only a
// truly empty string is missing for string; other types ignore whitespace.
func isEmptyCell[T Value](raw string) bool {
	if _, ok := any(*new(T)).(string); ok {
		return raw == ""
	}
	return strings.TrimSpace(raw) == ""
}

// basicKindTypes maps the kinds usable as underlying types of named field
// types to their built-in type.
var basicKindTypes = map[reflect.Kind]reflect.Type{
	reflect.Int:     reflect.TypeFor[int](),
	reflect.Int8:    reflect.TypeFor[int8](),
	reflect.Int16:   reflect.TypeFor[int16](),
	reflect.Int32:   reflect.TypeFor[int32](),
	reflect.Int64:   reflect.TypeFor[int64](),
	reflect.Uint:    reflect.TypeFor[uint](),
	reflect.Uint8:   reflect.TypeFor[uint8](),
	reflect.Uint16:  reflect.TypeFor[uint16](),
	reflect.Uint32:  reflect.TypeFor[uint32](),
	reflect.Uint64:  reflect.TypeFor[uint64](),
	reflect.Float32: reflect.TypeFor[float32](),
	reflect.Float64: reflect.TypeFor[float64](),
	reflect.String:  reflect.TypeFor[string](),
	reflect.Bool:    reflect.TypeFor[bool](),
}

// codecFor returns the codec for field type typ.
func codecFor(typ reflect.Type) (structCodec, bool) {
	if c, ok := structCodecs[typ]; ok {
		return c, true
	}
	if base, ok := basicKindTypes[typ.Kind()]; ok {
		// Named type such as `type Status string`: view the field through a
		// pointer to its built-in underlying type.
		c := structCodecs[base]
		ptr := reflect.PointerTo(base)
		asBase := func(v reflect.Value) reflect.Value { return v.Addr().Convert(ptr).Elem() }
		return structCodec{
			format: func(v reflect.Value) string { return c.format(asBase(v)) },
			parse:  func(raw string, dst reflect.Value) error { return c.parse(raw, asBase(dst)) },
			empty:  c.empty,
		}, true
	}
	if typ.Kind() == reflect.Pointer {
		c, ok := codecFor(typ.Elem())
		if !ok {
			return structCodec{}, false
		}
		elem := typ.Elem()
		return structCodec{
			format: func(v reflect.Value) string {
				if v.IsNil() {
					return ""
				}
				return c.format(v.Elem())
			},
			parse: func(raw string, dst reflect.Value) error {
				if c.empty(raw) {
					dst.SetZero()
					return nil
				}
				n := reflect.New(elem)
				if err := c.parse(raw, n.Elem()); err != nil {
					return err
				}
				dst.Set(n)
				return nil
			},
			empty: c.empty,
		}, true
	}
	return structCodec{}, false
}

// mappingFor returns the cached column layout of struct type typ.
func mappingFor(typ reflect.Type) *structMapping {
	if m, ok := structMappings.Load(typ); ok {
		return m.(*structMapping)
	}
	m := &structMapping{}
	if typ.Kind() != reflect.Struct {
		m.err = fmt.Errorf("%s is not a struct type", typ)
	} else {
		seen := make(map[string]bool)
		m.fields, m.err = collectStructFields(typ, nil, seen, m.fields)
	}
	actual, _ := structMappings.LoadOrStore(typ, m)
	return actual.(*structMapping)
}

func collectStructFields(typ reflect.Type, prefix []int, seen map[string]bool, out []structField) ([]structField, error) {
	for i := range typ.NumField() {
		f := typ.Field(i)
		tag, hasTag := f.Tag.Lookup("gseq")
		if tag == "-" {
			continue
		}
		index := append(append([]int(nil), prefix...), i)
		if f.Anonymous && !hasTag && f.Type.Kind() == reflect.Struct && f.Type != reflect.TypeFor[time.Time]() {
			var err error
			if out, err = collectStructFields(f.Type, index, seen, out); err != nil {
				return nil, err
			}
			continue
		}
		if !f.IsExported() {
			continue
		}
		name, opts, _ := strings.Cut(tag, ",")
		if name == "" {
			name = f.Name
		}
		c, ok := codecFor(f.Type)
		if !ok {
			return nil, fmt.Errorf("field %s: unsupported type %s", f.Name, f.Type)
		}
		if seen[name] {
			return nil, fmt.Errorf("field %s: duplicate column %q", f.Name, name)
		}
		seen[name] = true
		out = append(out, structField{
			name:      name,
			index:     index,
			omitEmpty: hasTagOption(opts, "omitempty"),
			codec:     c,
		})
	}
	return out, nil
}

func hasTagOption(opts, want string) bool {
	for opt := range strings.SplitSeq(opts, ",") {
		if strings.TrimSpace(opt) == want {
			return true
		}
	}
	return false
}

// FromStructs builds a Table from a slice of structs or struct pointers. The
// columns are the mapped fields in declaration order; see the struct mapping
// rules above. A nil pointer item becomes a row of empty cells.
//
//	people := []Person{{Name: "Alice", Age: 30}, {Name: "Bob", Age: 25}}
//	t := table.FromStructs(people)
//
// If T is not a struct (or pointer to struct) type or has a field of an
// unsupported type, an empty Table with an error recorded is returned.
func FromStructs[T any](items []T) Table {
	typ := reflect.TypeFor[T]()
	isPtr := typ.Kind() == reflect.Pointer
	if isPtr {
		typ = typ.Elem()
	}
	m := mappingFor(typ)
	if m.err != nil {
		return New(nil, nil).withErrf("FromStructs: %v", m.err)
	}
	headers := make(slice.Slice[string], len(m.fields))
	for j, f := range m.fields {
		headers[j] = f.name
	}
	width := len(m.fields)
	cells := make([]string, len(items)*width)
	rows := make(slice.Slice[Row], len(items))
	src := reflect.ValueOf(items)
	for i := range items {
		vals := cells[i*width : (i+1)*width : (i+1)*width]
		item := src.Index(i)
		if isPtr {
			if item.IsNil() {
				rows[i] = NewRow(headers, vals)
				continue
			}
			item = item.Elem()
		}
		for j, f := range m.fields {
			v := item.FieldByIndex(f.index)
			if f.omitEmpty && v.IsZero() {
				continue
			}
			vals[j] = f.codec.format(v)
		}
		rows[i] = NewRow(headers, vals)
	}
	return newTable(headers, rows)
}

// ToStructs parses every row of t into a T, which must be a struct type. See
// the struct mapping rules above. Columns without a matching field are
// ignored.
//
// It returns an error if T is not a supported struct type, a mapped column
// does not exist (unless the field has omitempty), or a cell cannot be parsed
// (empty cells count as unparseable for non-optional, non-string fields).
//
//	people, err := t.ToStructs[Person]()
func (t Table) ToStructs[T any]() ([]T, error) {
	return toStructs[T](t.source, len(t.Rows), t.headerIdx, func(i, idx int) string {
		return valueAtRow(t.Rows[i].values, idx)
	})
}

// ToStructs parses every row of m into a T, which must be a struct type. See
// Table.ToStructs.
func (m *MutableTable) ToStructs[T any]() ([]T, error) {
	return toStructs[T](m.source, len(m.rows), m.headerIdx, func(i, idx int) string {
		return valueAt(m.rows[i], idx)
	})
}

func toStructs[T any](source string, n int, headerIdx map[string]int, cellAt func(i, idx int) string) ([]T, error) {
	const op = "ToStructs"
	mapping := mappingFor(reflect.TypeFor[T]())
	if mapping.err != nil {
		return nil, typedErrf(source, "%s: %v", op, mapping.err)
	}
	cols := make([]int, len(mapping.fields))
	for j, f := range mapping.fields {
		idx, ok := headerIdx[f.name]
		if !ok && !f.omitEmpty {
			return nil, typedErrf(source, "%s: unknown column %q", op, f.name)
		}
		if !ok {
			idx = -1
		}
		cols[j] = idx
	}
	out := make([]T, n)
	items := reflect.ValueOf(out)
	for i := range n {
		item := items.Index(i)
		for j, f := range mapping.fields {
			if cols[j] < 0 {
				continue
			}
			raw := cellAt(i, cols[j])
			if f.omitEmpty && f.codec.empty(raw) {
				continue
			}
			if err := f.codec.parse(raw, item.FieldByIndex(f.index)); err != nil {
				return nil, typedErrf(source, "%s: column %q row %d: %v", op, f.name, i, err)
			}
		}
	}
	return out, nil
}
