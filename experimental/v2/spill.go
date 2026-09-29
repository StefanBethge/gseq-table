package gtable

import (
	"container/heap"
	"os"

	"github.com/stefanbethge/gseq-table/experimental/v2/internal/block"
	"github.com/stefanbethge/gseq-table/experimental/v2/internal/spill"
)

// Spilling (D6, P1). A step over all rows collects its input in a sorter.
// Over the budget, the sorter writes its rows as a sorted run to a spill
// file in the run's directory; at the end it merges the runs. Sort sorts by
// its keys, group by by the key of the group, and a join keeps the order
// of its left rows. Rows with equal keys keep the order in which they came,
// so that a spilled step gives the rows of the step in memory.

// spillFile is a file in the spill directory, written in segments.
type spillFile struct {
	f      *os.File
	w      *spill.Writer
	start  int64
	segs   []segment
	closed bool
	isKept bool // holds copies of rejected rows (D49)
}

type segment struct{ off, n int64 }

func (m *runMem) newFile(pattern string) (*spillFile, error) {
	d, err := m.spillDir()
	if err != nil {
		return nil, err
	}
	f, err := d.CreateFile(pattern)
	if err != nil {
		return nil, err
	}
	sf := &spillFile{f: f, w: spill.NewWriter(f)}
	m.files = append(m.files, sf)
	return sf, nil
}

func (sf *spillFile) begin() { sf.start = sf.w.Offset() }

// end ends the segment begun last and returns it.
func (sf *spillFile) end() (segment, error) {
	if err := sf.w.Flush(); err != nil {
		return segment{}, err
	}
	return segment{sf.start, sf.w.Offset() - sf.start}, nil
}

func (sf *spillFile) reader(s segment) *spill.Reader { return spill.Section(sf.f, s.off, s.n) }

func (sf *spillFile) remove() {
	if sf == nil || sf.closed {
		return
	}
	sf.closed = true
	sf.f.Close()
	os.Remove(sf.f.Name())
}

// srcID returns the number of src in the spill files of the run.
func (m *runMem) srcID(src *rawSource) int {
	if m.reg == nil {
		m.reg = map[*rawSource]int{}
	}
	id, ok := m.reg[src]
	if !ok {
		id = len(m.regList)
		m.reg[src] = id
		m.regList = append(m.regList, src)
	}
	return id
}

func (m *runMem) writeRefs(w *spill.Writer, refs []srcRef) {
	w.Uvarint(uint64(len(refs)))
	for _, r := range refs {
		w.Uvarint(uint64(m.srcID(r.src)))
		w.Uvarint(uint64(r.row))
	}
}

func (m *runMem) readRefs(r *spill.Reader) []srcRef {
	n := r.Int()
	if n == 0 || r.Err() != nil {
		return nil
	}
	refs := make([]srcRef, n)
	for i := range refs {
		id, row := r.Int(), r.Int()
		if id < len(m.regList) {
			refs[i] = srcRef{m.regList[id], row}
		}
	}
	return refs
}

// writeFrame writes the rows of b with their origins and sequence numbers.
func (m *runMem) writeFrame(w *spill.Writer, b batch, seq []int64) {
	w.Bool(true)
	w.Block(b.blk)
	for i, o := range b.orig {
		m.writeRefs(w, o.refs)
		w.Uvarint(uint64(o.agg))
		m.writeRefs(w, o.members)
		w.Varint(seq[i])
	}
}

// endFrames marks the end of the frames of a segment.
func endFrames(w *spill.Writer) { w.Bool(false) }

// readFrame reads the next frame; ok is false at the end of the segment.
func (m *runMem) readFrame(r *spill.Reader) (b batch, seq []int64, ok bool) {
	if !r.Bool() {
		return batch{}, nil, false
	}
	blk := r.Block()
	n := blk.Len()
	orig := make([]origin, n)
	seq = make([]int64, n)
	for i := range n {
		orig[i].refs = m.readRefs(r)
		orig[i].agg = r.Int()
		orig[i].members = m.readRefs(r)
		seq[i] = r.Varint()
	}
	if r.Err() != nil {
		return batch{}, nil, false
	}
	return batch{blk, orig}, seq, true
}

// originBytes estimates the memory of an origin. The source rows an
// aggregated row stands for are not counted (G66).
func originBytes(o origin) int64 { return 48 + 16*int64(len(o.refs)) }

func batchBytes(b batch) int64 {
	n := blockBytes(b.blk) + 8*int64(len(b.orig)) // sequence numbers
	for _, o := range b.orig {
		n += originBytes(o)
	}
	return n
}

// sorter collects the rows of a step over all rows and spills them as
// sorted runs when the run is over its budget.
type sorter struct {
	m        *runMem
	s        schema    // columns of the spilled rows
	keys     []SortKey // on s; nil keeps the order of the rows
	frameLen int
	pattern  string
	prep     func(batch) batch    // turns a batch into rows of s before a spill
	onSpill  func([]origin) error // called with the origins of spilled rows

	buf      []batch
	seq      [][]int64
	bytes    int64
	rows     int64
	next     int64
	file     *spillFile
	rowBytes int64 // bytes per spilled row, for the fan-in of a merge
}

// add collects b; its rows are numbered in order.
func (x *sorter) add(b batch) {
	seq := make([]int64, b.blk.Len())
	for i := range seq {
		seq[i] = x.next
		x.next++
	}
	x.addSeq(b, seq)
}

// addSeq collects b with the given sequence numbers.
func (x *sorter) addSeq(b batch, seq []int64) {
	if b.blk.Len() == 0 {
		return
	}
	n := batchBytes(b)
	x.buf = append(x.buf, b)
	x.seq = append(x.seq, seq)
	x.bytes += n
	x.rows += int64(b.blk.Len())
	x.m.grow(n)
}

func (x *sorter) spilled() bool { return x.file != nil && len(x.file.segs) > 0 }

// take returns the collected blocks and origins and stops counting them.
func (x *sorter) take() ([]block.Block, []origin) {
	blks := make([]block.Block, len(x.buf))
	var orig []origin
	for i, b := range x.buf {
		blks[i] = b.blk
		orig = append(orig, b.orig...)
	}
	x.drop()
	return blks, orig
}

func (x *sorter) drop() {
	x.m.shrink(x.bytes)
	x.buf, x.seq, x.bytes, x.rows = nil, nil, 0, 0
}

// sorted returns the collected rows as one batch in order, with their
// sequence numbers.
func (x *sorter) sorted(bs []batch, seqs [][]int64) (batch, []int64) {
	blks := make([]block.Block, len(bs))
	var orig []origin
	var seq []int64
	for i, b := range bs {
		blks[i] = b.blk
		orig = append(orig, b.orig...)
		seq = append(seq, seqs[i]...)
	}
	all := concatBlocks(blks, x.s)
	if x.keys == nil {
		return batch{all, orig}, seq
	}
	idx := sortIndex(all, x.s, x.keys)
	out := make([]int64, len(idx))
	for i, j := range idx {
		out[i] = seq[j]
	}
	return batch{all.Take(idx), pick(orig, idx)}, out
}

// spill writes the collected rows as a sorted run.
func (x *sorter) spill() error {
	if len(x.buf) == 0 {
		return nil
	}
	bs := x.buf
	if x.prep != nil {
		bs = make([]batch, len(x.buf))
		for i, b := range x.buf {
			bs[i] = x.prep(b)
		}
	}
	b, seq := x.sorted(bs, x.seq)
	if x.file == nil {
		f, err := x.m.newFile(x.pattern)
		if err != nil {
			return err
		}
		x.file = f
	}
	x.file.begin()
	for _, c := range chunkRows(b.blk.Len(), x.frameLen) {
		x.m.writeFrame(x.file.w, batch{b.blk.Take(c), pick(b.orig, c)}, pickSeq(seq, c))
	}
	endFrames(x.file.w)
	seg, err := x.file.end()
	if err != nil {
		return err
	}
	x.file.segs = append(x.file.segs, seg)
	x.rowBytes = max(x.rowBytes, x.bytes/max(x.rows, 1))
	if x.onSpill != nil {
		if err := x.onSpill(b.orig); err != nil {
			return err
		}
	}
	x.drop()
	x.m.spills++
	return nil
}

// close removes the spill file of the sorter.
func (x *sorter) close() {
	x.file.remove()
	x.file = nil
}

func chunkRows(n, size int) [][]int {
	size = max(size, 1)
	var out [][]int
	for start := 0; start < n; start += size {
		c := make([]int, 0, min(size, n-start))
		for i := start; i < min(start+size, n); i++ {
			c = append(c, i)
		}
		out = append(out, c)
	}
	return out
}

func pickSeq(seq []int64, idx []int) []int64 {
	out := make([]int64, len(idx))
	for i, j := range idx {
		out[i] = seq[j]
	}
	return out
}

// row is row i of a batch that a merge yields, with the bytes it is
// counted with.
type row struct {
	b     batch
	seq   []int64
	i     int
	bytes int64
}

// merge yields the collected rows in order. If nothing was spilled, it
// sorts in memory.
func (x *sorter) merge(yield func(row) error) error {
	if !x.spilled() {
		if len(x.buf) == 0 {
			return nil
		}
		// The rows leave the buffer, so that a spill meanwhile has
		// nothing to write; they are counted until the merge ends.
		b, seq := x.sorted(x.buf, x.seq)
		bytes := x.bytes
		x.buf, x.seq, x.bytes, x.rows = nil, nil, 0, 0
		defer x.m.shrink(bytes)
		for i := range b.blk.Len() {
			if err := yield(row{b: b, seq: seq, i: i}); err != nil {
				return err
			}
		}
		return nil
	}
	if err := x.spill(); err != nil {
		return err
	}
	segs := x.file.segs
	// Merging many runs at once would hold a frame of each; merge them in
	// passes, earlier runs first so that equal keys keep their order.
	if fan := x.fanIn(); len(segs) > fan {
		for len(segs) > fan {
			seg, err := x.mergeInto(segs[:fan])
			if err != nil {
				return err
			}
			segs = append([]segment{seg}, segs[fan:]...)
		}
	}
	return x.mergeSegs(segs, yield)
}

// fanIn is the number of runs merged at once: as many frames as fit in an
// eighth of the budget, at least two, so that the runs of a process can
// merge at the same time.
func (x *sorter) fanIn() int {
	b := x.m.budget()
	frame := x.rowBytes * int64(max(x.frameLen, 1))
	if b <= 0 || frame <= 0 {
		return 1 << 20
	}
	return int(max(2, b/8/frame))
}

// mergeInto merges segs into a new segment at the end of the file.
func (x *sorter) mergeInto(segs []segment) (segment, error) {
	rb := newRowBuilder(x.s, x.m)
	x.file.begin()
	write := func() {
		b, seq := rb.flush()
		x.m.writeFrame(x.file.w, b, seq)
	}
	err := x.mergeSegs(segs, func(r row) error {
		if rb.add(r); rb.len() >= x.frameLen {
			write()
		}
		return nil
	})
	if err != nil {
		return segment{}, err
	}
	if rb.len() > 0 {
		write()
	}
	endFrames(x.file.w)
	return x.file.end()
}

// cursor reads the frames of one segment.
type cursor struct {
	x     *sorter
	r     *spill.Reader
	run   int
	b     batch
	seq   []int64
	kv    []*vec
	i     int
	bytes int64 // bytes of the current frame
	per   int64 // per row
}

func (c *cursor) load() (bool, error) {
	c.x.m.shrink(c.bytes)
	c.bytes = 0
	b, seq, ok := c.x.m.readFrame(c.r)
	if !ok {
		return false, c.r.Err()
	}
	c.b, c.seq, c.i = b, seq, 0
	c.kv = keyVecs(b.blk, c.x.s, c.x.keys)
	c.bytes = batchBytes(b)
	c.per = c.bytes / int64(max(b.blk.Len(), 1))
	c.x.m.grow(c.bytes)
	return true, nil
}

type cursorHeap []*cursor

func (h cursorHeap) Len() int { return len(h) }
func (h cursorHeap) Less(a, b int) bool {
	x, y := h[a], h[b]
	if c := compareKeys(x.kv, x.i, y.kv, y.i, x.x.keys); c != 0 {
		return c < 0
	}
	return x.run < y.run
}
func (h cursorHeap) Swap(a, b int) { h[a], h[b] = h[b], h[a] }
func (h *cursorHeap) Push(v any)   { *h = append(*h, v.(*cursor)) }
func (h *cursorHeap) Pop() any {
	old := *h
	c := old[len(old)-1]
	*h = old[:len(old)-1]
	return c
}

// mergeSegs yields the rows of the sorted segments in order; of equal
// keys, those of an earlier segment first.
func (x *sorter) mergeSegs(segs []segment, yield func(row) error) error {
	var h cursorHeap
	defer func() {
		for _, c := range h {
			x.m.shrink(c.bytes)
		}
	}()
	for i, s := range segs {
		c := &cursor{x: x, r: x.file.reader(s), run: i}
		ok, err := c.load()
		if err != nil {
			return err
		}
		if ok {
			h = append(h, c)
		}
	}
	heap.Init(&h)
	for len(h) > 0 {
		c := h[0]
		if err := yield(row{b: c.b, seq: c.seq, i: c.i, bytes: c.per}); err != nil {
			return err
		}
		if c.i++; c.i < c.b.blk.Len() {
			heap.Fix(&h, 0)
			continue
		}
		ok, err := c.load()
		if err != nil {
			return err
		}
		if ok {
			heap.Fix(&h, 0)
		} else {
			heap.Pop(&h)
		}
	}
	return nil
}

// rowBuilder collects rows that a merge yields into a batch.
type rowBuilder struct {
	s     schema
	m     *runMem
	bs    []*block.Builder
	orig  []origin
	seq   []int64
	bytes int64
}

func newRowBuilder(s schema, m *runMem) *rowBuilder {
	rb := &rowBuilder{s: s, m: m, bs: make([]*block.Builder, len(s))}
	for i, f := range s {
		rb.bs[i] = block.NewBuilder(f.kind, 0)
	}
	return rb
}

func (rb *rowBuilder) add(r row) {
	for j, b := range rb.bs {
		b.AppendFrom(r.b.blk.Column(j), r.i)
	}
	rb.orig = append(rb.orig, r.b.orig[r.i])
	rb.seq = append(rb.seq, r.seq[r.i])
	rb.bytes += r.bytes
	rb.m.grow(r.bytes)
}

func (rb *rowBuilder) len() int { return len(rb.orig) }

// flush returns the collected rows and starts over.
func (rb *rowBuilder) flush() (batch, []int64) {
	cols := make([]block.Column, len(rb.bs))
	for i, b := range rb.bs {
		cols[i] = b.Build()
	}
	b := batch{newBlock(cols, len(rb.orig)), rb.orig}
	seq := rb.seq
	rb.orig, rb.seq = nil, nil
	rb.m.shrink(rb.bytes)
	rb.bytes = 0
	return b, seq
}

// rawSpill is the spilled raw state of a source read in a run: blocks of
// rows still in the plan, and copies of rejected rows (D49, D55).
type rawSpill struct {
	chunks *spillFile
	at     map[int]segment // spilled chunks
	kept   *spillFile
	keptAt map[int]segment // spilled copies of rejected rows
	closed bool

	cacheC    int
	cache     []block.Column
	lastRow   int
	lastCells []keptCell
}

// mem returns what the run that reads the source counts, or nil.
func (r *rawSource) mem() *runMem {
	if r.rel == nil || r.rel.owner == nil {
		return nil
	}
	return r.rel.owner.mem
}

func (r *rawSource) spillState() *rawSpill {
	if r.sp == nil {
		r.sp = &rawSpill{at: map[int]segment{}, keptAt: map[int]segment{}, cacheC: -1, lastRow: -1}
	}
	return r.sp
}

// spillChunk writes chunk c of the raw state to disk, if rows in the plan
// still need it.
func (r *rawSource) spillChunk(c int, m *runMem) error {
	if r.chunks[c] == nil || r.rel.chunkLive[c] == 0 || len(r.s) == 0 {
		return nil
	}
	sp := r.spillState()
	if sp.chunks == nil {
		f, err := m.newFile("raw-*")
		if err != nil {
			return err
		}
		sp.chunks = f
	}
	blk, err := block.New(r.chunks[c]...)
	if err != nil {
		return err
	}
	sp.chunks.begin()
	sp.chunks.w.Block(blk)
	seg, err := sp.chunks.end()
	if err != nil {
		return err
	}
	sp.at[c] = seg
	r.chunks[c] = nil
	m.shrink(r.rel.bytes[c])
	return nil
}

// spilledChunk returns chunk c from disk, or nil if it was not spilled.
func (r *rawSource) spilledChunk(c int) []block.Column {
	sp := r.sp
	if sp == nil {
		return nil
	}
	if sp.cacheC == c {
		return sp.cache
	}
	seg, ok := sp.at[c]
	if !ok {
		return nil
	}
	if sp.closed {
		panic("gtable: the spilled rows of this result were closed")
	}
	rd := sp.chunks.reader(seg)
	blk := rd.Block()
	if rd.Err() != nil {
		panic("gtable: reading spilled raw state: " + rd.Err().Error())
	}
	cols := make([]block.Column, blk.Width())
	for i := range cols {
		cols[i] = blk.Column(i)
	}
	sp.cacheC, sp.cache = c, cols
	return cols
}

// spilledKept returns the spilled copy of the raw state of a rejected row.
func (r *rawSource) spilledKept(row int) ([]keptCell, bool) {
	sp := r.sp
	if sp == nil {
		return nil, false
	}
	if sp.lastRow == row {
		return sp.lastCells, true
	}
	seg, ok := sp.keptAt[row]
	if !ok {
		return nil, false
	}
	if sp.closed {
		panic("gtable: the spilled rejected rows of this result were closed")
	}
	rd := sp.kept.reader(seg)
	cells := make([]keptCell, rd.Int())
	for i := range cells {
		cells[i].ok = rd.Bool()
		cells[i].s = rd.String()
	}
	if rd.Err() != nil {
		panic("gtable: reading spilled rejected rows: " + rd.Err().Error())
	}
	sp.lastRow, sp.lastCells = row, cells
	return cells, true
}

func keptBytes(cells []keptCell) int64 {
	n := int64(64)
	for _, c := range cells {
		n += 24 + int64(len(c.s))
	}
	return n
}

// spillKept writes the copies of the raw state of rejected rows to disk.
// They stay there until the result is closed (D49).
func (r *rawSource) spillKept(m *runMem) error {
	if r.rel == nil || len(r.rel.kept) == 0 {
		return nil
	}
	sp := r.spillState()
	if sp.kept == nil {
		f, err := m.newFile("rejects-*")
		if err != nil {
			return err
		}
		f.isKept = true
		sp.kept = f
	}
	w := sp.kept.w
	for row, cells := range r.rel.kept {
		off := w.Offset()
		w.Uvarint(uint64(len(cells)))
		for _, c := range cells {
			w.Bool(c.ok)
			w.String(c.s)
		}
		sp.keptAt[row] = segment{off, w.Offset() - off}
	}
	if err := w.Flush(); err != nil {
		return err
	}
	r.rel.kept = nil
	m.shrink(r.rel.keptBytes)
	r.rel.keptBytes = 0
	m.keptSpilled = true
	m.spills++
	return nil
}

// unspill reads the spilled blocks that rows of the result still need back
// into memory, and removes their file (D87).
func (r *rawSource) unspill() error {
	sp := r.sp
	if sp == nil || sp.chunks == nil {
		return nil
	}
	for c := range sp.at {
		if r.rel.chunkLive[c] > 0 {
			r.chunks[c] = r.spilledChunk(c)
		}
	}
	sp.chunks.remove()
	sp.chunks, sp.at, sp.cacheC, sp.cache = nil, map[int]segment{}, -1, nil
	return nil
}

// closeSpill closes the spilled copies of rejected rows at Close.
func (r *rawSource) closeSpill() {
	if r.sp == nil {
		return
	}
	r.sp.closed = true
	if r.sp.kept != nil {
		r.sp.kept.closed = true
		r.sp.kept.f.Close()
	}
}
