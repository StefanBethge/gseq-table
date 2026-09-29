package testutil

// Docs gates of the v2 design set (design decision D37, feature F24).
//
// TestDocsIDConsistency checks the integrity of the IDs and links in
// docs/explanation/design/v2; TestPlanMappingConsistency checks the table
// "Welcher Test beweist welchen Fall" in the test plan against the
// Proves(t, "T<n>") calls in this module. Both are stdlib-only and run in the
// normal `go test ./...` of the module.
//
// The gates check the integrity of IDs and references, not the truth of what
// the documents say.

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"unicode"
)

// designDir is the design set checked by the gates, relative to the
// repository root.
const designDir = "docs/explanation/design/v2"

// familyFloors is the number of IDs each family has. A parser that finds
// fewer has gone blind (for example after a change of the heading style); one
// that finds more means a new ID was appended without raising the floor here
// in the same commit.
var familyFloors = map[string]int{
	"UC": 8,
	"D":  107,
	"F":  24,
	"T":  69,
	"P":  8,
	"G":  68,
}

// noUseCaseDecisions lists the decisions that deliberately name no use case
// ("Betroffene Use Cases: keine"). D37 exempts decisions about the design set
// itself; D68 is accepted in G62.
var noUseCaseDecisions = []string{"D37", "D68"}

// extraFiles are Markdown files outside the design set whose ID mentions and
// links are checked too, relative to the repository root.
var extraFiles = []string{
	"experimental/v2/CLAUDE.md",
	"experimental/v2/examples/README.md",
	"experimental/v2/bench/RESULTS.md",
}

func TestDocsIDConsistency(t *testing.T) {
	root := repoRoot(t)
	cfg := docsConfig{
		designDir:          filepath.Join(root, designDir),
		extraFiles:         joinAll(root, extraFiles),
		floors:             familyFloors,
		noUseCaseDecisions: noUseCaseDecisions,
	}
	for _, p := range checkDocs(cfg) {
		t.Error(p)
	}
}

// ---------------------------------------------------------------------------
// Configuration and entry point

type docsConfig struct {
	designDir          string
	extraFiles         []string
	floors             map[string]int
	noUseCaseDecisions []string
}

var families = []string{"UC", "D", "F", "T", "P", "G"}

// idDef is one ID defined by a heading "### <ID> — <Titel>".
type idDef struct {
	id     string
	family string
	num    int
	file   string // absolute path of the defining file
	line   int
	anchor string
}

// mdFile is a parsed Markdown file.
type mdFile struct {
	path     string
	lines    []mdLine
	anchors  map[string]bool
	sections []section // sections that start with an ID heading
}

type mdLine struct {
	num     int
	raw     string
	code    bool   // inside a fenced code block
	heading int    // heading level, 0 if not a heading
	text    string // heading text, if heading > 0
}

type section struct {
	def   idDef
	lines []mdLine // lines after the heading up to the next heading of level <= 3
}

func checkDocs(cfg docsConfig) []string {
	var probs []string
	report := func(format string, args ...any) { probs = append(probs, fmt.Sprintf(format, args...)) }

	paths, err := filepath.Glob(filepath.Join(cfg.designDir, "*.md"))
	if err != nil || len(paths) == 0 {
		return []string{fmt.Sprintf("no Markdown files in %s", cfg.designDir)}
	}
	files := map[string]*mdFile{}
	for _, p := range paths {
		f, err := parseMarkdown(p)
		if err != nil {
			report("%v", err)
			continue
		}
		files[p] = f
	}

	defs, defProbs := collectDefs(files)
	probs = append(probs, defProbs...)
	probs = append(probs, checkFamilies(defs, cfg.floors)...)

	// Links and bare IDs in the design set and the extra files.
	checked := make([]*mdFile, 0, len(files)+len(cfg.extraFiles))
	for _, p := range paths {
		if f := files[p]; f != nil {
			checked = append(checked, f)
		}
	}
	for _, p := range cfg.extraFiles {
		f, err := parseMarkdown(p)
		if err != nil {
			report("%v", err)
			continue
		}
		checked = append(checked, f)
	}
	for _, f := range checked {
		probs = append(probs, checkLinks(f, files, defs)...)
		probs = append(probs, checkBareIDs(f)...)
	}

	if idx := files[filepath.Join(cfg.designDir, "index.md")]; idx != nil {
		probs = append(probs, checkIndexRanges(idx, defs)...)
	} else {
		report("%s: index.md missing", cfg.designDir)
	}
	probs = append(probs, checkUseCaseCoverage(files, defs, cfg.noUseCaseDecisions)...)
	return probs
}

// ---------------------------------------------------------------------------
// Parsing

var (
	headingRe = regexp.MustCompile(`^(#{1,6})[ \t]+(.*?)[ \t]*#*[ \t]*$`)
	// idHeadingRe is the one accepted form of an ID heading.
	idHeadingRe = regexp.MustCompile(`^(UC|D|F|T|P|G)([1-9][0-9]*) — (\S.*)$`)
	// looseIDHeadingRe catches headings that look like an ID heading in any
	// other form, so a changed heading style fails loudly.
	looseIDHeadingRe = regexp.MustCompile(`^(UC|D|F|T|P|G)[0-9]+\b`)
	fenceRe          = regexp.MustCompile("^[ \t]*(```|~~~)")
	codeSpanRe       = regexp.MustCompile("`+[^`]*`+")
	linkRe           = regexp.MustCompile(`!?\[([^\]]*)\]\(([^)\s]*)\)`)
	idTokenRe        = regexp.MustCompile(`\b(UC|D|F|T|P|G)[0-9]+\b`)
	exactIDRe        = regexp.MustCompile(`^(UC|D|F|T|P|G)([0-9]+)$`)
)

func parseMarkdown(path string) (*mdFile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	f := &mdFile{path: path, anchors: map[string]bool{}}
	inFence := false
	for i, raw := range strings.Split(string(data), "\n") {
		l := mdLine{num: i + 1, raw: raw}
		switch {
		case fenceRe.MatchString(raw):
			inFence = !inFence
			l.code = true
		case inFence:
			l.code = true
		default:
			if m := headingRe.FindStringSubmatch(raw); m != nil {
				l.heading = len(m[1])
				l.text = m[2]
				slug, err := slugify(renderHeading(l.text))
				if err != nil {
					return nil, fmt.Errorf("%s:%d: %v", path, l.num, err)
				}
				f.anchors[slug] = true
			}
		}
		f.lines = append(f.lines, l)
	}
	return f, nil
}

// renderHeading approximates the text a Markdown renderer produces for a
// heading: link syntax and inline markup are removed, their text is kept.
func renderHeading(s string) string {
	s = linkRe.ReplaceAllString(s, "$1")
	return strings.NewReplacer("`", "", "*", "").Replace(s)
}

func collectDefs(files map[string]*mdFile) (map[string]idDef, []string) {
	var probs []string
	defs := map[string]idDef{}
	for _, path := range sortedKeys(files) {
		f := files[path]
		var cur *section
		for _, l := range f.lines {
			if l.heading > 0 && l.heading <= 3 {
				cur = nil
			}
			if l.heading == 0 {
				if cur != nil {
					cur.lines = append(cur.lines, l)
				}
				continue
			}
			m := idHeadingRe.FindStringSubmatch(l.text)
			if m == nil || l.heading != 3 {
				if looseIDHeadingRe.MatchString(l.text) {
					probs = append(probs, fmt.Sprintf("%s:%d: heading %q is not of the form \"### <ID> — <Titel>\"", rel(path), l.num, l.raw))
				}
				continue
			}
			num, _ := strconv.Atoi(m[2])
			slug, _ := slugify(renderHeading(l.text)) // already validated in parseMarkdown
			d := idDef{id: m[1] + m[2], family: m[1], num: num, file: path, line: l.num, anchor: slug}
			if prev, dup := defs[d.id]; dup {
				probs = append(probs, fmt.Sprintf("%s:%d: %s defined again (first at %s:%d)", rel(path), l.num, d.id, rel(prev.file), prev.line))
				continue
			}
			defs[d.id] = d
			f.sections = append(f.sections, section{def: d})
			cur = &f.sections[len(f.sections)-1]
		}
	}
	return defs, probs
}

// ---------------------------------------------------------------------------
// Checks

// checkFamilies checks uniqueness of the defining file, contiguity and floors.
func checkFamilies(defs map[string]idDef, floors map[string]int) []string {
	var probs []string
	for _, fam := range families {
		var nums []int
		filesOf := map[string]bool{}
		for _, d := range defs {
			if d.family == fam {
				nums = append(nums, d.num)
				filesOf[rel(d.file)] = true
			}
		}
		sort.Ints(nums)
		if len(filesOf) > 1 {
			probs = append(probs, fmt.Sprintf("family %s is defined in more than one file: %s", fam, strings.Join(sortedKeys(filesOf), ", ")))
		}
		for i, n := range nums {
			if n != i+1 {
				probs = append(probs, fmt.Sprintf("family %s is not contiguous: %s%d missing (IDs are replaced, never deleted)", fam, fam, i+1))
				break
			}
		}
		floor := floors[fam]
		switch {
		case len(nums) < floor:
			probs = append(probs, fmt.Sprintf("family %s: found %d IDs, floor is %d; the parser may have gone blind", fam, len(nums), floor))
		case len(nums) > floor:
			probs = append(probs, fmt.Sprintf("family %s: found %d IDs, floor is %d; raise the floor in familyFloors in the same commit", fam, len(nums), floor))
		}
	}
	return probs
}

// checkLinks checks that every relative link resolves to an existing file and
// heading anchor, and that every link whose text is an ID points to the
// defining file and the heading anchor of that ID.
func checkLinks(f *mdFile, files map[string]*mdFile, defs map[string]idDef) []string {
	var probs []string
	report := func(l mdLine, format string, args ...any) {
		probs = append(probs, fmt.Sprintf("%s:%d: %s", rel(f.path), l.num, fmt.Sprintf(format, args...)))
	}
	for _, l := range f.lines {
		if l.code {
			continue
		}
		prose := codeSpanRe.ReplaceAllStringFunc(l.raw, blank)
		for _, m := range linkRe.FindAllStringSubmatch(prose, -1) {
			text, target := m[1], m[2]
			if strings.Contains(target, "://") || strings.HasPrefix(target, "mailto:") {
				continue
			}
			pathPart, anchor, _ := strings.Cut(target, "#")
			resolved := f.path
			if pathPart != "" {
				resolved = filepath.Clean(filepath.Join(filepath.Dir(f.path), pathPart))
			}
			if _, err := os.Stat(resolved); err != nil {
				report(l, "link [%s](%s): target file does not exist", text, target)
				continue
			}

			if im := exactIDRe.FindStringSubmatch(text); im != nil {
				d, ok := defs[text]
				switch {
				case !ok:
					report(l, "link [%s](%s): %s is not defined", text, target, text)
				case resolved != d.file:
					report(l, "link [%s](%s): %s is defined in %s", text, target, text, rel(d.file))
				case anchor != d.anchor:
					report(l, "link [%s](%s): anchor should be #%s", text, target, d.anchor)
				}
				continue
			}
			if idTokenRe.MatchString(text) {
				report(l, "link [%s](%s): link text mixes an ID with other text; link each ID on its own", text, target)
			}
			if anchor != "" {
				tf := files[resolved]
				if tf == nil {
					var err error
					if tf, err = parseMarkdown(resolved); err != nil {
						report(l, "link [%s](%s): %v", text, target, err)
						continue
					}
				}
				if !tf.anchors[anchor] {
					report(l, "link [%s](%s): no heading with anchor #%s in %s", text, target, anchor, rel(resolved))
				}
			}
		}
	}
	return probs
}

// checkBareIDs reports ID mentions in prose that are not links. Headings are
// titles, not prose: an ID in a title (such as "G61 — UC8 fachlich prüfen")
// cannot be a link without changing the anchor. Code spans and fenced blocks
// are exempt too.
func checkBareIDs(f *mdFile) []string {
	var probs []string
	for _, l := range f.lines {
		if l.code || l.heading > 0 {
			continue
		}
		s := codeSpanRe.ReplaceAllStringFunc(l.raw, blank)
		s = linkRe.ReplaceAllStringFunc(s, blank)
		for _, id := range idTokenRe.FindAllString(s, -1) {
			probs = append(probs, fmt.Sprintf("%s:%d: bare ID %s; every ID mention is a link", rel(f.path), l.num, id))
		}
	}
	return probs
}

var rangeRe = regexp.MustCompile(`\[(UC|D|F|T|P|G)([0-9]+)\]\([^)]*\)\s*[–-]\s*\[(UC|D|F|T|P|G)([0-9]+)\]\([^)]*\)`)

// checkIndexRanges checks that index.md states the current range of every
// family.
func checkIndexRanges(idx *mdFile, defs map[string]idDef) []string {
	var probs []string
	maxOf := map[string]int{}
	for _, d := range defs {
		maxOf[d.family] = max(maxOf[d.family], d.num)
	}
	seen := map[string]bool{}
	for _, l := range idx.lines {
		if l.code {
			continue
		}
		for _, m := range rangeRe.FindAllStringSubmatch(l.raw, -1) {
			fam := m[1]
			from, _ := strconv.Atoi(m[2])
			to, _ := strconv.Atoi(m[4])
			seen[fam] = true
			if m[3] != fam {
				probs = append(probs, fmt.Sprintf("%s:%d: range %s%s–%s%s mixes families", rel(idx.path), l.num, fam, m[2], m[3], m[4]))
				continue
			}
			if from != 1 || to != maxOf[fam] {
				probs = append(probs, fmt.Sprintf("%s:%d: range %s%d–%s%d is stale; current range is %s1–%s%d", rel(idx.path), l.num, fam, from, fam, to, fam, fam, maxOf[fam]))
			}
		}
	}
	for _, fam := range families {
		if !seen[fam] {
			probs = append(probs, fmt.Sprintf("%s: no range statement for family %s", rel(idx.path), fam))
		}
	}
	return probs
}

const (
	useCasesField = "**Betroffene Use Cases:**"
	rejectedMark  = `!!! failure`
)

// checkUseCaseCoverage checks that every decision names at least one use case
// and every use case is named by at least one decision. Rejected entries
// (marked with `!!! failure`) are exempt, and so are the decisions in allow.
func checkUseCaseCoverage(files map[string]*mdFile, defs map[string]idDef, allow []string) []string {
	var probs []string
	named := map[string]bool{}
	for _, path := range sortedKeys(files) {
		for _, s := range files[path].sections {
			if s.def.family != "D" || s.rejected() {
				continue
			}
			var field *mdLine
			for i := range s.lines {
				if strings.HasPrefix(s.lines[i].raw, useCasesField) {
					field = &s.lines[i]
					break
				}
			}
			if field == nil {
				probs = append(probs, fmt.Sprintf("%s:%d: %s has no line %q", rel(path), s.def.line, s.def.id, useCasesField))
				continue
			}
			var ucs []string
			for _, m := range linkRe.FindAllStringSubmatch(field.raw, -1) {
				if im := exactIDRe.FindStringSubmatch(m[1]); im != nil && im[1] == "UC" {
					ucs = append(ucs, m[1])
					named[m[1]] = true
				}
			}
			rest := strings.TrimSpace(strings.TrimPrefix(field.raw, useCasesField))
			exempt := slices.Contains(allow, s.def.id)
			switch {
			case len(ucs) == 0 && !exempt:
				probs = append(probs, fmt.Sprintf("%s:%d: %s names no use case", rel(path), field.num, s.def.id))
			case len(ucs) == 0 && !strings.HasPrefix(rest, "keine"):
				probs = append(probs, fmt.Sprintf("%s:%d: %s is exempt from naming a use case but does not say \"keine\"", rel(path), field.num, s.def.id))
			case len(ucs) > 0 && exempt:
				probs = append(probs, fmt.Sprintf("%s:%d: %s names use cases but is listed in noUseCaseDecisions; remove it there", rel(path), field.num, s.def.id))
			}
		}
	}
	for _, path := range sortedKeys(files) {
		for _, s := range files[path].sections {
			if s.def.family == "UC" && !s.rejected() && !named[s.def.id] {
				probs = append(probs, fmt.Sprintf("%s:%d: %s is not named by any decision", rel(path), s.def.line, s.def.id))
			}
		}
	}
	for _, id := range allow {
		if _, ok := defs[id]; !ok {
			probs = append(probs, fmt.Sprintf("noUseCaseDecisions lists %s, which is not defined", id))
		}
	}
	return probs
}

func (s section) rejected() bool {
	return slices.ContainsFunc(s.lines, func(l mdLine) bool { return strings.HasPrefix(strings.TrimSpace(l.raw), rejectedMark) })
}

// ---------------------------------------------------------------------------
// Anchors

var (
	slugDropRe = regexp.MustCompile(`[^\w\s-]`)
	slugSepRe  = regexp.MustCompile(`[-\s]+`)
)

// slugify computes a heading anchor like the default slugify of the
// Python-Markdown toc extension: NFKD-normalise and drop non-ASCII, drop
// everything except word characters, whitespace and hyphens, lowercase, and
// collapse runs of whitespace and hyphens into one hyphen.
//
// The standard library has no Unicode normalisation, so foldNFKD covers the
// characters the design set uses. A letter, digit or space it does not know is
// an error rather than a silently wrong anchor.
func slugify(s string) (string, error) {
	var b strings.Builder
	for _, r := range s {
		if r <= unicode.MaxASCII {
			b.WriteRune(r)
			continue
		}
		if f, ok := foldNFKD[r]; ok {
			b.WriteString(f)
			continue
		}
		if unicode.IsLetter(r) || unicode.IsDigit(r) || unicode.IsSpace(r) {
			return "", fmt.Errorf("slugify: no NFKD folding for %q in %q; extend foldNFKD", r, s)
		}
		// Punctuation and symbols without a compatibility decomposition
		// (dashes, quotes, arrows) are dropped, as NFKD+ASCII does.
	}
	v := slugDropRe.ReplaceAllString(b.String(), "")
	v = strings.ToLower(strings.TrimSpace(v))
	return slugSepRe.ReplaceAllString(v, "-"), nil
}

// foldNFKD maps non-ASCII runes to the ASCII part of their NFKD
// decomposition. An empty string means the rune decomposes to nothing ASCII.
var foldNFKD = map[rune]string{
	// Latin-1 letters with diacritics.
	'À': "A", 'Á': "A", 'Â': "A", 'Ã': "A", 'Ä': "A", 'Å': "A", 'Ç': "C",
	'È': "E", 'É': "E", 'Ê': "E", 'Ë': "E", 'Ì': "I", 'Í': "I", 'Î': "I", 'Ï': "I",
	'Ñ': "N", 'Ò': "O", 'Ó': "O", 'Ô': "O", 'Õ': "O", 'Ö': "O",
	'Ù': "U", 'Ú': "U", 'Û': "U", 'Ü': "U", 'Ý': "Y",
	'à': "a", 'á': "a", 'â': "a", 'ã': "a", 'ä': "a", 'å': "a", 'ç': "c",
	'è': "e", 'é': "e", 'ê': "e", 'ë': "e", 'ì': "i", 'í': "i", 'î': "i", 'ï': "i",
	'ñ': "n", 'ò': "o", 'ó': "o", 'ô': "o", 'õ': "o", 'ö': "o",
	'ù': "u", 'ú': "u", 'û': "u", 'ü': "u", 'ý': "y", 'ÿ': "y",
	// Letters without a decomposition.
	'ß': "", 'Æ': "", 'æ': "", 'Ø': "", 'ø': "", 'Œ': "", 'œ': "", 'Ð': "", 'ð': "", 'Þ': "", 'þ': "",
	// Compatibility decompositions.
	' ': " ", ' ': " ", ' ': " ", ' ': " ", ' ': " ",
	'ª': "a", 'º': "o", '¹': "1", '²': "2", '³': "3",
	'…': "...", '™': "TM",
	'ﬀ': "ff", 'ﬁ': "fi", 'ﬂ': "fl", 'ﬃ': "ffi", 'ﬄ': "ffl",
}

// ---------------------------------------------------------------------------
// Helpers

// repoRoot walks up from the working directory to the directory that
// contains the design set.
func repoRoot(t testing.TB) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, designDir, "index.md")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("no %s above the working directory", designDir)
		}
		dir = parent
	}
}

// relBase is the directory problems are reported relative to.
var relBase, _ = os.Getwd()

func rel(p string) string {
	if r, err := filepath.Rel(relBase, p); err == nil {
		return r
	}
	return p
}

func blank(s string) string { return strings.Repeat(" ", len(s)) }

func joinAll(root string, rels []string) []string {
	out := make([]string, len(rels))
	for i, r := range rels {
		out[i] = filepath.Join(root, r)
	}
	return out
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
