package etl

// Transform is a named, reusable pipeline transformation. It packages a
// pipeline-to-pipeline function with a descriptive name, making it easy to
// share standard processing steps across multiple pipelines.
//
//	normalize := etl.NewTransform("normalize", func(p etl.Pipeline) etl.Pipeline {
//	    return p.
//	        Then(func(t table.Table) table.Table { return t.DropEmpty("id") }).
//	        Then(func(t table.Table) table.Table { return t.FillEmpty("region", "unknown") })
//	})
//
//	p.Apply(normalize).Apply(validate).Unwrap()
type Transform struct {
	// Name identifies this transform in traces and error messages.
	Name string
	fn   func(Pipeline) Pipeline
}

// NewTransform creates a named Transform from a pipeline-to-pipeline function.
func NewTransform(name string, fn func(Pipeline) Pipeline) Transform {
	return Transform{Name: name, fn: fn}
}

// Apply applies t to the pipeline. If tracing is active, a StepRecord is
// appended with the transform's name and the aggregate row delta.
func (p Pipeline) Apply(t Transform) Pipeline {
	if p.trace == nil || p.r.IsErr() {
		return t.fn(p)
	}
	inputRows := p.r.Unwrap().Len()
	out := t.fn(p)
	if out.r.IsOk() {
		*p.trace = append(*p.trace, StepRecord{
			Name:       t.Name,
			InputRows:  inputRows,
			OutputRows: out.r.Unwrap().Len(),
		})
	}
	return out
}
