package expr

import (
	"bytes"
	"fmt"
)

// A QueryPart represents a section of a parsed SQL statement, which forms
// a complete query when processed together with its surrounding parts, in
// their correct order.
type queryPart interface {
	// String returns the part's representation for debugging purposes.
	String() string

	// marker method
	part()
}

// FullName represents a table column or a Go type identifier.
type fullName struct {
	prefix, name string
}

func (fn fullName) String() string {
	if fn.prefix == "" {
		return fn.name
	} else if fn.name == "" {
		return fn.prefix
	}
	return fn.prefix + "." + fn.name
}

// inputPart represents a named parameter that will be sent to the database
// while performing the query.
type inputPart struct {
	cols   []string
	source []fullName
}

func (p *inputPart) String() string {
	return fmt.Sprintf("Input[%+v %+v]", p.cols, p.source)
}

func (p *inputPart) raw() string {
	var b bytes.Buffer
	if len(p.cols) > 0 {
		b.WriteString("(")
		for i, c := range p.cols {
			b.WriteString(c)
			if i < len(p.cols)-1 {
				b.WriteString(", ")
			}
		}
		b.WriteString(") VALUES ")
	}

	b.WriteString("(")
	for i, s := range p.source {
		b.WriteString("$")
		b.WriteString(s.String())
		if i < len(p.source)-1 {
			b.WriteString(", ")
		}
	}
	b.WriteString(")")

	return b.String()
}

func (p *inputPart) part() {}

// outputPart represents a named target output variable in the SQL expression,
// as well as the source table and column where it will be read from.
type outputPart struct {
	source []fullName
	target []fullName
}

func (p *outputPart) String() string {
	return fmt.Sprintf("Output[%+v %+v]", p.source, p.target)
}

func (p *outputPart) raw() string {
	var b bytes.Buffer
	if len(p.source) > 0 {
		b.WriteString("(")
		for i, s := range p.source {
			b.WriteString(s.String())
			if i < len(p.source)-1 {
				b.WriteString(", ")
			}
		}
		b.WriteString(") AS ")
	}
	if len(p.target) > 1 {
		b.WriteString("(")
	}
	for i, t := range p.target {
		b.WriteString("&")
		b.WriteString(t.String())
		if i < len(p.target)-1 {
			b.WriteString(", ")
		}
	}
	if len(p.target) > 1 {
		b.WriteString(")")
	}
	return b.String()
}

func (p *outputPart) part() {}

// bypassPart represents a part of the expression that we want to pass to the
// backend database verbatim.
type bypassPart struct {
	chunk string
}

func (p *bypassPart) String() string {
	return "Bypass[" + p.chunk + "]"
}

func (p *bypassPart) part() {}
