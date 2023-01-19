package expr

import (
	"bytes"
	"fmt"
	"regexp"
)

// A QueryPart represents a section of a parsed SQL statement, which forms
// a complete query when processed together with its surrounding parts, in
// their correct order.
type queryPart interface {
	// String returns the part's representation for debugging purposes.
	String() string

	// ToSQL returns the SQL representation of the part.
	toSQL([]string, int) string
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
	source fullName
}

func (p *inputPart) String() string {
	return fmt.Sprintf("Input[%+v]", p.source)
}

func (p *inputPart) toSQL([]string, int) string {
	return "?"
}

// outputPart represents a named target output variable in the SQL expression,
// as well as the source table and column where it will be read from.
type outputPart struct {
	source []fullName
	target []fullName
}

func (p *outputPart) String() string {
	return fmt.Sprintf("Output[%+v %+v]", p.source, p.target)
}

var alphaNum = regexp.MustCompile("[^a-zA-Z0-9]+")

func (p *outputPart) toSQL(cs []string, n int) string {
	var out bytes.Buffer
	for i, c := range cs {
		out.WriteString(c)
		out.WriteString(" AS ")
		c := alphaNum.ReplaceAllString(c, "")
		out.WriteString("_sqlair_" + c + fmt.Sprintf("_%d", n))
		if i != len(cs)-1 {
			out.WriteString(", ")
		}
		n++
	}
	return out.String()
}

// bypassPart represents a part of the expression that we want to pass to the
// backend database verbatim.
type bypassPart struct {
	chunk string
}

func (p *bypassPart) String() string {
	return "Bypass[" + p.chunk + "]"
}

func (p *bypassPart) toSQL([]string, int) string {
	return p.chunk
}
