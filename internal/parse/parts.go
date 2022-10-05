package parse

// A queryPart represents a section of a parsed SQL statement, which forms
// a complete query when processed together with its surrounding parts, in
// their correct order.
type queryPart interface {
	// String returns the part's representation for debugging purposes.
	String() string

	// ToSQL returns the SQL representation of the part.
	ToSQL() string
}

type M map[string]any

// FullName represents a name made up of two parts delimited by a full stop
// '.'. It is used as a representation for Go types in input and output
// expressions where Prefix is a struct name and Name and is the field name. It
// is also used to represent columns where Prefix is the table name and Name is
// the column title.
type FullName struct {
	Prefix, Name string
}

func (fn FullName) String() string {
	if fn.Prefix == "" {
		return fn.Name
	} else if fn.Name == "" {
		return fn.Prefix
	}
	return fn.Prefix + "." + fn.Name
}

// InputPart represents a named parameter that will be send to the database
// while performing the query.
type InputPart struct {
	FullName
}

func (p *InputPart) String() string {
	return "InputPart[" + p.FullName.String() + "]"
}

func (p *InputPart) ToSQL() string {
	return "?"
}

// OutputPart represents an expression to be used as output in our SDL.
type OutputPart struct {
	Columns []FullName
	Target  FullName
}

func (p *OutputPart) String() string {
	var colString string
	for _, col := range p.Columns {
		colString = colString + col.String() + " "
	}
	if len(colString) >= 2 {
		colString = colString[:len(colString)-1]
	}
	return "OutputPart[Columns:" + colString + " Target:" + p.Target.String() + "]"
}

func (p *OutputPart) ToSQL() string {
	// The &Type.Field syntax is part of the DSL but not SQL so we can not
	// print that. We do need to print the columns though (if any)
	// There are two cases here
	var out string
	if len(p.Columns) != 0 {
		// Case 1
		// foo as &Type.Field --> print foo
		for i, c := range p.Columns {
			if i > 0 {
				out = out + ", "
			}
			out = out + c.String()
		}
		return out
	}

	// Case 2: No AS just a Go Struct with a field
	// &Type.column --> column
	if p.Target.Name != "" {
		return p.Target.Name
	}

	// Case 3: A Go struct, map or var with no field
	// &Type --> *
	return "*"
}

// BypassPart represents a part of the SDL that we want to pass to the
// backend database verbatim.
type BypassPart struct {
	Chunk string
}

func (p *BypassPart) String() string {
	return "BypassPart[" + p.Chunk + "]"
}

func (p *BypassPart) ToSQL() string {
	return p.Chunk
}
