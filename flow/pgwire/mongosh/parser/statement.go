package parser

// Statement represents a parsed MongoDB shell statement.
type Statement struct {
	Type             string    // "collection" or "database"
	Database         string    // database name (if specified)
	Collection       string    // collection name
	Method           string    // method name (find, aggregate, etc.)
	Arguments        []string  // raw argument strings
	Chainers         []Chainer // chained methods
	IsExplain        bool      // whether wrapped in explain()
	ExplainVerbosity string    // explain verbosity level
	IsHelp           bool      // whether this is a help() call
	HelpContext      string    // "global", "database", "collection", or method name
}

// Chainer represents a chained method call.
type Chainer struct {
	Name      string   // chainer name (sort, limit, etc.)
	Arguments []string // raw argument strings
}
