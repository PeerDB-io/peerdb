package clickhouse

// ExecTraits describes the failed execution behind an ExecError.
type ExecTraits struct {
	// View is set when the exception was raised while pushing to a view.
	View bool
	// JSONParse is set when ClickHouse could not parse a value as JSON.
	JSONParse bool
	// JSONCast is set when the failed query itself converts values to the JSON type.
	JSONCast bool
}

// ExecError is a ClickHouse exception returned by Exec, together with what Exec knows about the failed query.
type ExecError struct {
	err    error
	traits ExecTraits
}

func NewExecError(err error, traits ExecTraits) *ExecError {
	return &ExecError{err: err, traits: traits}
}

func (e *ExecError) Error() string {
	return "ClickHouse exec error: " + e.err.Error()
}

func (e *ExecError) Unwrap() error {
	return e.err
}

func (e *ExecError) View() bool {
	return e.traits.View
}

func (e *ExecError) JSONParse() bool {
	return e.traits.JSONParse
}

func (e *ExecError) JSONCast() bool {
	return e.traits.JSONCast
}
