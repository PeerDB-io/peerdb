package utils

import (
	"bytes"
	"fmt"
	"text/template"
)

// ExecuteTemplate executes a text template with the provided parameters and returns the resulting string.
func ExecuteTemplate(tmpl string, params map[string]string) (string, error) {
	t, err := template.New("tmpl").Parse(tmpl)
	if err != nil {
		return "", fmt.Errorf("failed to parse template %q: %w", tmpl, err)
	}

	buf := new(bytes.Buffer)
	if err := t.Execute(buf, params); err != nil {
		return "", fmt.Errorf("failed to execute template %q: %w", tmpl, err)
	}
	res := buf.String()

	return res, nil
}
