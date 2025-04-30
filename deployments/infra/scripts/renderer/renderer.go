package renderer

import (
	"bytes"
	"embed"
	"fmt"
	"sync"
	"text/template"

	"github.com/Masterminds/sprig/v3"
)

const templateDir = "templates/"

//go:embed templates/*.tmpl
var tplFS embed.FS

var (
	tplCache = sync.Map{}
)

// exec executes a pre-parsed template.
func exec(t *template.Template, data any) (string, error) {
	var buf bytes.Buffer
	if err := t.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("executing template %q: %w", t.Name(), err)
	}
	return buf.String(), nil
}

// Render merges the named template file with data, using a cache.
func Render(name TemplateName, data any) (string, error) {
	strName := string(name)

	// Check cache first
	if tVal, ok := tplCache.Load(strName); ok {
		if t, okTpl := tVal.(*template.Template); okTpl {
			return exec(t, data)
		}
		return "", fmt.Errorf("invalid type found in template cache for %q", strName)
	}

	// If not in cache, parse and cache
	path := templateDir + strName
	// Use the default sprig func map which includes the correct errorf
	t, err := template.New(strName).
		Funcs(sprig.TxtFuncMap()).
		ParseFS(tplFS, path)
	if err != nil {
		return "", fmt.Errorf("parsing template %q: %w", path, err)
	}

	// Store in cache before executing
	tplCache.Store(strName, t)

	return exec(t, data)
}
