package schema

import (
	"bytes"
	t "text/template"
)

type Version uint32

// Currently not implemented anywhere. Need to implement when creating schema change functionality
type Schema interface {
	Upgrade(start, end Version)
	Downgrade(start, end Version)
	Version() Version
}

func Templater(template string, values map[string]interface{}) string {
	var tpl bytes.Buffer
	templ := t.Must(t.New("template").Parse(template))
	templ.Execute(&tpl, values)
	return tpl.String()
}
