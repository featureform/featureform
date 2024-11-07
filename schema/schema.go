// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

package schema

import (
	"bytes"
	"fmt"
	"regexp"
	"strings"
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

func ParseTemplate(template, input string) (map[string]string, error) {
	pattern, err := convertTemplateToRegex(template)
	if err != nil {
		return nil, err
	}

	re := regexp.MustCompile(pattern)
	match := re.FindStringSubmatch(input)
	if match == nil {
		return nil, fmt.Errorf("input string does not match the pattern")
	}

	result := make(map[string]string)
	for i, name := range re.SubexpNames() {
		if i > 0 && name != "" {
			result[name] = match[i]
		}
	}
	return result, nil
}

// convertTemplateToRegex converts a Go template string to a regex pattern with named capture groups
func convertTemplateToRegex(templateStr string) (string, error) {
	// Regular expression to match template placeholders like {{ .TaskRunID }}
	rePattern := regexp.MustCompile(`{{\s*\.\s*(\w+)\s*}}`)
	// Replace placeholders with named capture groups
	regexStr := rePattern.ReplaceAllStringFunc(templateStr, func(m string) string {
		// Extract the placeholder name
		placeholder := strings.TrimSpace(m[4 : len(m)-2]) // Adjust index to skip '{{ .'
		return fmt.Sprintf(`(?P<%s>\w+)`, placeholder)
	})
	return regexStr, nil
}
