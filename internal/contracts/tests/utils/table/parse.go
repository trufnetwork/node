package table

import (
	"fmt"
	"regexp"
	"strings"
	"unicode"

	"github.com/samber/lo"
)

type Table struct {
	Headers []string
	Rows    [][]string
}

func TableFromMarkdown(table string) (*Table, error) {
	lines := strings.Split(strings.TrimSpace(table), "\n")
	// filter out empty lines and comments
	lines = lo.Filter(lines, func(line string, _ int) bool {
		return strings.TrimSpace(line) != "" && !strings.HasPrefix(strings.TrimSpace(line), "#")
	})
	if len(lines) < 3 {
		return nil, fmt.Errorf("not enough lines for a valid table: %d", len(lines))
	}

	// Extract headers
	headers := strings.Split(strings.Trim(lines[0], "|"), "|")
	for i := range headers {
		headers[i] = strings.TrimSpace(headers[i])
	}

	// check if there's a separator line
	separator := parseRow(lines[1])
	separatorRegex := regexp.MustCompile(`^[-|]+$`)
	if !separatorRegex.MatchString(strings.Join(separator, "")) {
		return nil, fmt.Errorf("not a valid table: separator line %s is not valid", lines[1])
	}

	// Skip the separator line
	data := lines[2:]

	result := Table{
		Headers: headers,
		Rows:    [][]string{},
	}
	for _, line := range data {
		row := parseRow(line)
		if len(row) != len(headers) {
			return nil, fmt.Errorf("not a valid table: row %s has %d columns, expected %d", line, len(row), len(headers))

		}

		result.Rows = append(result.Rows, row)
	}

	return &result, nil
}

func parseRow(row string) []string {
	trimmedLine := strings.TrimFunc(row, unicode.IsSpace)
	trimmedLine = strings.Trim(trimmedLine, "|")
	fields := regexp.MustCompile(`\s*\|\s*`).Split(trimmedLine, -1)
	for i := range fields {
		fields[i] = strings.TrimSpace(fields[i])
	}
	// If the last field starts with '#', remove it
	if len(fields) > 0 && strings.HasPrefix(fields[len(fields)-1], "#") {
		fields = fields[:len(fields)-1]
	}
	return fields
}
