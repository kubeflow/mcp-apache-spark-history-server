package util

import (
	"fmt"
	"io"
	"reflect"
	"strings"
	"text/tabwriter"
)

// PrintTable writes a slice of structs as a tab-aligned table.
// Column headers and order are determined by `col` struct tags.
func PrintTable(w io.Writer, rows any) error {
	rv := reflect.ValueOf(rows)
	if rv.Kind() != reflect.Slice {
		return fmt.Errorf("PrintTable: expected slice, got %s", rv.Kind())
	}
	if rv.Len() == 0 {
		return nil
	}

	rt := rv.Type().Elem()
	// collect fields with col tags
	type colInfo struct {
		index int
		name  string
	}
	var cols []colInfo
	for i := 0; i < rt.NumField(); i++ {
		if tag := rt.Field(i).Tag.Get("col"); tag != "" {
			cols = append(cols, colInfo{i, tag})
		}
	}
	if len(cols) == 0 {
		return fmt.Errorf("PrintTable: no `col` tags found on %s", rt.Name())
	}

	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)

	// header
	headers := make([]string, len(cols))
	for i, c := range cols {
		headers[i] = c.name
	}
	_, _ = fmt.Fprintln(tw, strings.Join(headers, "\t"))

	// rows
	vals := make([]string, len(cols))
	for i := 0; i < rv.Len(); i++ {
		row := rv.Index(i)
		for j, c := range cols {
			vals[j] = fmt.Sprint(row.Field(c.index).Interface())
		}
		_, _ = fmt.Fprintln(tw, strings.Join(vals, "\t"))
	}

	return tw.Flush()
}
