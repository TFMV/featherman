package cmd

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"

	"gopkg.in/yaml.v3"
)

// OutputFormat represents the output format type
type OutputFormat string

const (
	OutputTable OutputFormat = "table"
	OutputJSON  OutputFormat = "json"
	OutputYAML  OutputFormat = "yaml"
	OutputCSV   OutputFormat = "csv"
)

// Outputter handles different output formats
type Outputter struct {
	format OutputFormat
	writer *os.File
}

// NewOutputter creates a new outputter with the specified format
func NewOutputter(format string) *Outputter {
	return &Outputter{
		format: OutputFormat(format),
		writer: os.Stdout,
	}
}

// PrintTable prints data as a formatted table
func (o *Outputter) PrintTable(headers []string, rows [][]string) error {
	switch o.format {
	case OutputTable:
		return o.printAsTable(headers, rows)
	case OutputJSON:
		return o.printAsJSON(headers, rows)
	case OutputYAML:
		return o.printAsYAML(headers, rows)
	case OutputCSV:
		return o.printAsCSV(headers, rows)
	default:
		return fmt.Errorf("unsupported output format: %s", o.format)
	}
}

// PrintObject prints a single object in the specified format
func (o *Outputter) PrintObject(obj interface{}) error {
	switch o.format {
	case OutputTable:
		return o.printObjectAsTable(obj)
	case OutputJSON:
		return o.printObjectAsJSON(obj)
	case OutputYAML:
		return o.printObjectAsYAML(obj)
	case OutputCSV:
		return fmt.Errorf("CSV format not supported for single objects")
	default:
		return fmt.Errorf("unsupported output format: %s", o.format)
	}
}

func (o *Outputter) printAsTable(headers []string, rows [][]string) error {
	// Simple table implementation since tablewriter API seems different
	if len(headers) > 0 {
		// Print headers
		for i, header := range headers {
			fmt.Fprint(o.writer, header)
			if i < len(headers)-1 {
				fmt.Fprint(o.writer, "\t")
			}
		}
		fmt.Fprint(o.writer, "\n")

		// Print separator
		for i := range headers {
			fmt.Fprint(o.writer, strings.Repeat("-", len(headers[i])))
			if i < len(headers)-1 {
				fmt.Fprint(o.writer, "\t")
			}
		}
		fmt.Fprint(o.writer, "\n")
	}

	// Print rows
	for _, row := range rows {
		for i, cell := range row {
			fmt.Fprint(o.writer, cell)
			if i < len(row)-1 {
				fmt.Fprint(o.writer, "\t")
			}
		}
		fmt.Fprint(o.writer, "\n")
	}

	return nil
}

func (o *Outputter) printObjectAsTable(obj interface{}) error {
	headers, rows := o.objectToTableData(obj)
	return o.printAsTable(headers, [][]string{rows})
}

func (o *Outputter) printAsJSON(headers []string, rows [][]string) error {
	var objects []map[string]interface{}

	for _, row := range rows {
		obj := make(map[string]interface{})
		for i, header := range headers {
			if i < len(row) {
				obj[header] = row[i]
			}
		}
		objects = append(objects, obj)
	}

	encoder := json.NewEncoder(o.writer)
	encoder.SetIndent("", "  ")
	return encoder.Encode(objects)
}

func (o *Outputter) printObjectAsJSON(obj interface{}) error {
	encoder := json.NewEncoder(o.writer)
	encoder.SetIndent("", "  ")
	return encoder.Encode(obj)
}

func (o *Outputter) printAsYAML(headers []string, rows [][]string) error {
	var objects []map[string]interface{}

	for _, row := range rows {
		obj := make(map[string]interface{})
		for i, header := range headers {
			if i < len(row) {
				obj[header] = row[i]
			}
		}
		objects = append(objects, obj)
	}

	encoder := yaml.NewEncoder(o.writer)
	defer encoder.Close()
	return encoder.Encode(objects)
}

func (o *Outputter) printObjectAsYAML(obj interface{}) error {
	encoder := yaml.NewEncoder(o.writer)
	defer encoder.Close()
	return encoder.Encode(obj)
}

func (o *Outputter) printAsCSV(headers []string, rows [][]string) error {
	writer := csv.NewWriter(o.writer)
	defer writer.Flush()

	// Write header
	if err := writer.Write(headers); err != nil {
		return err
	}

	// Write rows
	for _, row := range rows {
		if err := writer.Write(row); err != nil {
			return err
		}
	}

	return nil
}

func (o *Outputter) objectToTableData(obj interface{}) ([]string, []string) {
	var headers []string
	var values []string

	v := reflect.ValueOf(obj)
	t := reflect.TypeOf(obj)

	// Handle pointer types
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
		t = t.Elem()
	}

	// Handle struct types
	if v.Kind() == reflect.Struct {
		for i := 0; i < v.NumField(); i++ {
			field := t.Field(i)
			value := v.Field(i)

			// Skip unexported fields
			if !field.IsExported() {
				continue
			}

			// Use field name as header
			headers = append(headers, field.Name)
			values = append(values, fmt.Sprintf("%v", value.Interface()))
		}
	} else {
		// For non-struct types, just use the value
		headers = []string{"Value"}
		values = []string{fmt.Sprintf("%v", obj)}
	}

	return headers, values
}

// PrintSuccess prints a success message
func (o *Outputter) PrintSuccess(message string) {
	fmt.Fprintf(o.writer, "✓ %s\n", message)
}

// PrintError prints an error message
func (o *Outputter) PrintError(message string) {
	fmt.Fprintf(os.Stderr, "✗ %s\n", message)
}

// PrintWarning prints a warning message
func (o *Outputter) PrintWarning(message string) {
	fmt.Fprintf(o.writer, "⚠ %s\n", message)
}

// PrintInfo prints an info message
func (o *Outputter) PrintInfo(message string) {
	fmt.Fprintf(o.writer, "ℹ %s\n", message)
}
