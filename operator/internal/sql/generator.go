package sql

import (
	"bytes"
	"fmt"
	"strings"
	"text/template"

	ducklakev1alpha1 "github.com/TFMV/featherman/operator/api/v1alpha1"
)

// Generator generates SQL statements for DuckDB operations
type Generator struct {
	templates *template.Template
}

// NewGenerator creates a new SQL generator
func NewGenerator() *Generator {
	templates := template.New("sql")
	template.Must(templates.New("create_table").Parse(`
CREATE TABLE IF NOT EXISTS {{ .Name }} (
	{{- range $i, $col := .Columns }}
	{{ if $i }},{{ end }}
	{{ $col.Name }} {{ $col.Type }}{{ if not $col.Nullable }} NOT NULL{{ end }}
	{{- end }}
)
{{- if .Comment }}
COMMENT ON TABLE {{ .Name }} IS '{{ .Comment }}'
{{- end }}
;`))

	template.Must(templates.New("attach_parquet").Parse(`
CALL ducklake.attach_parquet(
	'{{ .Location }}',
	'{{ .TableName }}',
	{{- if .Partitioning }}
	ARRAY[{{ range $i, $p := .Partitioning }}{{ if $i }}, {{ end }}'{{ $p }}'{{ end }}]
	{{- else }}
	NULL
	{{- end }}
);`))

	return &Generator{
		templates: templates,
	}
}

// CreateTableSQL generates SQL for creating a table
func (g *Generator) CreateTableSQL(table *ducklakev1alpha1.DuckLakeTable) (string, error) {
	var buf bytes.Buffer
	err := g.templates.ExecuteTemplate(&buf, "create_table", map[string]interface{}{
		"Name":    table.Spec.Name,
		"Columns": table.Spec.Columns,
		"Comment": table.Spec.Comment,
	})
	if err != nil {
		return "", fmt.Errorf("failed to generate create table SQL: %w", err)
	}
	return buf.String(), nil
}

// AttachParquetSQL generates SQL for attaching Parquet files
func (g *Generator) AttachParquetSQL(table *ducklakev1alpha1.DuckLakeTable, location string) (string, error) {
	var buf bytes.Buffer
	err := g.templates.ExecuteTemplate(&buf, "attach_parquet", map[string]interface{}{
		"TableName":    table.Spec.Name,
		"Location":     location,
		"Partitioning": table.Spec.Format.Partitioning,
	})
	if err != nil {
		return "", fmt.Errorf("failed to generate attach parquet SQL: %w", err)
	}
	return buf.String(), nil
}

// ColumnDefinitionToSQL converts a column definition to SQL type
func ColumnDefinitionToSQL(col ducklakev1alpha1.ColumnDefinition) string {
	var parts []string
	parts = append(parts, col.Name)
	parts = append(parts, string(col.Type))
	if !col.Nullable {
		parts = append(parts, "NOT NULL")
	}
	if col.Comment != "" {
		parts = append(parts, fmt.Sprintf("COMMENT '%s'", col.Comment))
	}
	return strings.Join(parts, " ")
}

// TransactionSQL wraps SQL statements in a transaction
func TransactionSQL(statements ...string) string {
	var buf bytes.Buffer
	buf.WriteString("BEGIN TRANSACTION;\n")
	for _, stmt := range statements {
		buf.WriteString(stmt)
		buf.WriteString("\n")
	}
	buf.WriteString("COMMIT;")
	return buf.String()
}
