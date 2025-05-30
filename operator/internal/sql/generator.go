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

// GenerateTableSQL generates SQL for table creation
func (g *Generator) GenerateTableSQL(table *ducklakev1alpha1.DuckLakeTable) (string, error) {
	var sb strings.Builder

	// Begin transaction
	sb.WriteString("BEGIN TRANSACTION;\n\n")

	// Create table
	sb.WriteString(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n", table.Spec.Name))

	// Add columns
	for i, col := range table.Spec.Columns {
		sb.WriteString(fmt.Sprintf("  %s %s", col.Name, col.Type))
		if !col.Nullable {
			sb.WriteString(" NOT NULL")
		}
		if col.Comment != "" {
			sb.WriteString(fmt.Sprintf(" COMMENT '%s'", strings.ReplaceAll(col.Comment, "'", "''")))
		}
		if i < len(table.Spec.Columns)-1 {
			sb.WriteString(",")
		}
		sb.WriteString("\n")
	}
	sb.WriteString(")")

	// Add table comment
	if table.Spec.Comment != "" {
		sb.WriteString(fmt.Sprintf(" COMMENT '%s'", strings.ReplaceAll(table.Spec.Comment, "'", "''")))
	}
	sb.WriteString(";\n\n")

	// Configure Parquet settings
	sb.WriteString(fmt.Sprintf("COPY %s TO 's3://%s/%s' (\n", table.Spec.Name, table.Spec.ObjectStore.Bucket, table.Spec.Location))
	sb.WriteString(fmt.Sprintf("  FORMAT 'parquet',\n"))
	sb.WriteString(fmt.Sprintf("  COMPRESSION '%s'", table.Spec.Format.Compression))

	// Add partitioning
	if len(table.Spec.Format.Partitioning) > 0 {
		sb.WriteString(",\n  PARTITION_BY (")
		for i, col := range table.Spec.Format.Partitioning {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(col)
		}
		sb.WriteString(")")
	}
	sb.WriteString("\n);\n\n")

	// Commit transaction
	sb.WriteString("COMMIT;")

	return sb.String(), nil
}

// GenerateInsertSQL generates SQL for data insertion
func (g *Generator) GenerateInsertSQL(table *ducklakev1alpha1.DuckLakeTable, data string) (string, error) {
	var sb strings.Builder

	// Begin transaction
	sb.WriteString("BEGIN TRANSACTION;\n\n")

	// Insert data
	if table.Spec.Mode == ducklakev1alpha1.TableModeOverwrite {
		sb.WriteString(fmt.Sprintf("TRUNCATE TABLE %s;\n\n", table.Spec.Name))
	}

	sb.WriteString(fmt.Sprintf("COPY %s FROM '%s';\n\n", table.Spec.Name, data))

	// Commit transaction
	sb.WriteString("COMMIT;")

	return sb.String(), nil
}

// GenerateQuerySQL generates SQL for data querying
func (g *Generator) GenerateQuerySQL(table *ducklakev1alpha1.DuckLakeTable, query string) (string, error) {
	var sb strings.Builder

	// Begin transaction
	sb.WriteString("BEGIN TRANSACTION;\n\n")

	// Execute query
	sb.WriteString(query)
	sb.WriteString(";\n\n")

	// Commit transaction
	sb.WriteString("COMMIT;")

	return sb.String(), nil
}
