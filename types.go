package main

import "database/sql"

type DatabaseInfo struct {
	Host         string
	DatabaseName string
	TableCount   int
	TotalSize    int64 // in bytes
}

type ComparisonSummary struct {
	DifferentTables    []string
	DifferentRowCounts map[string]struct{ Source, Target int }
	TotalTablesChecked int
	SchemaOnly         bool
}

type TableSchema struct {
	Name        string
	Columns     []ColumnSchema
	Indexes     []IndexSchema
	ForeignKeys []ForeignKeySchema
	PrimaryKeys []string
}

type ColumnSchema struct {
	Name     string
	DataType string
	Nullable string
	Key      string
	Default  sql.NullString
	Extra    string
}

type IndexSchema struct {
	Name       string
	ColumnName string
	NonUnique  int
}

type ForeignKeySchema struct {
	Name             string
	ColumnName       string
	ReferencedTable  string
	ReferencedColumn string
}
