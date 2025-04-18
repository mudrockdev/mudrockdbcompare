package main

import (
	"database/sql"
	"fmt"
)

// DatabaseAdapter defines the interface for database-specific operations
type DatabaseAdapter interface {
	Connect(connectionString string) (*sql.DB, error)
	GetTableList(db *sql.DB) ([]string, error)
	GetTableSchema(db *sql.DB, tableName string) (TableSchema, error)
	CompareTableDataByChecksum(sourceDB, targetDB *sql.DB, tableName string, schema TableSchema) (bool, error)
	CompareRowCounts(sourceDB, targetDB *sql.DB, tableName string) (int, int, error)
	GetConnectStringFromURL(url string) string
}

// GetAdapter returns the appropriate adapter for the given database type
func GetAdapter(dbType string) (DatabaseAdapter, error) {
	switch dbType {
	case "mysql":
		return &MySQLAdapter{}, nil
	case "postgres":
		return &PostgreSQLAdapter{}, nil
	case "sqlite":
		return &SQLiteAdapter{}, nil
	default:
		return nil, fmt.Errorf("unsupported database type: %s", dbType)
	}
}
