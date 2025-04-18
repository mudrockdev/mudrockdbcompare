package main

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
)

func contains(slice []string, item string) bool {
	for _, a := range slice {
		if a == item {
			return true
		}
	}
	return false
}

func formatSize(bytes int64) string {
	const (
		KB = 1024
		MB = 1024 * KB
		GB = 1024 * MB
	)

	switch {
	case bytes >= GB:
		return fmt.Sprintf("%.2f GB", float64(bytes)/float64(GB))
	case bytes >= MB:
		return fmt.Sprintf("%.2f MB", float64(bytes)/float64(MB))
	case bytes >= KB:
		return fmt.Sprintf("%.2f KB", float64(bytes)/float64(KB))
	default:
		return fmt.Sprintf("%d bytes", bytes)
	}
}

// Helper functions
func compareStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	// Create maps for easier comparison
	mapA := make(map[string]bool)
	mapB := make(map[string]bool)

	for _, val := range a {
		mapA[val] = true
	}

	for _, val := range b {
		mapB[val] = true
	}

	// Check if all items in a are in b
	for val := range mapA {
		if !mapB[val] {
			return false
		}
	}

	// Check if all items in b are in a
	for val := range mapB {
		if !mapA[val] {
			return false
		}
	}

	return true
}

func GetDatabaseInfo(adapter DatabaseAdapter, db *sql.DB, connectionString string) (DatabaseInfo, error) {
	info := DatabaseInfo{}

	// Extract host and database name from connection string
	// This is a simplified approach - in practice you'd need proper connection string parsing
	if strings.Contains(connectionString, "@") {
		parts := strings.Split(connectionString, "@")
		if len(parts) > 1 {
			hostPart := strings.Split(parts[1], "/")
			info.Host = hostPart[0]
			if len(hostPart) > 1 {
				info.DatabaseName = strings.Split(hostPart[1], "?")[0]
			}
		}
	} else {
		// For SQLite
		info.Host = "local"
		info.DatabaseName = connectionString
	}

	// Get table count
	tables, err := adapter.GetTableList(db)
	if err != nil {
		return info, err
	}
	info.TableCount = len(tables)

	// Try to estimate database size
	// This is database specific, so we'll need to handle each type
	switch adapter.(type) {
	case *MySQLAdapter:
		var size int64
		err := db.QueryRow("SELECT SUM(data_length + index_length) FROM information_schema.tables WHERE table_schema = DATABASE()").Scan(&size)
		if err == nil {
			info.TotalSize = size
		}
	case *PostgreSQLAdapter:
		var size int64
		err := db.QueryRow("SELECT pg_database_size(current_database())").Scan(&size)
		if err == nil {
			info.TotalSize = size
		}
	case *SQLiteAdapter:
		var size int64
		err := db.QueryRow("SELECT page_count * page_size FROM pragma_page_count(), pragma_page_size()").Scan(&size)
		if err == nil {
			info.TotalSize = size
		}
	}

	return info, nil
}

func compareValues(v1, v2 interface{}) bool {
	// Special case for []byte (typically strings in SQL)
	if b1, ok1 := v1.([]byte); ok1 {
		if b2, ok2 := v2.([]byte); ok2 {
			return string(b1) == string(b2)
		}
		return false
	}

	return reflect.DeepEqual(v1, v2)
}

func formatValue(v interface{}) string {
	if v == nil {
		return "NULL"
	}

	if b, ok := v.([]byte); ok {
		return string(b)
	}

	return fmt.Sprintf("%v", v)
}

// Helper function to get ORDER BY clause for PostgreSQL MD5 hash
func getOrderByClause(schema TableSchema) string {
	if len(schema.PrimaryKeys) > 0 {
		// Use primary keys if available
		orderCols := make([]string, len(schema.PrimaryKeys))
		for i, col := range schema.PrimaryKeys {
			orderCols[i] = "\"" + col + "\""
		}
		return strings.Join(orderCols, ", ")
	} else {
		// Fallback to all columns
		orderCols := make([]string, len(schema.Columns))
		for i, col := range schema.Columns {
			orderCols[i] = "\"" + col.Name + "\""
		}
		return strings.Join(orderCols, ", ")
	}
}
