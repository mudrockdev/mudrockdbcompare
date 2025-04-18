package main

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

// SQLiteAdapter implements DatabaseAdapter for SQLite
type SQLiteAdapter struct{}

func (a *SQLiteAdapter) Connect(connectionString string) (*sql.DB, error) {
	return sql.Open("sqlite3", connectionString)
}

func (a *SQLiteAdapter) GetConnectStringFromURL(url string) string {
	// For SQLite, remove sqlite:// prefix if present
	if strings.HasPrefix(url, "sqlite://") {
		return url[9:]
	}
	return url
}

func (a *SQLiteAdapter) GetTableList(db *sql.DB) ([]string, error) {
	rows, err := db.Query("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, err
		}
		tables = append(tables, tableName)
	}

	return tables, nil
}

func (a *SQLiteAdapter) GetTableSchema(db *sql.DB, tableName string) (TableSchema, error) {
	tableSchema := TableSchema{Name: tableName}

	// Get columns and schema
	rows, err := db.Query(fmt.Sprintf("PRAGMA table_info(%s)", tableName))
	if err != nil {
		return tableSchema, err
	}
	defer rows.Close()

	for rows.Next() {
		var cid int
		var name, typeName string
		var notNull, pk int
		var dfltValue sql.NullString

		if err := rows.Scan(&cid, &name, &typeName, &notNull, &dfltValue, &pk); err != nil {
			return tableSchema, err
		}

		col := ColumnSchema{
			Name:     name,
			DataType: typeName,
			Default:  dfltValue,
		}

		if notNull == 0 {
			col.Nullable = "YES"
		} else {
			col.Nullable = "NO"
		}

		if pk > 0 {
			col.Key = "PRI"
			tableSchema.PrimaryKeys = append(tableSchema.PrimaryKeys, name)
		}

		tableSchema.Columns = append(tableSchema.Columns, col)
	}

	// Get indexes
	indexes, err := db.Query(fmt.Sprintf("PRAGMA index_list(%s)", tableName))
	if err != nil {
		return tableSchema, err
	}
	defer indexes.Close()

	for indexes.Next() {
		var seq int
		var indexName string
		var unique int
		var origin, partial string

		if err := indexes.Scan(&seq, &indexName, &unique, &origin, &partial); err != nil {
			return tableSchema, err
		}

		// Get columns in this index
		indexCols, err := db.Query(fmt.Sprintf("PRAGMA index_info(%s)", indexName))
		if err != nil {
			return tableSchema, err
		}
		defer indexCols.Close()

		for indexCols.Next() {
			var seqno, cid int
			var colName string

			if err := indexCols.Scan(&seqno, &cid, &colName); err != nil {
				return tableSchema, err
			}

			indexSchema := IndexSchema{
				Name:       indexName,
				ColumnName: colName,
				NonUnique:  1 - unique, // Convert SQLite's unique (1=unique) to MySQL's non_unique (0=unique)
			}

			tableSchema.Indexes = append(tableSchema.Indexes, indexSchema)
		}
	}

	// Get foreign keys
	fkeys, err := db.Query(fmt.Sprintf("PRAGMA foreign_key_list(%s)", tableName))
	if err != nil {
		return tableSchema, err
	}
	defer fkeys.Close()

	for fkeys.Next() {
		var id, seq int
		var table, from, to string
		var onUpdate, onDelete, match string

		if err := fkeys.Scan(&id, &seq, &table, &from, &to, &onUpdate, &onDelete, &match); err != nil {
			return tableSchema, err
		}

		fk := ForeignKeySchema{
			Name:             fmt.Sprintf("fk_%s_%d", tableName, id), // SQLite doesn't name FKs, so we create a name
			ColumnName:       from,
			ReferencedTable:  table,
			ReferencedColumn: to,
		}

		tableSchema.ForeignKeys = append(tableSchema.ForeignKeys, fk)
	}

	return tableSchema, nil
}

func (a *SQLiteAdapter) CompareTableDataByChecksum(sourceDB, targetDB *sql.DB, tableName string, schema TableSchema) (bool, error) {
	fmt.Printf("Comparing data for table '%s'...\n", tableName)

	// SQLite doesn't have a built-in checksum function
	// Instead, we can compare row counts and then sample a few rows if needed
	sourceCount, targetCount, err := a.CompareRowCounts(sourceDB, targetDB, tableName)
	if err != nil {
		return false, err
	}

	if sourceCount != targetCount {
		fmt.Printf("Table '%s' has different row counts: source=%d, target=%d\n",
			tableName, sourceCount, targetCount)
		return true, nil
	}

	// If row counts are the same, check the total changes by summarizing all values
	query := fmt.Sprintf("SELECT total(rowid) FROM %s", tableName)

	var sourceSum, targetSum int64

	err = sourceDB.QueryRow(query).Scan(&sourceSum)
	if err != nil {
		fmt.Printf("Error getting source sum: %v\n", err)
		return false, err
	}

	err = targetDB.QueryRow(query).Scan(&targetSum)
	if err != nil {
		fmt.Printf("Error getting target sum: %v\n", err)
		return false, err
	}

	if sourceSum != targetSum {
		fmt.Printf("Table '%s' has different data (row sums differ)\n", tableName)
		return true, nil
	} else {
		fmt.Printf("Table '%s' has likely identical data (same row count and sums)\n", tableName)
		return false, nil
	}
}

func (a *SQLiteAdapter) CompareRowCounts(sourceDB, targetDB *sql.DB, tableName string) (int, int, error) {
	var sourceCount, targetCount int

	sourceRow := sourceDB.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM \"%s\"", tableName))
	if err := sourceRow.Scan(&sourceCount); err != nil {
		return 0, 0, err
	}

	targetRow := targetDB.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM \"%s\"", tableName))
	if err := targetRow.Scan(&targetCount); err != nil {
		return 0, 0, err
	}

	return sourceCount, targetCount, nil
}
