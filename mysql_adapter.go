package main

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

// MySQLAdapter implements DatabaseAdapter for MySQL
type MySQLAdapter struct{}

func (a *MySQLAdapter) Connect(connectionString string) (*sql.DB, error) {
	if !strings.Contains(connectionString, "tcp(") && strings.Contains(connectionString, "@") {
		parts := strings.SplitN(connectionString, "@", 2)
		if len(parts) == 2 {
			userPass := parts[0]
			hostDBPart := parts[1]

			// Split hostDBPart by first slash to separate host:port from dbname
			hostPortDB := strings.SplitN(hostDBPart, "/", 2)
			if len(hostPortDB) == 2 {
				hostPort := hostPortDB[0]
				dbname := hostPortDB[1]

				// Reconstruct with tcp() wrapper for the driver
				connectionString = fmt.Sprintf("%s@tcp(%s)/%s", userPass, hostPort, dbname)
			}
		}
	}

	return sql.Open("mysql", connectionString)
}

func (a *MySQLAdapter) GetConnectStringFromURL(url string) string {
	// For MySQL, remove mysql:// prefix if present
	if strings.HasPrefix(url, "mysql://") {
		return url[8:]
	}
	return url
}

func (a *MySQLAdapter) GetTableList(db *sql.DB) ([]string, error) {
	rows, err := db.Query("SHOW TABLES")
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

func (a *MySQLAdapter) GetTableSchema(db *sql.DB, tableName string) (TableSchema, error) {
	tableSchema := TableSchema{Name: tableName}

	// Get columns
	columns, err := db.Query(fmt.Sprintf("DESCRIBE `%s`", tableName))
	if err != nil {
		return tableSchema, err
	}
	defer columns.Close()

	for columns.Next() {
		var col ColumnSchema
		var fieldType string
		var null string
		var key string
		var defaultValue sql.NullString
		var extra string

		if err := columns.Scan(&col.Name, &fieldType, &null, &key, &defaultValue, &extra); err != nil {
			return tableSchema, err
		}

		col.DataType = fieldType
		col.Nullable = null
		col.Key = key
		col.Default = defaultValue
		col.Extra = extra

		// Track primary keys
		if key == "PRI" {
			tableSchema.PrimaryKeys = append(tableSchema.PrimaryKeys, col.Name)
		}

		tableSchema.Columns = append(tableSchema.Columns, col)
	}

	// Get indexes
	indexes, err := db.Query(fmt.Sprintf("SHOW INDEX FROM `%s`", tableName))
	if err != nil {
		return tableSchema, err
	}
	defer indexes.Close()

	for indexes.Next() {
		var indexSchema IndexSchema
		var tableName, columnName, indexName string
		var nonUnique int
		var temp sql.NullString // For columns we don't need

		// Modified to scan 14 columns instead of 12
		if err := indexes.Scan(
			&tableName, &nonUnique, &indexName, &temp, &columnName,
			&temp, &temp, &temp, &temp, &temp, &temp, &temp,
			&temp, &temp); err != nil {
			return tableSchema, err
		}

		indexSchema.Name = indexName
		indexSchema.ColumnName = columnName
		indexSchema.NonUnique = nonUnique
		tableSchema.Indexes = append(tableSchema.Indexes, indexSchema)
	}

	// Get foreign keys
	foreignKeys, err := db.Query(fmt.Sprintf(`
		SELECT
			CONSTRAINT_NAME,
			COLUMN_NAME,
			REFERENCED_TABLE_NAME,
			REFERENCED_COLUMN_NAME
		FROM
			INFORMATION_SCHEMA.KEY_COLUMN_USAGE
		WHERE
			TABLE_SCHEMA = DATABASE() AND
			TABLE_NAME = '%s' AND
			REFERENCED_TABLE_NAME IS NOT NULL
	`, tableName))
	if err != nil {
		return tableSchema, err
	}
	defer foreignKeys.Close()

	for foreignKeys.Next() {
		var fk ForeignKeySchema
		if err := foreignKeys.Scan(&fk.Name, &fk.ColumnName, &fk.ReferencedTable, &fk.ReferencedColumn); err != nil {
			return tableSchema, err
		}
		tableSchema.ForeignKeys = append(tableSchema.ForeignKeys, fk)
	}

	return tableSchema, nil
}

func (a *MySQLAdapter) CompareTableDataByChecksum(sourceDB, targetDB *sql.DB, tableName string, schema TableSchema) (bool, error) {
	// fmt.Printf("Comparing data for table '%s' by checksum...\n", tableName)

	// Use MySQL's built-in checksum table function
	var sourceChecksum, targetChecksum sql.NullInt64

	var tableNameCol string
	err := sourceDB.QueryRow(fmt.Sprintf("CHECKSUM TABLE `%s`", tableName)).Scan(&tableNameCol, &sourceChecksum)
	if err != nil {
		fmt.Printf("Error getting source checksum: %v\n", err)
		return false, err
	}

	var targetTableNameCol string
	err = targetDB.QueryRow(fmt.Sprintf("CHECKSUM TABLE `%s`", tableName)).Scan(&targetTableNameCol, &targetChecksum)
	if err != nil {
		fmt.Printf("Error getting target checksum: %v\n", err)
		return false, err
	}

	if !sourceChecksum.Valid && !targetChecksum.Valid {
		fmt.Printf("Checksums not available for table '%s'\n", tableName)
		return true, nil
	}

	if sourceChecksum.Valid != targetChecksum.Valid {
		fmt.Printf("Table '%s' has different data (checksum validity differs)\n", tableName)
		return true, nil
	}

	if sourceChecksum.Int64 != targetChecksum.Int64 {
		//fmt.Printf("Table '%s' has different data (checksums: source=%d, target=%d)\n",
		//	tableName, sourceChecksum.Int64, targetChecksum.Int64)
		return true, nil // Return true to indicate differences
	} else {
		// fmt.Printf("Table '%s' has identical data according to checksum\n", tableName)
		return false, nil // Return false when no differences
	}
}

func (a *MySQLAdapter) CompareRowCounts(sourceDB, targetDB *sql.DB, tableName string) (int, int, error) {
	var sourceCount, targetCount int

	sourceRow := sourceDB.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM `%s`", tableName))
	if err := sourceRow.Scan(&sourceCount); err != nil {
		return 0, 0, err
	}

	targetRow := targetDB.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM `%s`", tableName))
	if err := targetRow.Scan(&targetCount); err != nil {
		return 0, 0, err
	}

	return sourceCount, targetCount, nil
}
