package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"reflect"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
)

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

// DatabaseAdapter defines the interface for database-specific operations
type DatabaseAdapter interface {
	Connect(connectionString string) (*sql.DB, error)
	GetTableList(db *sql.DB) ([]string, error)
	GetTableSchema(db *sql.DB, tableName string) (TableSchema, error)
	CompareTableDataByChecksum(sourceDB, targetDB *sql.DB, tableName string, schema TableSchema) (bool, error)
	CompareRowCounts(sourceDB, targetDB *sql.DB, tableName string) (int, int, error)
	GetConnectStringFromURL(url string) string
}

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

// PostgreSQLAdapter implements DatabaseAdapter for PostgreSQL
type PostgreSQLAdapter struct{}

func (a *PostgreSQLAdapter) Connect(connectionString string) (*sql.DB, error) {
	return sql.Open("postgres", connectionString)
}

func (a *PostgreSQLAdapter) GetConnectStringFromURL(url string) string {
	// For Postgres, the URL format should already be compatible
	return url
}

func (a *PostgreSQLAdapter) GetTableList(db *sql.DB) ([]string, error) {
	rows, err := db.Query(`
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema='public' AND table_type='BASE TABLE'
	`)
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

func (a *PostgreSQLAdapter) GetTableSchema(db *sql.DB, tableName string) (TableSchema, error) {
	tableSchema := TableSchema{Name: tableName}

	// Get columns
	columns, err := db.Query(`
		SELECT
			column_name,
			data_type,
			is_nullable,
			column_default,
			''::text as extra
		FROM
			information_schema.columns
		WHERE
			table_schema = 'public' AND
			table_name = $1
		ORDER BY
			ordinal_position
	`, tableName)
	if err != nil {
		return tableSchema, err
	}
	defer columns.Close()

	for columns.Next() {
		var col ColumnSchema
		var nullable string
		var defaultValue sql.NullString
		var extra string

		if err := columns.Scan(&col.Name, &col.DataType, &nullable, &defaultValue, &extra); err != nil {
			return tableSchema, err
		}

		col.Nullable = nullable
		col.Default = defaultValue
		col.Extra = extra

		tableSchema.Columns = append(tableSchema.Columns, col)
	}

	// Get primary keys
	primaryKeys, err := db.Query(`
		SELECT a.attname
		FROM   pg_index i
		JOIN   pg_attribute a ON a.attrelid = i.indrelid
								AND a.attnum = ANY(i.indkey)
		WHERE  i.indrelid = $1::regclass
		AND    i.indisprimary
	`, tableName)
	if err != nil {
		return tableSchema, err
	}
	defer primaryKeys.Close()

	for primaryKeys.Next() {
		var pkColumn string
		if err := primaryKeys.Scan(&pkColumn); err != nil {
			return tableSchema, err
		}
		tableSchema.PrimaryKeys = append(tableSchema.PrimaryKeys, pkColumn)

		// Update the key field in the column schema
		for i, col := range tableSchema.Columns {
			if col.Name == pkColumn {
				tableSchema.Columns[i].Key = "PRI"
			}
		}
	}

	// Get indexes
	indexes, err := db.Query(`
		SELECT
			i.relname as index_name,
			a.attname as column_name,
			ix.indisunique as is_unique
		FROM
			pg_class t,
			pg_class i,
			pg_index ix,
			pg_attribute a
		WHERE
			t.oid = ix.indrelid
			AND i.oid = ix.indexrelid
			AND a.attrelid = t.oid
			AND a.attnum = ANY(ix.indkey)
			AND t.relkind = 'r'
			AND t.relname = $1
	`, tableName)
	if err != nil {
		return tableSchema, err
	}
	defer indexes.Close()

	for indexes.Next() {
		var indexSchema IndexSchema
		var isUnique bool

		if err := indexes.Scan(&indexSchema.Name, &indexSchema.ColumnName, &isUnique); err != nil {
			return tableSchema, err
		}

		indexSchema.NonUnique = 0
		if !isUnique {
			indexSchema.NonUnique = 1
		}

		tableSchema.Indexes = append(tableSchema.Indexes, indexSchema)
	}

	// Get foreign keys
	foreignKeys, err := db.Query(`
		SELECT
			tc.constraint_name,
			kcu.column_name,
			ccu.table_name AS referenced_table,
			ccu.column_name AS referenced_column
		FROM
			information_schema.table_constraints tc
			JOIN information_schema.key_column_usage kcu
				ON tc.constraint_name = kcu.constraint_name
				AND tc.table_schema = kcu.table_schema
			JOIN information_schema.constraint_column_usage ccu
				ON ccu.constraint_name = tc.constraint_name
				AND ccu.table_schema = tc.table_schema
		WHERE
			tc.constraint_type = 'FOREIGN KEY' AND
			tc.table_name = $1
	`, tableName)
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

func (a *PostgreSQLAdapter) CompareTableDataByChecksum(sourceDB, targetDB *sql.DB, tableName string, schema TableSchema) (bool, error) {
	fmt.Printf("Comparing data for table '%s' by hash...\n", tableName)

	// PostgreSQL doesn't have CHECKSUM TABLE, so use MD5 on all rows
	query := fmt.Sprintf("SELECT MD5(CAST((array_agg(t.* ORDER BY %s)) AS text)) FROM %s t",
		getOrderByClause(schema), tableName)

	var sourceHash, targetHash sql.NullString

	err := sourceDB.QueryRow(query).Scan(&sourceHash)
	if err != nil {
		fmt.Printf("Error getting source hash: %v\n", err)
		return false, err
	}

	err = targetDB.QueryRow(query).Scan(&targetHash)
	if err != nil {
		fmt.Printf("Error getting target hash: %v\n", err)
		return false, err
	}

	if !sourceHash.Valid && !targetHash.Valid {
		fmt.Printf("Hash not available for table '%s'\n", tableName)
		return false, nil
	}

	if sourceHash.Valid != targetHash.Valid {
		fmt.Printf("Table '%s' has different data (hash validity differs)\n", tableName)
		return true, nil
	}

	if sourceHash.String != targetHash.String {
		fmt.Printf("Table '%s' has different data (hashes differ)\n", tableName)
		return true, nil
	} else {
		fmt.Printf("Table '%s' has identical data according to hash\n", tableName)
		return false, nil
	}
}

func (a *PostgreSQLAdapter) CompareRowCounts(sourceDB, targetDB *sql.DB, tableName string) (int, int, error) {
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

func contains(slice []string, item string) bool {
	for _, a := range slice {
		if a == item {
			return true
		}
	}
	return false
}

func main() {
	if len(os.Args) < 4 {
		fmt.Println("Usage: mudrockdbcompare [db-type] [source-connection-string] [target-connection-string]")
		fmt.Println("supported database types: mysql")
		fmt.Println("Examples:")
		fmt.Println("  mudrockdbcompare mysql \"user:password@localhost:3306/dbname1\" \"user:password@localhost:3306/dbname2\"")
		fmt.Println("  mudrockdbcompare postgres \"postgres://user:password@localhost/dbname1\" \"postgres://user:password@localhost/dbname2\"")
		fmt.Println("  mudrockdbcompare sqlite \"file:db1.sqlite\" \"file:db2.sqlite\"")
		return
	}

	// Get database type and connection strings
	dbType := os.Args[1]
	sourceConfig := os.Args[2]
	targetConfig := os.Args[3]

	// Get the appropriate adapter
	adapter, err := GetAdapter(dbType)
	if err != nil {
		log.Fatalf("Error: %v", err)
	}

	// Process connection strings if needed
	sourceConnStr := adapter.GetConnectStringFromURL(sourceConfig)
	targetConnStr := adapter.GetConnectStringFromURL(targetConfig)

	// Connect to databases
	sourceDB, err := adapter.Connect(sourceConnStr)
	if err != nil {
		log.Fatalf("Failed to connect to source database: %v", err)
	}
	defer sourceDB.Close()

	targetDB, err := adapter.Connect(targetConnStr)
	if err != nil {
		log.Fatalf("Failed to connect to target database: %v", err)
	}
	defer targetDB.Close()

	// Get schema information from both databases
	fmt.Println("\nGetting table lists...")
	sourceTables, err := adapter.GetTableList(sourceDB)
	if err != nil {
		log.Fatalf("Failed to get source tables: %v", err)
	}

	targetTables, err := adapter.GetTableList(targetDB)
	if err != nil {
		log.Fatalf("Failed to get target tables: %v", err)
	}

	// Get detailed schemas
	fmt.Println("Getting table schemas...")
	sourceSchemas, err := getAllTableSchemas(adapter, sourceDB, sourceTables)
	if err != nil {
		log.Fatalf("Failed to get source schemas: %v", err)
	}

	targetSchemas, err := getAllTableSchemas(adapter, targetDB, targetTables)
	if err != nil {
		log.Fatalf("Failed to get target schemas: %v", err)
	}

	fmt.Println("Collecting database information...")
	sourceInfo, err := GetDatabaseInfo(adapter, sourceDB, sourceConnStr)
	if err != nil {
		fmt.Printf("Warning: couldn't collect full source database info: %v\n", err)
	}

	targetInfo, err := GetDatabaseInfo(adapter, targetDB, targetConnStr)
	if err != nil {
		fmt.Printf("Warning: couldn't collect full target database info: %v\n", err)
	}

	// Display database information
	fmt.Println("\n=== Database Information ===")
	fmt.Printf("Source: %s, Database: %s, Tables: %d, Size: %s\n",
		sourceInfo.Host, sourceInfo.DatabaseName, sourceInfo.TableCount, formatSize(sourceInfo.TotalSize))
	fmt.Printf("Target: %s, Database: %s, Tables: %d, Size: %s\n",
		targetInfo.Host, targetInfo.DatabaseName, targetInfo.TableCount, formatSize(targetInfo.TotalSize))

	summary := ComparisonSummary{
		DifferentRowCounts: make(map[string]struct{ Source, Target int }),
	}

	missingTables, extraTables, commonTables, schemaDifferences := compareDatabases(sourceSchemas, targetSchemas)

	// Compare data in common tables
	fmt.Println("\n=== Data Differences ===")
	fmt.Printf("Comparing data for %d tables...\n", len(commonTables))

	// Track progress
	totalTables := len(commonTables)
	lastPercentReported := -1

	for tableName := range schemaDifferences {
		if !contains(summary.DifferentTables, tableName) {
			summary.DifferentTables = append(summary.DifferentTables, tableName)
		}
	}

	for i, tableName := range commonTables {
		// Calculate and report progress
		currentPercent := (i * 100) / totalTables
		if currentPercent > lastPercentReported {
			fmt.Printf("Progress: %d%%\n", currentPercent)
			lastPercentReported = currentPercent
		}

		//schema := sourceSchemas[tableName]

		// Compare row counts
		sourceCount, targetCount, err := adapter.CompareRowCounts(sourceDB, targetDB, tableName)
		if err != nil {
			fmt.Printf("Error comparing row counts for table %s: %v\n", tableName, err)
			continue
		}

		if sourceCount != targetCount {
			fmt.Printf("Table '%s' has different row counts: source=%d, target=%d\n",
				tableName, sourceCount, targetCount)
			summary.DifferentRowCounts[tableName] = struct{ Source, Target int }{sourceCount, targetCount}
			summary.DifferentTables = append(summary.DifferentTables, tableName)
		}
	}

	// Complete progress
	fmt.Println("Progress: 100%")

	// Compare and report schema differences
	if len(missingTables) > 0 {
		fmt.Printf("Tables in source but not in target: %v\n", missingTables)
	}

	if len(extraTables) > 0 {
		fmt.Printf("Tables in target but not in source: %v\n", extraTables)
	}

	// Print summary
	fmt.Println("\n=== Comparison Summary ===")
	if len(summary.DifferentTables) == 0 {
		fmt.Println("No differences found between the databases.")
	} else {
		fmt.Printf("Found differences in %d tables:\n", len(summary.DifferentTables))

		// First, report tables with row count differences
		for tableName, counts := range summary.DifferentRowCounts {
			fmt.Printf("- %s (row counts differ: source=%d, target=%d)\n",
				tableName, counts.Source, counts.Target)
		}

		// Then add missing tables
		for _, tableName := range missingTables {
			fmt.Printf("- %s (exists in source but not in target)\n", tableName)
		}

		// Then add extra tables
		for _, tableName := range extraTables {
			fmt.Printf("- %s (exists in target but not in source)\n", tableName)
		}

		// Then add tables with schema differences
		for tableName, diffs := range schemaDifferences {
			// Skip if we already reported it for row counts
			if _, reported := summary.DifferentRowCounts[tableName]; reported {
				continue
			}

			// Only print first difference to keep the summary concise
			if len(diffs) > 0 {
				fmt.Printf("- %s (%s)\n", tableName, diffs[0])
				if len(diffs) > 1 {
					fmt.Printf("  (and %d more differences)\n", len(diffs)-1)
				}
			}
		}
	}

	fmt.Println("\n=== Database Comparison Finished ===")
}

func getAllTableSchemas(adapter DatabaseAdapter, db *sql.DB, tables []string) (map[string]TableSchema, error) {
	schemas := make(map[string]TableSchema)

	for _, table := range tables {
		schema, err := adapter.GetTableSchema(db, table)
		if err != nil {
			return nil, err
		}
		schemas[table] = schema
	}

	return schemas, nil
}

func compareDatabases(sourceSchemas, targetSchemas map[string]TableSchema) ([]string, []string, []string, map[string][]string) {
	missingTables := []string{}
	extraTables := []string{}
	commonTables := []string{}
	schemaDifferences := make(map[string][]string)

	// Check for tables in source but not in target
	for tableName := range sourceSchemas {
		if _, exists := targetSchemas[tableName]; !exists {
			missingTables = append(missingTables, tableName)
			continue
		}

		// Table exists in both, compare schema
		hasDiffs, diffs := compareTableSchema(tableName, sourceSchemas[tableName], targetSchemas[tableName])
		if hasDiffs {
			schemaDifferences[tableName] = diffs
		}
		commonTables = append(commonTables, tableName)
	}

	// Check for tables in target but not in source
	for tableName := range targetSchemas {
		if _, exists := sourceSchemas[tableName]; !exists {
			extraTables = append(extraTables, tableName)
		}
	}

	return missingTables, extraTables, commonTables, schemaDifferences
}

func compareTableSchema(tableName string, sourceSchema, targetSchema TableSchema) (bool, []string) {
	hasDifferences := false
	differences := []string{}

	// Compare columns
	sourceColumns := make(map[string]ColumnSchema)
	for _, col := range sourceSchema.Columns {
		sourceColumns[col.Name] = col
	}

	targetColumns := make(map[string]ColumnSchema)
	for _, col := range targetSchema.Columns {
		targetColumns[col.Name] = col
	}

	// Check for columns in source but not in target
	for colName, sourceCol := range sourceColumns {
		if targetCol, exists := targetColumns[colName]; !exists {
			differences = append(differences, fmt.Sprintf("Column '%s.%s' exists in source but not in target", tableName, colName))
			hasDifferences = true
		} else {
			// Compare column properties
			if sourceCol.DataType != targetCol.DataType {
				differences = append(differences, fmt.Sprintf("Column '%s.%s' has different data type: source='%s', target='%s'",
					tableName, colName, sourceCol.DataType, targetCol.DataType))
				hasDifferences = true
			}
			if sourceCol.Nullable != targetCol.Nullable {
				differences = append(differences, fmt.Sprintf("Column '%s.%s' has different nullable property: source='%s', target='%s'",
					tableName, colName, sourceCol.Nullable, targetCol.Nullable))
				hasDifferences = true
			}
			// Compare other properties as needed
		}
	}

	// Check for columns in target but not in source
	for colName := range targetColumns {
		if _, exists := sourceColumns[colName]; !exists {
			differences = append(differences, fmt.Sprintf("Column '%s.%s' exists in target but not in source", tableName, colName))
			hasDifferences = true
		}
	}

	// Compare primary keys
	if !compareStringSlices(sourceSchema.PrimaryKeys, targetSchema.PrimaryKeys) {
		differences = append(differences, fmt.Sprintf("Table '%s' has different primary keys: source=%v, target=%v",
			tableName, sourceSchema.PrimaryKeys, targetSchema.PrimaryKeys))
		hasDifferences = true
	}

	return hasDifferences, differences
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

func compareIndexes(tableName string, sourceIndexes, targetIndexes []IndexSchema) bool {
	hasDifferences := false
	// Create maps of indexes by name and column for more efficient comparison
	sourceIndexMap := make(map[string]map[string]IndexSchema)
	targetIndexMap := make(map[string]map[string]IndexSchema)

	for _, idx := range sourceIndexes {
		if sourceIndexMap[idx.Name] == nil {
			sourceIndexMap[idx.Name] = make(map[string]IndexSchema)
		}
		sourceIndexMap[idx.Name][idx.ColumnName] = idx
	}

	for _, idx := range targetIndexes {
		if targetIndexMap[idx.Name] == nil {
			targetIndexMap[idx.Name] = make(map[string]IndexSchema)
		}
		targetIndexMap[idx.Name][idx.ColumnName] = idx
	}

	// Check for indexes in source but not in target
	for name, sourceIdx := range sourceIndexMap {
		if _, exists := targetIndexMap[name]; !exists {
			columns := []string{}
			for col := range sourceIdx {
				columns = append(columns, col)
			}
			fmt.Printf("Index '%s' on columns %v exists in source but not in target for table '%s'\n",
				name, columns, tableName)
			continue
		}

		// Index exists in both, compare columns
		for col, srcIdxCol := range sourceIdx {
			if _, exists := targetIndexMap[name][col]; !exists {
				fmt.Printf("Column '%s' of index '%s' exists in source but not in target for table '%s'\n",
					col, name, tableName)
			} else if srcIdxCol.NonUnique != targetIndexMap[name][col].NonUnique {
				fmt.Printf("Index '%s' on column '%s' has different uniqueness in table '%s': "+
					"source=%v, target=%v\n", name, col, tableName,
					srcIdxCol.NonUnique == 0, targetIndexMap[name][col].NonUnique == 0)
			}
		}

		for col := range targetIndexMap[name] {
			if _, exists := sourceIdx[col]; !exists {
				fmt.Printf("Column '%s' of index '%s' exists in target but not in source for table '%s'\n",
					col, name, tableName)
			}
		}
	}

	// Check for indexes in target but not in source
	for name, targetIdx := range targetIndexMap {
		if _, exists := sourceIndexMap[name]; !exists {
			columns := []string{}
			for col := range targetIdx {
				columns = append(columns, col)
			}
			fmt.Printf("Index '%s' on columns %v exists in target but not in source for table '%s'\n",
				name, columns, tableName)
		}
	}

	return hasDifferences
}

func compareForeignKeys(tableName string, sourceFKs, targetFKs []ForeignKeySchema) bool {
	hasDifferences := false
	sourceFKMap := make(map[string]ForeignKeySchema)
	targetFKMap := make(map[string]ForeignKeySchema)

	// For simpler comparison, create maps with a composite key
	for _, fk := range sourceFKs {
		key := fmt.Sprintf("%s_%s_%s_%s", fk.Name, fk.ColumnName, fk.ReferencedTable, fk.ReferencedColumn)
		sourceFKMap[key] = fk
	}

	for _, fk := range targetFKs {
		key := fmt.Sprintf("%s_%s_%s_%s", fk.Name, fk.ColumnName, fk.ReferencedTable, fk.ReferencedColumn)
		targetFKMap[key] = fk
	}

	// Check for foreign keys in source but not in target
	for key, fk := range sourceFKMap {
		if _, exists := targetFKMap[key]; !exists {
			fmt.Printf("Foreign key '%s' from '%s.%s' to '%s.%s' exists in source but not in target\n",
				fk.Name, tableName, fk.ColumnName, fk.ReferencedTable, fk.ReferencedColumn)
		}
	}

	// Check for foreign keys in target but not in source
	for key, fk := range targetFKMap {
		if _, exists := sourceFKMap[key]; !exists {
			fmt.Printf("Foreign key '%s' from '%s.%s' to '%s.%s' exists in target but not in source\n",
				fk.Name, tableName, fk.ColumnName, fk.ReferencedTable, fk.ReferencedColumn)
		}
	}

	return hasDifferences
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
