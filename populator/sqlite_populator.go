package main

import (
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"time"

	_ "modernc.org/sqlite"
)

const (
	dbPath     = "test_data.db"
	targetSize = 2 * 1024 * 1024 * 1024 // 2GiB in bytes
	batchSize  = 1000                   // Insert records in batches
)

// Define possible column types
type ColumnType int

const (
	TypeInteger ColumnType = iota
	TypeReal
	TypeText
	TypeBlob
	TypeDateTime
)

// Define column structure
type Column struct {
	Name     string
	Type     ColumnType
	TextSize int // For TEXT columns: 0=small, 1=medium, 2=large
}

// Define table structure
type Table struct {
	Name    string
	Columns []Column
}

func main() {
	// Remove existing database if it exists
	os.Remove(dbPath)

	// Open database connection
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Set pragmas for better performance
	_, err = db.Exec("PRAGMA synchronous = OFF")
	if err != nil {
		panic(err)
	}
	_, err = db.Exec("PRAGMA journal_mode = MEMORY")
	if err != nil {
		panic(err)
	}
	_, err = db.Exec("PRAGMA cache_size = 1000000") // Use a larger cache
	if err != nil {
		panic(err)
	}

	// Initialize random number generator
	rand.Seed(time.Now().UnixNano())

	// Generate random tables
	tableCount := 3 + rand.Intn(8) // Generate 3-10 tables
	tables := make([]Table, tableCount)

	fmt.Printf("Creating %d random tables...\n", tableCount)

	for i := 0; i < tableCount; i++ {
		tables[i] = generateRandomTableSchema(i + 1)

		// Create the table
		tableSQL := createTableSQL(tables[i])
		fmt.Printf("Creating table %s with %d columns\n", tables[i].Name, len(tables[i].Columns))
		_, err := db.Exec(tableSQL)
		if err != nil {
			panic(err)
		}

		// Create an index on one of the non-primary key numeric columns if available
		createRandomIndex(db, tables[i])
	}

	fmt.Println("Generating data until database reaches approximately 2GiB...")

	// Track progress
	rowCount := 0
	startTime := time.Now()
	lastReportTime := startTime

	for {
		// Choose a random table to insert into
		tableIndex := rand.Intn(len(tables))
		table := tables[tableIndex]

		// Begin transaction for batch insert
		tx, err := db.Begin()
		if err != nil {
			panic(err)
		}

		insertSQL := generateInsertStatement(table)
		stmt, err := tx.Prepare(insertSQL)
		if err != nil {
			panic(err)
		}

		// Insert batch of records
		for i := 0; i < batchSize; i++ {
			// Generate values for each column (skip ID which is auto-increment)
			values := make([]interface{}, 0, len(table.Columns)-1)
			for _, col := range table.Columns[1:] { // Skip first column (ID)
				values = append(values, generateRandomValue(col))
			}

			_, err := stmt.Exec(values...)
			if err != nil {
				tx.Rollback()
				panic(err)
			}
		}

		stmt.Close()
		err = tx.Commit()
		if err != nil {
			panic(err)
		}

		rowCount += batchSize

		// Check database size periodically
		if time.Since(lastReportTime).Seconds() > 5 {
			dbSize := getDatabaseSize(dbPath)
			progress := float64(dbSize) / float64(targetSize) * 100
			speed := float64(rowCount) / time.Since(startTime).Seconds()

			fmt.Printf("Inserted %d rows. Database size: %.2f MiB (%.2f%% of target). Speed: %.0f rows/sec\n",
				rowCount, float64(dbSize)/(1024*1024), progress, speed)

			if dbSize >= targetSize {
				fmt.Println("Target size reached. Stopping.")
				break
			}

			lastReportTime = time.Now()
		}
	}

	// Final database info
	dbSize := getDatabaseSize(dbPath)
	fmt.Printf("Final database size: %.2f GiB with %d rows across %d tables\n",
		float64(dbSize)/(1024*1024*1024), rowCount, tableCount)
	fmt.Printf("Elapsed time: %s\n", time.Since(startTime))
}

// Generate a random table schema
func generateRandomTableSchema(tableIndex int) Table {
	columnCount := 5 + rand.Intn(16) // Random number between 5 and 20 columns
	table := Table{
		Name:    fmt.Sprintf("random_table_%d", tableIndex),
		Columns: make([]Column, 0, columnCount),
	}

	// Add primary key column (always INTEGER)
	table.Columns = append(table.Columns, Column{
		Name: "id",
		Type: TypeInteger,
	})

	// Add random columns
	for i := 0; i < columnCount-1; i++ {
		columnType := ColumnType(rand.Intn(5)) // Random column type
		textSize := 0
		if columnType == TypeText {
			textSize = rand.Intn(3) // 0=small, 1=medium, 2=large
		}

		column := Column{
			Name:     fmt.Sprintf("col_%d", i+1),
			Type:     columnType,
			TextSize: textSize,
		}

		table.Columns = append(table.Columns, column)
	}

	return table
}

// Create SQL for table creation
func createTableSQL(table Table) string {
	sql := fmt.Sprintf("CREATE TABLE %s (\n", table.Name)

	for i, col := range table.Columns {
		if i > 0 {
			sql += ",\n"
		}

		// Add column name
		sql += fmt.Sprintf("    %s ", col.Name)

		// Add column type
		switch col.Type {
		case TypeInteger:
			sql += "INTEGER"
			if col.Name == "id" {
				sql += " PRIMARY KEY"
			}
		case TypeReal:
			sql += "REAL"
		case TypeText:
			sql += "TEXT"
		case TypeBlob:
			sql += "BLOB"
		case TypeDateTime:
			sql += "DATETIME"
		}
	}

	sql += "\n)"
	return sql
}

// Create a random index on a numeric column
func createRandomIndex(db *sql.DB, table Table) {
	var eligibleColumns []Column

	// Find eligible numeric columns
	for _, col := range table.Columns {
		if col.Name != "id" && (col.Type == TypeInteger || col.Type == TypeReal) {
			eligibleColumns = append(eligibleColumns, col)
		}
	}

	if len(eligibleColumns) > 0 {
		// Choose a random column to index
		col := eligibleColumns[rand.Intn(len(eligibleColumns))]
		indexSQL := fmt.Sprintf("CREATE INDEX idx_%s_%s ON %s(%s)",
			table.Name, col.Name, table.Name, col.Name)

		_, err := db.Exec(indexSQL)
		if err != nil {
			fmt.Printf("Warning: Failed to create index on %s.%s: %v\n", table.Name, col.Name, err)
		} else {
			fmt.Printf("Created index on %s.%s\n", table.Name, col.Name)
		}
	}
}

// Generate insert statement for a specific table
func generateInsertStatement(table Table) string {
	sql := fmt.Sprintf("INSERT INTO %s (", table.Name)

	// Skip the ID column as it's auto-increment
	for i, col := range table.Columns {
		if i > 0 {
			if i > 1 {
				sql += ", "
			}
			sql += col.Name
		}
	}

	sql += ") VALUES ("

	// Add placeholders
	for i := 1; i < len(table.Columns); i++ {
		if i > 1 {
			sql += ", "
		}
		sql += "?"
	}

	sql += ")"
	return sql
}

// Generate random value based on column type
func generateRandomValue(col Column) interface{} {
	switch col.Type {
	case TypeInteger:
		return rand.Int63()
	case TypeReal:
		return rand.Float64()
	case TypeText:
		switch col.TextSize {
		case 0: // Small
			return randomString(10 + rand.Intn(20))
		case 1: // Medium
			return randomString(100 + rand.Intn(200))
		default: // Large
			return randomString(1000 + rand.Intn(4000))
		}
	case TypeBlob:
		return randomBytes(500 + rand.Intn(1500))
	case TypeDateTime:
		return time.Now().Add(-time.Duration(rand.Intn(86400*365)) * time.Second)
	default:
		return nil
	}
}

// Generate random string of specified length
func randomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*()-_=+[]{}|;:,.<>?/"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

// Generate random bytes of specified length
func randomBytes(length int) []byte {
	bytes := make([]byte, length)
	rand.Read(bytes)
	return bytes
}

// Get the current size of the database file
func getDatabaseSize(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.Size()
}
