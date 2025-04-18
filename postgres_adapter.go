package main

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

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
