package main

import (
	"database/sql"
	"fmt"
)

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
