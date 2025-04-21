package main

import (
	"fmt"
	"log"
	"os"
)

func main() {
	if len(os.Args) < 4 {
		fmt.Println("Usage: mudrockdbcompare [db-type] [source-connection-string] [target-connection-string]")
		fmt.Println("supported database types: mysql, sqlite")
		fmt.Println("Examples:")
		fmt.Println("  mudrockdbcompare mysql \"user:password@localhost:3306/dbname1\" \"user:password@localhost:3306/dbname2\"")
		fmt.Println("  mudrockdbcompare postgres \"postgres://user:password@localhost/dbname1\" \"postgres://user:password@localhost/dbname2\"")
		fmt.Println("  mudrockdbcompare sqlite \"path/to/db1.db\" \"path/to/db2.db\"")
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

	// Print summary
	fmt.Println("\n=== Comparison Summary ===")
	if len(summary.DifferentTables) == 0 && len(extraTables) == 0 && len(summary.DifferentRowCounts) == 0 && len(missingTables) == 0 {
		fmt.Println("No differences found between the databases.")
	} else {
		fmt.Printf("Found differences in %d tables:\n", len(summary.DifferentTables)+len(extraTables)+len(missingTables))

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
