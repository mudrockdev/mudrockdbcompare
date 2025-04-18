# mudrockdbcompare

a compare tool for small databases

## Usage

```console
./mudrockdbcompare mysql user:password@localhost:3306/dbname1 user:password@localhost:3306/dbname2
./mudrockdbcompare sqlite path/to/db1.db path/to/db2.db
```

currently supported databases: mysql, sqlite

planned to be supported: postgres
