# mysql2csv
A simple tool to transform queries into a CSV written in Go. Modeled after the `mysql` and `mysqdump` command line utilities, this project is essentially just a wrapper around the standard [Go MySQL Driver](https://github.com/go-sql-driver/mysql) and Go's internal [encoding/csv](https://pkg.go.dev/encoding/csv) module.

## Install
`go install github.com/wyattis/mysql2csv@latest` or download the binary from the releases.

## Usage
Get full usage with `mysql2csv help`

### Execute a single query
`mysql2csv -e "select * from user" testdb > users.csv`

### Execute multiple queries from a file and write to separate files
`mysql2csv -o output.%d.csv testdb < queries.sql`

