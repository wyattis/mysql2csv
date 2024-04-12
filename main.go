package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"

	_ "embed"

	_ "github.com/go-sql-driver/mysql"

	"github.com/urfave/cli/v2"
)

//go:embed VERSION
var version string

var app = cli.App{
	Name:                 "mysql2csv",
	Usage:                "Execute a query against a MySQL database and output the results as CSV",
	Description:          "Execute a query against a MySQL database and output the results as CSV",
	Version:              version,
	EnableBashCompletion: true,
	Args:                 true,
	ArgsUsage:            "<database>",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:    "execute",
			Aliases: []string{"e"},
			Usage:   "The query to execute. If not provided, the query will be read from stdin",
		},
		&cli.StringFlag{
			Name:    "user",
			Aliases: []string{"u"},
			Usage:   "MySQL username",
			EnvVars: []string{"MYSQL_USER", "MYSQL_USERNAME"},
			Value:   "root",
		},
		&cli.StringFlag{
			Name:    "password",
			Aliases: []string{"p"},
			EnvVars: []string{"MYSQL_PASSWORD"},
			Usage:   "MySQL password",
		},
		&cli.StringFlag{
			Name:    "host",
			Aliases: []string{"h"},
			EnvVars: []string{"MYSQL_HOST"},
			Usage:   "MySQL host",
			Value:   "127.0.0.1",
		},
		&cli.IntFlag{
			Name:    "port",
			Aliases: []string{"P"},
			EnvVars: []string{"MYSQL_PORT"},
			Usage:   "MySQL port",
			Value:   3306,
		},
		// &cli.BoolFlag{
		// 	Name:  "ip",
		// 	Usage: "Read the password interactively from the terminal",
		// },
		&cli.BoolFlag{
			Name:  "no-header",
			Usage: "Do not output the column names as the first row",
		},
		&cli.StringFlag{
			Name:    "output",
			Aliases: []string{"o"},
			Usage: formatUsageString(`The file to write the output to. If not provided, the output will be written to stdout. 
			Add %d to create multiple files with a number in the filename. 
			%0Nd will prefix the number with zeros to create a string of length N. For example, -o output-%03d.csv will create files output-001.csv, output-002.csv, etc.`),
		},
	},
	Action: func(c *cli.Context) (err error) {
		query := c.String("execute")

		// Try reading the query from stdin if it wasn't provided as an argument
		if strings.TrimSpace(query) == "" {
			stat, err := os.Stdin.Stat()
			if err != nil {
				return err
			}
			if stat.Mode()&os.ModeCharDevice != 0 {
				return fmt.Errorf("A query must be provided")
			}

			queryBytes, err := io.ReadAll(os.Stdin)
			if err != nil {
				return err
			}
			query = string(queryBytes)
		}

		if strings.TrimSpace(query) == "" {
			return fmt.Errorf("A query must be provided")
		}

		password := c.String("password")
		if password == "" && c.Bool("ip") {
			// TODO: figure out how to prompt for password while also getting a piped query from stdin
		}

		database := c.Args().First()
		if database == "" {
			database = os.Getenv("MYSQL_DATABASE")
		}

		dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?multiStatements=true", c.String("user"), password, c.String("host"), c.Int("port"), database)
		if password == "" {
			dsn = fmt.Sprintf("%s@tcp(%s:%d)/%s?multiStatements=true", c.String("user"), c.String("host"), c.Int("port"), database)
		}

		passwordLessDsn := strings.ReplaceAll(dsn, password, "******")
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			return fmt.Errorf("Error connecting to database (%s): %w", passwordLessDsn, err)
		}
		defer db.Close()
		rows, err := db.Query(query)
		if err != nil {
			return fmt.Errorf("Error executing query (%s) on (%s): %w", query, passwordLessDsn, err)
		}
		defer rows.Close()

		hasResultSet := true
		outputData := OutputData{
			OutputTemplate: c.String("output"),
		}
		var prevCols []string
		for hasResultSet {
			cols, err := rows.Columns()
			if err != nil {
				return err
			}
			if len(cols) != len(prevCols) && len(prevCols) > 0 && !outputCreatesMultipleFiles(outputData.OutputTemplate) {
				return fmt.Errorf("The number of columns in each result set must be the same when writing to stdout or a valid output template must be provided")
			}
			prevCols = cols
			output, err := getOutput(outputData)
			if err != nil {
				return fmt.Errorf("Error getting output: %w", err)
			}
			if err = writeResultSet(rows, output, c.Bool("no-header")); err != nil {
				return fmt.Errorf("Error writing result set: %w", err)
			}
			hasResultSet = rows.NextResultSet()
			outputData.FileNum++
		}
		return
	},
}

type OutputData struct {
	OutputTemplate string
	FileNum        int
}

func getOutput(data OutputData) (output io.WriteCloser, err error) {
	output = NopCloser{os.Stdout}
	if data.OutputTemplate != "" {
		filename := data.OutputTemplate
		if outputCreatesMultipleFiles(filename) {
			filename = fmt.Sprintf(filename, data.FileNum)
		}
		if filename != "" {
			output, err = os.Create(filename)
			if err != nil {
				return nil, err
			}
		}
	}
	return
}

func formatUsageString(s string) string {
	res := strings.ReplaceAll(s, "\n", " ")
	res = iterativeReplaceAll(res, []string{"  ", "\t"}, " ")
	return res
}

func iterativeReplaceAll(s string, from []string, to string) string {
	hasChanged := true
	o := s
	for hasChanged {
		o = s
		for _, f := range from {
			s = strings.ReplaceAll(s, f, to)
		}
		hasChanged = o != s
	}
	return s
}

var hasPercentD = regexp.MustCompile("%(0\\d)?d")

func outputCreatesMultipleFiles(outputTemplate string) bool {
	return hasPercentD.MatchString(outputTemplate)
}

func writeResultSet(rows *sql.Rows, output io.WriteCloser, noHeader bool) (err error) {
	defer output.Close()
	writer := csv.NewWriter(output)
	defer writer.Flush()
	columns, err := rows.Columns()
	if err != nil {
		return
	}
	if !noHeader {
		if err = writer.Write(columns); err != nil {
			return
		}
	}
	values := make([]interface{}, len(columns))
	stringVals := make([]string, len(columns))
	for i := range values {
		values[i] = &sql.RawBytes{}
	}

	for rows.Next() {
		if err = rows.Err(); err != nil {
			return
		}
		if err = rows.Scan(values...); err != nil {
			return
		}
		for i, val := range values {
			v := val.(*sql.RawBytes)
			stringVals[i] = string(*v)
		}
		if err = writer.Write(stringVals); err != nil {
			return
		}
	}
	return
}

type NopCloser struct {
	io.Writer
}

func (NopCloser) Close() error {
	return nil
}

func main() {
	cli.HelpFlag = &cli.BoolFlag{
		Name:  "help",
		Usage: "Show help",
	}
	if err := app.Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
