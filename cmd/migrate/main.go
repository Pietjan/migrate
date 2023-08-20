package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"strings"

	"github.com/pietjan/migrate"
	"github.com/pietjan/migrate/database/postgres"
	"github.com/pietjan/migrate/database/sqlite"
	"github.com/pietjan/migrate/database/sqlserver"
	"github.com/pietjan/migrate/source/file"

	_ "github.com/lib/pq"
	_ "github.com/microsoft/go-mssqldb"
	_ "modernc.org/sqlite"
)

type config struct {
	source   string
	database string
	schema   string
}

func main() {
	var config config
	if err := parseConfig(&config); err != nil {
		log.Fatal(err)
	}

	if err := isValid(config); err != nil {
		log.Fatal(err)
	}

	source, err := getSource(config)
	if err != nil {
		log.Fatal(err)
	}

	database, err := getDatabase(config)
	if err != nil {
		log.Fatal(err)
	}

	if err := migrate.New(source, database).Run(); err != nil {
		log.Fatal(err)
	}
}

func parseConfig(c *config) error {
	flag.StringVar(&c.source, `source`, ``, `file://`)
	flag.StringVar(&c.database, `database`, ``, `postgres://`)
	flag.StringVar(&c.schema, `schema`, ``, `database schema (optional)`)

	flag.Parse()

	return nil
}

func getSource(c config) (migrate.Source, error) {
	switch driver(c.source) {
	case `file`:
		return file.New(file.Dir(dsn(c.source))), nil
	default:
		return nil, fmt.Errorf(`unknown source %s`, c.source)
	}
}

func getDatabase(c config) (migrate.Database, error) {
	switch driver(c.database) {
	case `sqlite`:
		return sqliteDatabase(c)
	case `postgres`:
		return postgresDatabase(c)
	case `sqlserver`:
		return sqlserverDatabase(c)
	default:
		return nil, fmt.Errorf(`unknown database %s`, c.database)
	}
}

func sqliteDatabase(c config) (migrate.Database, error) {
	db, err := sql.Open(driver(c.database), dsn(c.database))
	if err != nil {
		return nil, err
	}
	return sqlite.New(sqlite.DB(db)), nil
}

func postgresDatabase(c config) (migrate.Database, error) {
	db, err := sql.Open(driver(c.database), c.database)
	if err != nil {
		return nil, err
	}

	var options []postgres.Option
	options = append(options, postgres.DB(db))

	if len(c.schema) > 0 {
		options = append(options, postgres.Schema(c.schema))
	}

	return postgres.New(options...), nil
}

func sqlserverDatabase(c config) (migrate.Database, error) {
	db, err := sql.Open(driver(c.database), c.database)
	if err != nil {
		return nil, err
	}

	var options []sqlserver.Option
	options = append(options, sqlserver.DB(db))

	if len(c.schema) > 0 {
		options = append(options, sqlserver.Schema(c.schema))
	}

	return sqlserver.New(options...), nil
}

func isValid(c config) error {
	if len(c.source) == 0 {
		return fmt.Errorf(`source not specified`)
	}

	if len(c.database) == 0 {
		return fmt.Errorf(`database not specified`)
	}

	return nil
}

func driver(s string) string {
	parts := strings.Split(s, `://`)
	if len(parts) > 1 {
		return parts[0]
	}

	return ``
}

func dsn(s string) string {
	parts := strings.Split(s, `://`)
	if len(parts) > 1 {
		return parts[1]
	}

	return ``
}
