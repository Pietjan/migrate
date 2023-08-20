package postgres

import (
	"database/sql"
	"io"
	"strconv"

	"github.com/pietjan/migrate/database"
)

type Option = func(*driver)

func New(options ...Option) database.Driver {
	d := &driver{
		table: `version`,
	}

	for _, fn := range options {
		fn(d)
	}

	if d.db == nil {
		panic(`nil sql.DB`)
	}

	return d
}

func DB(db *sql.DB) Option {
	return func(d *driver) {
		d.db = db
	}
}

func Schema(schema string) Option {
	return func(d *driver) {
		d.schema = schema
	}
}

type driver struct {
	db     *sql.DB
	schema string
	table  string
}

func (d *driver) Run(r io.Reader, version int) error {
	migration, err := io.ReadAll(r)
	if err != nil {
		return err
	}

	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if len(d.schema) > 0 && version == 1 {
		if err := ensureSchemaExists(tx, d.schema); err != nil {
			return err
		}
	}

	if len(d.schema) > 0 {
		if err := setSchema(tx, d.schema); err != nil {
			return err
		}
	} else {
		schema, err := getSchema(tx)
		if err != nil {
			return err
		}
		d.schema = schema
	}

	if version == 1 {
		if err := ensureTableExists(tx, d.schema, d.table); err != nil {
			return err
		}
	}

	if _, err := tx.Exec(string(migration)); err != nil {
		return err
	}

	if err := setVersion(tx, d.schema, d.table, version); err != nil {
		return err
	}

	return tx.Commit()
}

func (d driver) Version() (int, error) {
	var exists bool
	var version int
	err := d.db.QueryRow(`SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2)`, d.schema, d.table).Scan(&exists)
	if err != nil {
		return version, err
	}

	if !exists {
		return version, nil
	}

	err = d.db.QueryRow(`SELECT version FROM ` + d.schema + `.` + d.table + ` ORDER BY version DESC`).Scan(&version)
	if err != sql.ErrNoRows && err != nil {
		return version, err
	}

	return version, nil
}

func ensureSchemaExists(tx *sql.Tx, schema string) error {
	_, err := tx.Exec(`CREATE SCHEMA IF NOT EXISTS ` + schema)
	return err
}

func ensureTableExists(tx *sql.Tx, schema, table string) error {
	_, err := tx.Exec(`CREATE TABLE IF NOT EXISTS ` + schema + `.` + table + ` (version integer NOT NULL PRIMARY KEY)`)
	return err
}

func setVersion(tx *sql.Tx, schema, table string, version int) (err error) {
	_, err = tx.Exec(`INSERT INTO ` + schema + `.` + table + ` (version) VALUES (` + strconv.Itoa(version) + `)`)
	return err
}

func setSchema(tx *sql.Tx, schema string) error {
	_, err := tx.Exec(`SET search_path TO ` + schema)
	return err
}

func getSchema(tx *sql.Tx) (schema string, err error) {
	err = tx.QueryRow(`SHOW search_path`).Scan(&schema)
	return
}
