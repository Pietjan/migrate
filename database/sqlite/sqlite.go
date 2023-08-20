package sqlite

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

type driver struct {
	db    *sql.DB
	table string
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

	if version == 1 {
		ensureTableExists(tx, d.table)
	}

	if _, err := tx.Exec(string(migration)); err != nil {
		return err
	}

	if err := setVersion(tx, d.table, version); err != nil {
		return err
	}

	return tx.Commit()
}

func (d driver) Version() (int, error) {
	var version int
	var exists bool

	err := d.db.QueryRow(`SELECT 1 FROM sqlite_master WHERE type='table' AND name=?`, d.table).Scan(&exists)
	if err != sql.ErrNoRows && err != nil {
		return 0, err
	}

	if !exists {
		return 0, nil
	}

	err = d.db.QueryRow(`SELECT version FROM ` + d.table + ` ORDER BY Version DESC`).Scan(&version)
	if err != sql.ErrNoRows && err != nil {
		return 0, err
	}

	return version, nil
}

func ensureTableExists(tx *sql.Tx, table string) error {
	_, err := tx.Exec(`CREATE TABLE IF NOT EXISTS ` + table + ` (version INTEGER NOT NULL PRIMARY KEY)`)
	return err
}

func setVersion(tx *sql.Tx, table string, version int) (err error) {
	_, err = tx.Exec(`INSERT INTO ` + table + ` (version) VALUES (` + strconv.Itoa(version) + `)`)
	return err
}
