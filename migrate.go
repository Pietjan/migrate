package migrate

import (
	"errors"
	"fmt"
	"os"

	"github.com/pietjan/migrate/database"
	"github.com/pietjan/migrate/database/sqlite"
	"github.com/pietjan/migrate/source"
	"github.com/pietjan/migrate/source/file"
)

type Database = database.Driver
type Source = source.Driver

type Migrate interface {
	Run() error
}

func New(source Source, database Database) Migrate {
	return &migrate{
		source:   source,
		database: database,
	}
}

func FromFile(options ...file.Option) Source {
	return file.New(options...)
}

func ToSqlite(options ...sqlite.Option) Database {
	return sqlite.New(options...)
}

type migrate struct {
	source   Source
	database Database
}

func (m migrate) Run() error {
	current, err := m.database.Version()
	if err != nil {
		return err
	}

	for {
		version, err := m.source.Next(current)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}

		if errors.Is(err, os.ErrNotExist) {
			break
		}

		migration, file, err := m.source.Read(version)
		if err != nil {
			return err
		}
		defer migration.Close()

		if err := m.database.Run(migration, version); err != nil {
			return fmt.Errorf(`migration %s failed: %w`, file, err)
		}

		current = version
	}

	return nil
}
