package database

import "io"

type Driver interface {
	// Run the migration & update the schema version
	Run(r io.Reader, version int) error
	// Version returns the current schema version
	Version() (version int, err error)
}
