package file

import (
	"io"
	"io/fs"
	"os"

	"github.com/pietjan/migrate/source"
)

type Option = func(*driver)

func New(options ...Option) source.Driver {
	d := &driver{
		glob: `*.sql`,
	}

	for _, fn := range options {
		fn(d)
	}

	if d.fs == nil {
		panic(`nil fs.FS`)
	}

	if len(d.glob) == 0 {
		panic(`glob pattern must not be empty`)
	}

	return d
}

func Dir(dir string) Option {
	return func(d *driver) {
		d.fs = os.DirFS(dir)
	}
}

func FS(fs fs.FS) Option {
	return func(d *driver) {
		d.fs = fs
	}
}

func Glob(glob string) Option {
	return func(d *driver) {
		d.glob = glob
	}
}

type driver struct {
	fs   fs.FS
	glob string
}

func (d driver) Next(version int) (next int, err error) {
	migrations, err := migrations(d)
	if len(migrations) == version || version < 0 {
		return -1, os.ErrNotExist
	}

	return version + 1, nil
}

func (d driver) Read(version int) (r io.ReadCloser, file string, err error) {
	migration := version - 1
	migrations, err := migrations(d)
	if len(migrations) < migration || version <= 0 {
		return nil, file, os.ErrNotExist
	}
	file = migrations[migration]
	r, err = d.fs.Open(file)

	return
}

func migrations(d driver) (migrations []string, err error) {
	fs.Sub(d.fs, `.`)
	return fs.Glob(d.fs, d.glob)
}
