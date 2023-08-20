package source

import (
	"io"
)

type Driver interface {
	Next(version int) (next int, err error)
	Read(version int) (r io.ReadCloser, file string, err error)
}
