package database

import (
	"io"
)

//close stream
func mustClose(c io.Closer) {
	if err := c.Close(); err != nil {
		panic(err)
	}
}
