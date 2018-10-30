package database

import (
	"database/sql"
	"fmt"
	"net/url"
)

type DatabaseDriver interface {
	// Check the dependencies, e.g: mysqldump,pg_dump.
	CheckDependency() error
	// Ping verifies a connection to the database server. It does not verify whether the
	// specified database exists.
	Ping(*url.URL) error
	// Creates a new database connection
	Open(*url.URL) (*sql.DB, error)
	// Dump the current database
	Export(*url.URL) (string, error)
	// Restore the database
	Import(*url.URL, string) error
	// Lock the databases
	Lock(*url.URL) error
	// Unlocak the databases
	UnLock(*url.URL) error
	// Get a basic summary of all tables, which can be used for validation.
	GetSum(*url.URL) (map[string]int, error)
}

var drivers = map[string]DatabaseDriver{}

//Register driver
func RegisterDriver(drv DatabaseDriver, scheme string) {
	drivers[scheme] = drv
}

// GetDriver loads a database driver by name
func GetDriver(name string) (DatabaseDriver, error) {
	if val, ok := drivers[name]; ok {
		return val, nil
	}

	return nil, fmt.Errorf("unsupported driver: %s", name)
}
