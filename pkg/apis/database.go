package apis

import (
	"fmt"
	"net/url"
	"strings"
)

// This should be moved to artifact
type Database struct {
	Username string
	Password string
	Protocal string
	Host     string
	Port     string
	Database string
	//Parameters string
}

// Get data source name (DSN)
func (db Database) ToDSN() string {
	//TODO: sqlite?

	var user string
	if db.Password != "" {
		user = fmt.Sprintf("%s:%s", db.Username, db.Password)
	} else {
		user = db.Username
	}

	var address string
	if db.Port != "" {
		address = fmt.Sprintf("%s:%s", db.Host, db.Port)
	} else {
		address = db.Host
	}

	dsn := fmt.Sprintf("%s://%s@%s", db.Protocal, user, address)
	if db.Database != "" {
		dsn = fmt.Sprintf("%s/%s", dsn, db.Database)
	}

	return dsn
}

func (db Database) ToURL() (*url.URL, error) {
	return url.Parse(db.ToDSN())
}

func ParseDSN(dsn string) (Database, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return Database{}, err
	}
	password, _ := u.User.Password() //TODO: when it returns false
	return Database{
		Username: u.User.Username(),
		Password: password,
		Protocal: u.Scheme,
		Host:     u.Host,
		Port:     u.Port(),
		Database: strings.TrimLeft(u.Path, "/"),
	}, nil
}
