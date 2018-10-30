package database

import (
	"database/sql"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"os/exec"
	"strings"

	_ "github.com/go-sql-driver/mysql" // mysql driver for database/sql
	"github.com/kun-lun/migration-producer/pkg/utils"
)

func init() {
	RegisterDriver(MySQLDriver{}, "mysql")
}

// MySQLDriver provides top level database functions
type MySQLDriver struct {
}

// check if mysql, mysqldump exist in env
func (drv MySQLDriver) CheckDependency() error {
	cmds := []string{"mysql", "mysqldump"}
	log.Printf("Checking cmd dependency: %s", cmds)

	for _, cmd := range cmds {
		if _, err := exec.LookPath(cmd); err != nil {
			log.Println(err)
			return err
		}
	}
	return nil
}

func (drv MySQLDriver) Ping(u *url.URL) error {
	db, err := drv.openRootDB(u)
	if err != nil {
		return err
	}
	defer mustClose(db)

	return db.Ping()
}

func (drv MySQLDriver) Open(u *url.URL) (*sql.DB, error) {
	return sql.Open("mysql", normalizeMySQLURL(u))
}

func (drv MySQLDriver) Export(u *url.URL) (string, error) {
	tmpfile, err := ioutil.TempFile("", "mysql-")
	if err != nil {
		log.Println(err)
		return "", err
	}
	defer tmpfile.Close()

	log.Printf("Will export mysql db to file: %s", tmpfile.Name())

	args := mysqldumpArgs(u)
	output, err := utils.RunCommandOutTOFile("mysqldump", tmpfile, args...)
	if err != nil {
		return "", err
	}

	f, err := os.Stat(tmpfile.Name())
	if err != nil {
		log.Printf("Error stat of exported file: %s", err)
		return "", err
	}
	if f.Size() == 0 {
		log.Printf("Nothing was exported to file: %s", tmpfile.Name())
		return "", errors.New("Nothing exported")
	}

	log.Printf("mysqldump output: %s", output)
	return tmpfile.Name(), nil
}

func (drv MySQLDriver) Import(u *url.URL, filename string) error {
	if err := drv.CreateDbIfNotExists(u); err != nil {
		log.Println(err)
		return err
	}

	log.Printf("Will import mysql db from file: %s", filename)

	f, err := os.Open(filename)
	if err != nil {
		log.Println("Failed to open file", filename)
		return err
	}
	args := mysqlArgs(u)
	_, err = utils.RunCommandWithStdin("mysql", f, args...)
	if err != nil {
		return err
	}

	return nil
}

func (drv MySQLDriver) Lock(u *url.URL) error {
	db, err := drv.Open(u)
	if err != nil {
		log.Printf("Failed to open db %s", u)
		return err
	}

	if _, err := db.Exec("FLUSH TABLES WITH READ LOCK"); err != nil {
		log.Printf("Failed to lock db %s", u)
		return err
	}
	log.Println("LOCKED DATABASE:", u)

	return nil
}

func (drv MySQLDriver) UnLock(u *url.URL) error {
	db, err := drv.Open(u)
	if err != nil {
		log.Printf("Failed to open db %s", u)
		return err
	}

	if _, err := db.Exec("UNLOCK TABLES"); err != nil {
		log.Printf("Failed to unlock db %s", u)
		return err
	}
	log.Println("UNLOCKED DATABASE:", u)

	return nil
}

func (drv MySQLDriver) GetSum(u *url.URL) (map[string]int, error) {
	sum := make(map[string]int)

	name := databaseName(u)

	db, err := drv.Open(u)
	if err != nil {
		log.Printf("Failed to open db %s", name)
		return nil, err
	}

	tables := []string{}
	res, err := db.Query("SHOW TABLES")
	if err != nil {
		return nil, err
	}
	for res.Next() {
		var table string
		res.Scan(&table)
		tables = append(tables, table)
	}

	for _, table := range tables {
		var count int
		query := fmt.Sprintf("SELECT COUNT(*) FROM %s", table)
		row := db.QueryRow(query)
		if err != nil {
			return nil, err
		}
		row.Scan(&count)
		sum[table] = count
	}

	log.Println(sum)

	return sum, nil
}

// helpers

// normalize the URL
func normalizeMySQLURL(u *url.URL) string {
	normalizedURL := *u
	normalizedURL.Scheme = ""

	// set default port
	if normalizedURL.Port() == "" {
		normalizedURL.Host = fmt.Sprintf("%s:3306", normalizedURL.Host)
	}

	// host format required by go-sql-driver/mysql
	normalizedURL.Host = fmt.Sprintf("tcp(%s)", normalizedURL.Host)

	query := normalizedURL.Query()
	query.Set("multiStatements", "true")
	normalizedURL.RawQuery = query.Encode()

	str := normalizedURL.String()
	return strings.TrimLeft(str, "/")
}

//mysqlArgs returns command mysql arguments
func mysqlArgs(u *url.URL) []string {
	args := []string{}

	if hostname := u.Hostname(); hostname != "" {
		args = append(args, "--host="+hostname)
	}
	if port := u.Port(); port != "" {
		args = append(args, "--port="+port)
	}
	if username := u.User.Username(); username != "" {
		args = append(args, "--user="+username)
	}
	// mysql recommends against using environment variables to supply password
	// https://dev.mysql.com/doc/refman/5.7/en/password-security-user.html
	if password, set := u.User.Password(); set {
		args = append(args, "--password="+password)
	}
	// add database name
	args = append(args, strings.TrimLeft(u.Path, "/"))

	return args
}

// mysqldumpArgs return arguments for mysqldump
func mysqldumpArgs(u *url.URL) []string {
	// generate CLI arguments
	args := []string{"--opt", "--routines"}
	//"--no-data", "--skip-dump-date", "--skip-add-drop-table"}

	args = append(args, mysqlArgs(u)...)

	return args
}

// databaseName returns the database name from a URL
func databaseName(u *url.URL) string {
	name := u.Path
	if len(name) > 0 && name[:1] == "/" {
		name = name[1:]
	}

	return name
}

// openRootDB open the root database
func (drv MySQLDriver) openRootDB(u *url.URL) (*sql.DB, error) {
	// connect to no particular database
	rootURL := *u
	rootURL.Path = "/"

	return drv.Open(&rootURL)
}

// Create database if it is not exist
func (drv MySQLDriver) CreateDbIfNotExists(u *url.URL) error {
	name := databaseName(u)

	db, err := drv.openRootDB(u)
	if err != nil {
		log.Printf("Failed to open db %s", name)
		return err
	}

	if _, err := db.Exec("CREATE DATABASE IF NOT EXISTS " + name); err != nil {
		log.Printf("Failed to create db %s", name)
		return err
	}
	return nil
}
