package subcommands

import (
	migration "github.com/kun-lun/migration-producer/pkg/apis"
	"github.com/kun-lun/migration-producer/pkg/migrator"
)

type DBMigrateCommand struct {
	SourceDSN      string `long:"source-dsn" env:"SOURCE_DSN"`
	DestinationDSN string `long:"dest-dsn" env:"DEST_DSN"`
}

func (c *DBMigrateCommand) Execute([]string) error {

	//src, err := migration.ParseDSN(c.SourceDSN)
	//dest, err := migration.ParseDSN(c.DestinationDSN)

	// test
	src := migration.Database{
		Username: "g2",
		Password: "g2",
		Protocal: "mysql",
		Host:     "127.0.0.1",
		Database: "ops",
	}

	dest := migration.Database{
		Username: "g2",
		Password: "g2",
		Protocal: "mysql",
		Host:     "127.0.0.1",
		Database: "ops2",
	}

	dm := migrator.NewDatabaseMigrator(src, dest)
	err := dm.Migrate()
	if err != nil {
		return err
	}
	return nil
}
