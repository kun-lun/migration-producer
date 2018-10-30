package migrator

import (
	"errors"
	"log"
	"reflect"

	migration "github.com/kun-lun/migration-producer/pkg/apis"
	"github.com/kun-lun/migration-producer/pkg/database"
)

const FullDump = "fulldump"

type DatabaseMigrator struct {
	Method      string
	Validate    bool
	Source      migration.Database
	Destination migration.Database
}

func NewDatabaseMigrator(src migration.Database, dest migration.Database) migration.Migrator {
	return &DatabaseMigrator{
		Method:      FullDump,
		Validate:    false,
		Source:      src,
		Destination: dest,
	}
}

func (dm *DatabaseMigrator) Migrate() error {
	var srcSum, dstSum map[string]int

	if err := dm.CheckCompatibility(); err != nil {
		return err
	}

	if err := dm.CheckConnections(); err != nil {
		return err
	}

	drv, err := database.GetDriver(dm.Source.Protocal)
	if err != nil {
		return err
	}

	if err := drv.CheckDependency(); err != nil {
		return err
	}

	src, _ := dm.Source.ToURL() //error already checked by CheckConnections

	locked := false
	if dm.Validate {
		drv.Lock(src) //TODO: when using the same host, mysql will hang in create database when it is locked, unlocked.
		locked = true

		//get summary, which should be compared with dest
		if srcSum, err = drv.GetSum(src); err != nil {
			drv.UnLock(src)
			return err
		}
	}

	//export
	fn, err := drv.Export(src)
	if err != nil {
		if locked {
			drv.UnLock(src)
		}
		return err
	}
	log.Println(fn)

	if locked {
		drv.UnLock(src)
	}

	// import
	dst, _ := dm.Destination.ToURL()
	if err := drv.Import(dst, fn); err != nil {
		return err
	}

	//validate
	if dm.Validate {
		if dstSum, err = drv.GetSum(dst); err != nil {
			return err
		}

		eq := reflect.DeepEqual(srcSum, dstSum)
		if !eq {
			log.Println("src and dst have different sum.", srcSum, dstSum)
			return errors.New("Failed to check summary")
		}
	}

	return nil
}

// check if the source and dest has compatible schema,version
func (dm *DatabaseMigrator) CheckCompatibility() error {
	if dm.Source.Protocal != dm.Destination.Protocal {
		return errors.New("Not compatiable protocal")
	}
	//TODO: version
	return nil
}

func (dm *DatabaseMigrator) CheckConnections() error {
	drv, err := database.GetDriver(dm.Source.Protocal)
	if err != nil {
		log.Println("Failed to get driver for", dm.Source.Protocal)
		return err
	}

	src_url, err := dm.Source.ToURL()
	err = drv.Ping(src_url)
	if err != nil {
		log.Println("Failed to Ping", src_url)
		return err
	}

	dest_url, err := dm.Destination.ToURL()
	err = drv.Ping(dest_url)
	if err != nil {
		log.Println("Failed to Ping", dest_url)
		return err
	}

	return nil
}
