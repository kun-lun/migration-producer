package apis

type Migrator interface {
	Migrate() error
}
