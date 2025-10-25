package goose_clickhouse_store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/huandu/go-sqlbuilder"
	"github.com/pressly/goose/v3/database"
)

var DistributedMigrationsTableConfigByDefault = DistributedMigrationsTableConfig{
	Cluster:     "default",
	ShardingKey: "rand()",
}

var LocalMigrationsTableConfigByDefault = LocalMigrationsTableConfig{
	ReplicaName: "{replica}",
}

type DistributedMigrationsTableConfig struct {
	Cluster     string // Ex: default
	Database    string // Ex: mydb
	TableName   string // Ex: migrations
	ShardingKey string // Ex: rand()
}

type LocalMigrationsTableConfig struct {
	ZooKeeperPath string
	ReplicaName   string
	Database      string
	TableName     string // Ex: migrations_local
}

type Store struct {
	distributedTableConfig DistributedMigrationsTableConfig
	localTableConfig       LocalMigrationsTableConfig
}

func NewStore(
	distributedMigrationsTableConfig DistributedMigrationsTableConfig,
	localMigrationsTableConfig LocalMigrationsTableConfig,
) (*Store, error) {
	if distributedMigrationsTableConfig.TableName == localMigrationsTableConfig.TableName {
		return nil, errors.New("distributed and local migrations table names must be different")
	}

	return &Store{
		distributedTableConfig: distributedMigrationsTableConfig,
		localTableConfig:       localMigrationsTableConfig,
	}, nil
}

func (s *Store) Tablename() string {
	return s.distributedTableConfig.TableName
}

func (s *Store) TablenameFull() string {
	return fmt.Sprintf("%s.%s",
		s.distributedTableConfig.Database,
		s.distributedTableConfig.TableName,
	)
}

// CreateVersionTable creates the version table, which is used to track migrations.
func (s *Store) CreateVersionTable(ctx context.Context, tx database.DBTxConn) error {
	localTable := fmt.Sprintf("%s.%s ON CLUSTER %s",
		s.localTableConfig.Database,
		s.localTableConfig.TableName,
		s.distributedTableConfig.Cluster,
	)
	localTableEngine := fmt.Sprintf("ENGINE = ReplicatedMergeTree('%s', '%s')",
		s.localTableConfig.ZooKeeperPath,
		s.localTableConfig.ReplicaName,
	)

	localCtb := sqlbuilder.NewCreateTableBuilder()
	localCtb.CreateTable(localTable).IfNotExists()
	localCtb.Define("version_id", "Int64")
	localCtb.Define("is_applied", "UInt8")
	localCtb.Define("date", "Date", "DEFAULT", "now()")
	localCtb.Define("tstamp", "DateTime", "DEFAULT", "now()")
	localCtb.Option(localTableEngine, "ORDER BY version_id")

	if _, err := tx.ExecContext(ctx, localCtb.String()); err != nil {
		return fmt.Errorf("create local migrations table: %w", err)
	}

	distributedTable := fmt.Sprintf("%s.%s ON CLUSTER %s AS %s.%s",
		s.distributedTableConfig.Database,
		s.distributedTableConfig.TableName,
		s.distributedTableConfig.Cluster,
		s.localTableConfig.Database,
		s.localTableConfig.TableName,
	)
	distributedTableEngine := fmt.Sprintf("ENGINE = Distributed(%s, '%s', '%s', %s)",
		s.distributedTableConfig.Cluster,
		s.distributedTableConfig.Database,
		s.localTableConfig.TableName,
		s.distributedTableConfig.ShardingKey,
	)

	distributedCtb := sqlbuilder.NewCreateTableBuilder()
	distributedCtb.CreateTable(distributedTable).IfNotExists()
	distributedCtb.Option(distributedTableEngine)

	if _, err := tx.ExecContext(ctx, distributedCtb.String()); err != nil {
		return fmt.Errorf("create distributed migrations table: %w", err)
	}

	return nil
}

// Insert a version id into the version table.
func (s *Store) Insert(ctx context.Context, tx database.DBTxConn, req database.InsertRequest) error {
	qb := sqlbuilder.NewInsertBuilder()
	query, args := qb.InsertInto(s.TablenameFull()).
		Cols("version_id", "is_applied").
		Values(req.Version, 1).
		Build()

	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("insert migration version %d: %w", req.Version, err)
	}

	return nil
}

// Delete removes a version id from the version table.
func (s *Store) Delete(ctx context.Context, tx database.DBTxConn, version int64) error {
	query := fmt.Sprintf(`ALTER TABLE %s DELETE WHERE version_id = ? SETTINGS mutations_sync = 2`,
		s.TablenameFull(),
	)

	if _, err := tx.ExecContext(ctx, query, version); err != nil {
		return fmt.Errorf("delete migration version %d: %w", version, err)
	}

	return nil
}

// GetMigration retrieves a single migration by version id. If the query succeeds, but the
// version is not found, this method must return [ErrVersionNotFound].
func (s *Store) GetMigration(
	ctx context.Context,
	tx database.DBTxConn,
	version int64,
) (*database.GetMigrationResult, error) {
	var isApplied uint8
	var timestamp time.Time

	qb := sqlbuilder.NewSelectBuilder()
	qb.Select("is_applied", "tstamp").
		From(s.TablenameFull()).
		Where(qb.Equal("version_id", version)).
		Limit(1)

	query, args := qb.Build()
	row := tx.QueryRowContext(ctx, query, args...)

	if err := row.Scan(&isApplied, &timestamp); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w: %d", database.ErrVersionNotFound, version)
		}

		return nil, fmt.Errorf("get migration %d: %w", version, err)
	}

	return &database.GetMigrationResult{
		Timestamp: timestamp,
		IsApplied: isApplied == 1,
	}, nil

}

// GetLatestVersion retrieves the last applied migration version. If no migrations exist, this
// method must return [ErrVersionNotFound].
func (s *Store) GetLatestVersion(ctx context.Context, tx database.DBTxConn) (int64, error) {
	var version sql.NullInt64

	qb := sqlbuilder.NewSelectBuilder()
	qb.Select("MAX(version_id)").
		From(s.TablenameFull())
	query, _ := qb.Build()

	row := tx.QueryRowContext(ctx, query)
	if err := row.Err(); err != nil {
		return -1, fmt.Errorf("get latest version: %w", err)
	}

	if err := row.Scan(&version); err != nil {
		return -1, fmt.Errorf("get latest version: %w", err)
	}

	if !version.Valid {
		return -1, database.ErrVersionNotFound
	}

	return version.Int64, nil
}

// ListMigrations retrieves all migrations sorted in descending order by id or timestamp. If
// there are no migrations, return an empty slice with no error. Typically, this method will return
// at least one migration because the initial version (0) is always inserted into the version
// table when it is created.
func (s *Store) ListMigrations(ctx context.Context, tx database.DBTxConn) ([]*database.ListMigrationsResult, error) {
	var migrations []*database.ListMigrationsResult

	qb := sqlbuilder.NewSelectBuilder()
	qb.Select("version_id", "is_applied").
		From(s.TablenameFull()).
		OrderByDesc("version_id")

	query, _ := qb.Build()

	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}

	defer rows.Close()

	for rows.Next() {
		var versionID int64
		var isApplied uint8

		if err := rows.Scan(&versionID, &isApplied); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}

		migrations = append(migrations, &database.ListMigrationsResult{
			Version:   versionID,
			IsApplied: isApplied == 1,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating rows: %w", err)
	}

	return migrations, nil
}
