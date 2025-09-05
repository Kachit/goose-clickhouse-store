package goose_clickhouse_store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/pressly/goose/v3/database"
	"time"
)

var quorumSettings = clickhouse.Settings{
	"insert_quorum":                 "all",
	"insert_quorum_timeout":         60000,
	"insert_distributed_sync":       1,
	"select_sequential_consistency": 1,
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
	conn                             clickhouse.Conn
	distributedMigrationsTableConfig DistributedMigrationsTableConfig
	localMigrationsTableConfig       LocalMigrationsTableConfig
}

func NewStore(
	conn clickhouse.Conn,
	distributedMigrationsTableConfig DistributedMigrationsTableConfig,
	localMigrationsTableConfig LocalMigrationsTableConfig,
) (*Store, error) {
	if distributedMigrationsTableConfig.TableName == localMigrationsTableConfig.TableName {
		return nil, errors.New("distributed and local migrations table names must be different")
	}

	return &Store{
		conn:                             conn,
		distributedMigrationsTableConfig: distributedMigrationsTableConfig,
		localMigrationsTableConfig:       localMigrationsTableConfig,
	}, nil
}

func (s *Store) Tablename() string {
	return s.distributedMigrationsTableConfig.TableName
}

// CreateVersionTable creates the version table, which is used to track migrations.
func (s *Store) CreateVersionTable(ctx context.Context, _ database.DBTxConn) error {
	ctx = clickhouse.Context(ctx, clickhouse.WithSettings(quorumSettings))

	localTableSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s ON CLUSTER %s (
			version_id Int64,
			is_applied UInt8,
			date Date DEFAULT now(),
			tstamp DateTime DEFAULT now()
		) ENGINE = ReplicatedMergeTree('%s', '%s')
		ORDER BY version_id`,
		s.localMigrationsTableConfig.Database,
		s.localMigrationsTableConfig.TableName+"_local",
		s.distributedMigrationsTableConfig.Cluster,
		s.localMigrationsTableConfig.ZooKeeperPath,
		s.localMigrationsTableConfig.ReplicaName,
	)

	if err := s.conn.Exec(ctx, localTableSQL); err != nil {
		return fmt.Errorf("create local migrations table: %w", err)
	}

	distributedTableSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s ON CLUSTER %s AS %s.%s
		ENGINE = Distributed(
			%s,
			'%s',
			'%s',
			%s
		)`,
		s.distributedMigrationsTableConfig.Database,
		s.distributedMigrationsTableConfig.TableName,
		s.distributedMigrationsTableConfig.Cluster,
		s.localMigrationsTableConfig.Database,
		s.localMigrationsTableConfig.TableName,
		s.distributedMigrationsTableConfig.Cluster,
		s.distributedMigrationsTableConfig.Database,
		s.localMigrationsTableConfig.TableName,
		s.distributedMigrationsTableConfig.ShardingKey,
	)

	if err := s.conn.Exec(ctx, distributedTableSQL); err != nil {
		return fmt.Errorf("create distributed migrations table: %w", err)
	}

	return nil
}

// Insert a version id into the version table.
func (s *Store) Insert(ctx context.Context, _ database.DBTxConn, req database.InsertRequest) error {
	ctx = clickhouse.Context(ctx, clickhouse.WithSettings(quorumSettings))

	sql := fmt.Sprintf(`
		INSERT INTO %s.%s (version_id, is_applied, date, tstamp)
		VALUES (?, ?, now(), now())`,
		s.distributedMigrationsTableConfig.Database,
		s.distributedMigrationsTableConfig.TableName,
	)

	if err := s.conn.Exec(ctx, sql, req.Version, 1); err != nil {
		return fmt.Errorf("insert migration version %d: %w", req.Version, err)
	}

	return nil
}

// Delete removes a version id from the version table.
func (s *Store) Delete(ctx context.Context, _ database.DBTxConn, version int64) error {
	query := fmt.Sprintf(`ALTER TABLE %s.%s DELETE WHERE version_id = ? SETTINGS mutations_sync = 2`,
		s.distributedMigrationsTableConfig.Database,
		s.distributedMigrationsTableConfig.TableName,
	)

	if err := s.conn.Exec(ctx, query, version); err != nil {
		return fmt.Errorf("delete migration version %d: %w", version, err)
	}

	return nil
}

// GetMigration retrieves a single migration by version id. If the query succeeds, but the
// version is not found, this method must return [ErrVersionNotFound].
func (s *Store) GetMigration(
	ctx context.Context,
	_ database.DBTxConn,
	version int64,
) (*database.GetMigrationResult, error) {
	ctx = clickhouse.Context(ctx, clickhouse.WithSettings(quorumSettings))

	query := fmt.Sprintf(`
		SELECT is_applied, tstamp
		FROM %s.%s
		WHERE version_id = ?
		LIMIT 1`,
		s.distributedMigrationsTableConfig.Database,
		s.distributedMigrationsTableConfig.TableName,
	)

	row := s.conn.QueryRow(ctx, query, version)

	var isApplied uint8
	var timestamp time.Time

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
func (s *Store) GetLatestVersion(ctx context.Context, _ database.DBTxConn) (int64, error) {
	ctx = clickhouse.Context(ctx, clickhouse.WithSettings(quorumSettings))

	query := fmt.Sprintf("SELECT MAX(version_id) FROM %s.%s",
		s.distributedMigrationsTableConfig.Database,
		s.distributedMigrationsTableConfig.TableName,
	)

	row := s.conn.QueryRow(ctx, query)

	var version sql.NullInt64

	if err := row.Scan(&version); err != nil {
		return 0, fmt.Errorf("get latest version: %w", err)
	}

	if !version.Valid {
		return 0, database.ErrVersionNotFound
	}

	return version.Int64, nil
}

// ListMigrations retrieves all migrations sorted in descending order by id or timestamp. If
// there are no migrations, return an empty slice with no error. Typically, this method will return
// at least one migration because the initial version (0) is always inserted into the version
// table when it is created.
func (s *Store) ListMigrations(ctx context.Context, _ database.DBTxConn) ([]*database.ListMigrationsResult, error) {
	ctx = clickhouse.Context(ctx, clickhouse.WithSettings(quorumSettings))

	query := fmt.Sprintf(`
		SELECT version_id, is_applied
		FROM %s.%s
		ORDER BY version_id DESC`,
		s.distributedMigrationsTableConfig.Database,
		s.distributedMigrationsTableConfig.TableName,
	)

	rows, err := s.conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}

	defer rows.Close()

	var migrations []*database.ListMigrationsResult

	for rows.Next() {
		var versionID int64
		var isApplied uint8
		var timestamp time.Time

		if err := rows.Scan(&versionID, &isApplied, &timestamp); err != nil {
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
