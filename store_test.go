package goose_clickhouse_store

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pressly/goose/v3/database"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

var createLocalTableSql = `CREATE TABLE IF NOT EXISTS db.migrations_part ON CLUSTER default (version_id Int64, is_applied UInt8, date Date DEFAULT now(), tstamp DateTime DEFAULT now()) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/nats2clickhouse/migrations', '{replica}') ORDER BY version_id`
var createDistributedTableSql = `CREATE TABLE IF NOT EXISTS db.migrations ON CLUSTER default AS db.migrations_part ENGINE = Distributed(default, 'db', 'migrations_part', rand())`

type StoreTestSuite struct {
	suite.Suite
	ctx      context.Context
	db       *sql.DB
	mock     sqlmock.Sqlmock
	testable *Store
}

func (suite *StoreTestSuite) SetupTest() {
	db, mock, _ := sqlmock.New()
	suite.ctx = context.Background()
	suite.db = db
	suite.mock = mock
	suite.testable, _ = NewStore(DistributedMigrationsTableConfig{
		Cluster:     "default",
		Database:    "db",
		TableName:   "migrations",
		ShardingKey: "rand()",
	},
		LocalMigrationsTableConfig{
			ZooKeeperPath: "/clickhouse/tables/{shard}/nats2clickhouse/migrations",
			ReplicaName:   "{replica}",
			Database:      "db",
			TableName:     "migrations_part",
		})
	suite.mock.MatchExpectationsInOrder(false)
}

func (suite *StoreTestSuite) TestDistributedAndLocalTableHasSameNameError() {
	store, err := NewStore(DistributedMigrationsTableConfig{
		Cluster:     "default",
		Database:    "db",
		TableName:   "migrations",
		ShardingKey: "rand()",
	},
		LocalMigrationsTableConfig{
			ZooKeeperPath: "/clickhouse/tables/{shard}/nats2clickhouse/migrations",
			ReplicaName:   "{replica}",
			Database:      "db",
			TableName:     "migrations",
		})
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), "distributed and local migrations table names must be different", err.Error())
	assert.Empty(suite.T(), store)
}

func (suite *StoreTestSuite) TestTableName() {
	suite.Equal("migrations", suite.testable.Tablename())
}

func (suite *StoreTestSuite) TestTableNameFull() {
	suite.Equal("db.migrations", suite.testable.TablenameFull())
}

func (suite *StoreTestSuite) TestCreateVersionTableSuccess() {
	suite.mock.ExpectExec(regexp.QuoteMeta(createLocalTableSql)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	suite.mock.ExpectExec(regexp.QuoteMeta(createDistributedTableSql)).
		WillReturnResult(sqlmock.NewResult(0, 0))

	err := suite.testable.CreateVersionTable(suite.ctx, suite.db)
	assert.NoError(suite.T(), err)
}

func (suite *StoreTestSuite) TestCreateVersionTableLocalTableError() {
	suite.mock.ExpectExec(regexp.QuoteMeta(createLocalTableSql)).
		WillReturnError(errors.New("test error"))

	err := suite.testable.CreateVersionTable(suite.ctx, suite.db)
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), "create local migrations table: test error", err.Error())
}

func (suite *StoreTestSuite) TestCreateVersionTableDistributedTableError() {
	suite.mock.ExpectExec(regexp.QuoteMeta(createLocalTableSql)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	suite.mock.ExpectExec(regexp.QuoteMeta(createDistributedTableSql)).
		WillReturnError(errors.New("test error"))

	err := suite.testable.CreateVersionTable(suite.ctx, suite.db)
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), "create distributed migrations table: test error", err.Error())
}

func (suite *StoreTestSuite) TestInsertSuccess() {
	req := database.InsertRequest{Version: 1234567}

	suite.mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO db.migrations (version_id, is_applied) VALUES (?, ?)`)).
		WithArgs(req.Version, 1).
		WillReturnResult(sqlmock.NewResult(1, 0))

	err := suite.testable.Insert(suite.ctx, suite.db, req)
	assert.NoError(suite.T(), err)
}

func (suite *StoreTestSuite) TestInsertError() {
	req := database.InsertRequest{Version: 1234567}

	suite.mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO db.migrations (version_id, is_applied) VALUES (?, ?)`)).
		WithArgs(req.Version, 1).
		WillReturnError(errors.New("error"))

	err := suite.testable.Insert(suite.ctx, suite.db, req)
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), "insert migration version 1234567: error", err.Error())
}

func (suite *StoreTestSuite) TestDeleteSuccess() {
	version := int64(1234567)

	suite.mock.ExpectExec(regexp.QuoteMeta(`ALTER TABLE db.migrations DELETE WHERE version_id = ? SETTINGS mutations_sync = 2`)).
		WithArgs(version).
		WillReturnResult(sqlmock.NewResult(0, 1))

	err := suite.testable.Delete(suite.ctx, suite.db, version)
	assert.NoError(suite.T(), err)
}

func (suite *StoreTestSuite) TestDeleteError() {
	version := int64(1234567)

	suite.mock.ExpectExec(regexp.QuoteMeta(`ALTER TABLE db.migrations DELETE WHERE version_id = ? SETTINGS mutations_sync = 2`)).
		WithArgs(version).
		WillReturnError(errors.New("error"))

	err := suite.testable.Delete(suite.ctx, suite.db, 1234567)
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), "delete migration version 1234567: error", err.Error())
}

func (suite *StoreTestSuite) TestGetMigrationFound() {
	version := int64(1234567)
	ts := time.Now().UTC()

	suite.mock.ExpectQuery(regexp.QuoteMeta(`SELECT is_applied, tstamp FROM db.migrations WHERE version_id = ? LIMIT ?`)).
		WithArgs(version, 1).
		WillReturnRows(sqlmock.NewRows([]string{"is_applied", "tstamp"}).AddRow(1, ts))

	result, err := suite.testable.GetMigration(suite.ctx, suite.db, version)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), true, result.IsApplied)
}

func (suite *StoreTestSuite) TestGetMigrationNotFound() {
	version := int64(1234567)

	suite.mock.ExpectQuery(regexp.QuoteMeta(`SELECT is_applied, tstamp FROM db.migrations WHERE version_id = ? LIMIT ?`)).
		WithArgs(version, 1).
		WillReturnRows(sqlmock.NewRows([]string{"is_applied", "tstamp"}))

	res, err := suite.testable.GetMigration(suite.ctx, suite.db, 1234567)
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), "version not found: 1234567", err.Error())
	assert.Empty(suite.T(), res)
}

func (suite *StoreTestSuite) TestGetMigrationError() {
	version := int64(1234567)

	suite.mock.ExpectQuery(regexp.QuoteMeta(`SELECT is_applied, tstamp FROM db.migrations WHERE version_id = ? LIMIT ?`)).
		WithArgs(version, 1).
		WillReturnError(errors.New("error"))

	res, err := suite.testable.GetMigration(suite.ctx, suite.db, 1234567)
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), "get migration 1234567: error", err.Error())
	assert.Empty(suite.T(), res)
}

func (suite *StoreTestSuite) TestGetLatestVersionFound() {
	suite.mock.ExpectQuery(regexp.QuoteMeta(`SELECT MAX(version_id) FROM db.migrations`)).
		WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow(123))

	res, err := suite.testable.GetLatestVersion(suite.ctx, suite.db)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), int64(123), res)
}

func (suite *StoreTestSuite) TestGetLatestVersionNotFound() {
	suite.mock.ExpectQuery(regexp.QuoteMeta(`SELECT MAX(version_id) FROM db.migrations`)).
		WillReturnRows(sqlmock.NewRows([]string{"version"}))

	res, err := suite.testable.GetLatestVersion(suite.ctx, suite.db)
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), "get latest version: sql: no rows in result set", err.Error())
	assert.Equal(suite.T(), int64(-1), res)
}

//func (suite *StoreTestSuite) TestGetLatestVersionInvalid() {
//	suite.mock.ExpectQuery(regexp.QuoteMeta(`SELECT MAX(version_id) FROM db.migrations`)).
//		WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow(0))
//
//	res, err := suite.testable.GetLatestVersion(suite.ctx, suite.db)
//	assert.Error(suite.T(), err)
//	//assert.Equal(suite.T(), "get latest version: sql: no rows in result set", err.Error())
//	assert.Equal(suite.T(), int64(0), res)
//}

func (suite *StoreTestSuite) TestGetLatestVersionError() {
	suite.mock.ExpectQuery(regexp.QuoteMeta(`SELECT MAX(version_id) FROM db.migrations`)).
		WillReturnError(errors.New("error"))

	res, err := suite.testable.GetLatestVersion(suite.ctx, suite.db)
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), "get latest version: error", err.Error())
	assert.Equal(suite.T(), int64(-1), res)
}

func (suite *StoreTestSuite) TestListMigrationsFound() {
	suite.mock.ExpectQuery(regexp.QuoteMeta(`SELECT version_id, is_applied FROM db.migrations ORDER BY version_id DESC`)).
		WillReturnRows(sqlmock.NewRows([]string{"version_id", "is_applied"}).AddRow(123, 1).AddRow(456, 0))

	res, err := suite.testable.ListMigrations(suite.ctx, suite.db)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), res, 2)
	assert.Equal(suite.T(), int64(123), res[0].Version)
	assert.Equal(suite.T(), true, res[0].IsApplied)
	assert.Equal(suite.T(), int64(456), res[1].Version)
	assert.Equal(suite.T(), false, res[1].IsApplied)
}

func (suite *StoreTestSuite) TestListMigrationsNotFound() {
	suite.mock.ExpectQuery(regexp.QuoteMeta(`SELECT version_id, is_applied FROM db.migrations ORDER BY version_id DESC`)).
		WillReturnRows(sqlmock.NewRows([]string{"version_id", "is_applied"}))

	res, err := suite.testable.ListMigrations(suite.ctx, suite.db)
	assert.NoError(suite.T(), err)
	assert.Len(suite.T(), res, 0)
}

func (suite *StoreTestSuite) TestListMigrationsScanError() {
	suite.mock.ExpectQuery(regexp.QuoteMeta(`SELECT version_id, is_applied FROM db.migrations ORDER BY version_id DESC`)).
		WillReturnRows(sqlmock.NewRows([]string{"version_id", "is_applied"}).AddRow(1.1, 10.1))

	res, err := suite.testable.ListMigrations(suite.ctx, suite.db)
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), `scan: sql: Scan error on column index 0, name "version_id": converting driver.Value type float64 ("1.1") to a int64: invalid syntax`, err.Error())
	assert.Empty(suite.T(), res)
}

func (suite *StoreTestSuite) TestListMigrationsError() {
	suite.mock.ExpectQuery(regexp.QuoteMeta(`SELECT version_id, is_applied FROM db.migrations ORDER BY version_id DESC`)).
		WillReturnError(errors.New("error"))

	res, err := suite.testable.ListMigrations(suite.ctx, suite.db)
	assert.Error(suite.T(), err)
	assert.Equal(suite.T(), "query: error", err.Error())
	assert.Len(suite.T(), res, 0)
}

func TestStoreTestSuite(t *testing.T) {
	suite.Run(t, new(StoreTestSuite))
}
