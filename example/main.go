package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/pressly/goose/v3"
	"log"

	goose_clickhouse_store "github.com/dimuska139/goose-clickhouse-store"
	"github.com/dimuska139/goose-clickhouse-store/example/migrations"
)

func main() {
	clickhouseConfig, err := clickhouse.ParseDSN("")
	if err != nil {
		log.Fatal(err)
	}

	conn, err := clickhouse.Open(clickhouseConfig)
	if err != nil {
		log.Fatal(err)
	}

	defer conn.Close()

	dbName := "mystorage"

	clickhouseStore, err := goose_clickhouse_store.NewStore(
		conn,
		goose_clickhouse_store.DistributedMigrationsTableConfig{
			Cluster:     "default",
			Database:    dbName,
			TableName:   "migrations",
			ShardingKey: "rand()",
		},
		goose_clickhouse_store.LocalMigrationsTableConfig{
			ZooKeeperPath: "/clickhouse/tables/{shard}/nats2clickhouse/migrations",
			ReplicaName:   "{replica}",
			Database:      dbName,
			TableName:     "migrations_local",
		})
	if err != nil {
		log.Fatal(err)
	}

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		log.Fatal(err)
	}

	p, err := goose.NewProvider(
		"",
		db, // We don't use this DB, but goose requires it
		migrations.Embed,
		goose.WithStore(clickhouseStore),
	)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	_, err = p.Up(ctx)
	if err != nil {
		log.Fatal(err)
	}

	version, err := p.GetDBVersion(ctx)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Current database version is: %d", version)
}
