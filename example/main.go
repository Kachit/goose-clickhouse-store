package main

import (
	"context"
	"fmt"
	"log"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/pressly/goose/v3"

	goose_clickhouse_store "github.com/kachit/goose-clickhouse-store"
	"github.com/kachit/goose-clickhouse-store/example/migrations"
)

func main() {
	clickhouseConfig, err := clickhouse.ParseDSN("")
	if err != nil {
		log.Fatal(err)
	}

	conn := clickhouse.OpenDB(clickhouseConfig)
	if conn.Ping() != nil {
		log.Fatal(err)
	}

	defer conn.Close()

	dbName := "dbname"

	clickhouseStore, err := goose_clickhouse_store.NewStore(
		goose_clickhouse_store.DistributedMigrationsTableConfig{
			Cluster:     "default",
			Database:    dbName,
			TableName:   "migrations",
			ShardingKey: "rand()",
		},
		goose_clickhouse_store.LocalMigrationsTableConfig{
			ZooKeeperPath: fmt.Sprintf("/clickhouse/tables/{shard}/%s/migrations", dbName),
			ReplicaName:   "{replica}",
			Database:      dbName,
			TableName:     "migrations_local",
		})
	if err != nil {
		log.Fatal(err)
	}

	p, err := goose.NewProvider(
		"",
		conn,
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
