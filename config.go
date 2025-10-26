package goose_clickhouse_store

var DistributedMigrationsTableConfigByDefault = DistributedMigrationsTableConfig{
	Cluster:     "default",
	ShardingKey: "rand()",
}

var LocalMigrationsTableConfigByDefault = LocalMigrationsTableConfig{
	ReplicaName: "{replica}",
}

type DistributedMigrationsTableConfig struct {
	Cluster     string
	Database    string
	TableName   string
	ShardingKey string
}

type LocalMigrationsTableConfig struct {
	ZooKeeperPath string
	ReplicaName   string
	Database      string
	TableName     string
}
