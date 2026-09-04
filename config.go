package goose_clickhouse_store

var DistributedMigrationsTableConfigByDefault = DistributedMigrationsTableConfig{
	Cluster:       "default",
	ShardingKey:   "rand()",
	MutationsSync: 2,
}

var LocalMigrationsTableConfigByDefault = LocalMigrationsTableConfig{
	ReplicaName: "{replica}",
}

type DistributedMigrationsTableConfig struct {
	Cluster     string
	Database    string
	TableName   string
	ShardingKey string
	// MutationsSync controls the mutations_sync setting applied to the ALTER TABLE ... DELETE
	// statement in Delete: 0 - async, 1 - wait for the current server, 2 - wait for all replicas.
	MutationsSync uint8
}

type LocalMigrationsTableConfig struct {
	ZooKeeperPath string
	ReplicaName   string
	Database      string
	TableName     string
}
