package goloquent

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"
)

type replica struct {
	secondary  []*DB
	readonly   []*DB
	roundRobin int64
}

type replicaResolver int

const (
	DefaultReplicaResolver replicaResolver = iota
	ReplicaResolvePrimaryOnly
	ReplicaResolveSecondaryOnly
	ReplicaResolveReadOnly
)

var (
	writeNext uint32
	readNext  uint32
)

func (r *replica) resolveDatabase(resolver replicaResolver, op dbOperation) *DB {

	resolverList := make([]*DB, 0)

	switch resolver {

	case ReplicaResolvePrimaryOnly:
		return nil

	case ReplicaResolveSecondaryOnly:
		if len(r.secondary) == 0 {
			return nil
		}

		if len(r.secondary) == 1 {
			return r.secondary[0]
		}

		resolverList = append(resolverList, r.secondary...)

	case ReplicaResolveReadOnly:
		if len(r.readonly) == 0 {
			return nil
		}

		if len(r.readonly) == 1 {
			return r.readonly[0]
		}
		resolverList = append(resolverList, r.readonly...)

	case DefaultReplicaResolver:
		if op == operationRead {
			resolverList = append(resolverList, r.secondary...)
			resolverList = append(resolverList, r.readonly...)
			if len(resolverList) == 0 {
				resolverList = append(resolverList, nil)
			}
		}

		if op == operationWrite {
			resolverList = append(resolverList, nil)
			resolverList = append(resolverList, r.secondary...)
		}

	}

	next := uint32(0)
	if op == operationRead {
		next = atomic.AddUint32(&readNext, 1)
	} else {
		next = atomic.AddUint32(&writeNext, 1)
	}

	return resolverList[int(next)%len(resolverList)]
}

type ReplicaConfig struct {
	Username   string
	Password   string
	Host       string
	Port       string
	Database   string
	UnixSocket string
	TLSConfig  string
	ReadOnly   bool
	CharSet    *CharSet
	Logger     LogHandler
	Native     NativeHandler
}

func (db *DB) Replica(ctx context.Context, driver string, conf ReplicaConfig) error {
	// only allow add replicas on primary connection
	if db.replica == nil {
		return fmt.Errorf("goloquent: unsupported replica on replica db")
	}

	driver = strings.TrimSpace(strings.ToLower(driver))
	dialect, ok := GetDialect(driver)
	if !ok {
		panic(fmt.Errorf("goloquent: unsupported database driver %q", driver))
	}

	config := Config{
		Username:   conf.Username,
		Password:   conf.Password,
		Host:       conf.Host,
		Port:       conf.Port,
		TLSConfig:  conf.TLSConfig,
		Database:   conf.Database,
		UnixSocket: conf.UnixSocket,
		CharSet:    conf.CharSet,
		Logger:     conf.Logger,
	}
	config.Normalize()

	conn, err := dialect.Open(config)
	if err != nil {
		return err
	}

	if conf.Native != nil {
		conf.Native(conn)
	}

	if err := conn.Ping(); err != nil {
		return fmt.Errorf("goloquent: %s server has not response", driver)
	}

	client := Client{
		driver:    driver,
		sqlCommon: conn,
		dialect:   dialect,
		CharSet:   *config.CharSet,
		logger:    config.Logger,
	}
	dialect.SetDB(client)

	replicaDB := &DB{
		id:      fmt.Sprintf("%s:%d", driver, time.Now().UnixNano()),
		driver:  driver,
		name:    dialect.CurrentDB(ctx),
		client:  client,
		dialect: dialect,
	}

	if conf.ReadOnly {
		db.replica.readonly = append(db.replica.readonly, replicaDB)
	} else {
		db.replica.secondary = append(db.replica.secondary, replicaDB)
	}
	return nil
}
