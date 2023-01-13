package goloquent

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"
)

type replica struct {
	secondary  map[string]*DB
	readonly   map[string]*DB
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

func transformList(listDB map[string]*DB) []*DB {
	outDB := make([]*DB, 0)
	for _, db := range listDB {
		outDB = append(outDB, db)
	}
	return outDB
}

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
			return transformList(r.secondary)[0]
		}

		resolverList = append(resolverList, transformList(r.secondary)...)

	case ReplicaResolveReadOnly:
		if len(r.readonly) == 0 {
			return nil
		}

		if len(r.readonly) == 1 {
			return transformList(r.readonly)[0]
		}
		resolverList = append(resolverList, transformList(r.readonly)...)

	case DefaultReplicaResolver:
		if op == operationRead {
			resolverList = append(resolverList, transformList(r.secondary)...)
			resolverList = append(resolverList, transformList(r.readonly)...)
			if len(resolverList) == 0 {
				resolverList = append(resolverList, nil)
			}
		}

		if op == operationWrite {
			resolverList = append(resolverList, nil)
			resolverList = append(resolverList, transformList(r.secondary)...)
		}

	}

	if len(resolverList) == 1 {
		return resolverList[0]
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

func (db *DB) ReplicaPingInterval(seconds int64) error {
	// only allow add replicas on primary connection
	if db.replica == nil {
		return fmt.Errorf("goloquent: unsupported replica action on replica db")
	}

	db.replicaPingInterval = seconds
	return nil
}

func (db *DB) Replica(ctx context.Context, driver string, conf ReplicaConfig) error {
	// only allow add replicas on primary connection
	if db.replica == nil {
		return fmt.Errorf("goloquent: unsupported replica action on replica db")
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

	go func() {
		for {
			pingContext, cancel := context.WithTimeout(context.TODO(), time.Second*time.Duration(db.replicaPingInterval))
			defer cancel()

			select {

			case <-pingContext.Done():
				err := conn.PingContext(ctx)
				if conf.ReadOnly {
					if err != nil {
						delete(db.replica.readonly, replicaDB.id)
					} else {
						db.replica.readonly[replicaDB.id] = replicaDB
					}
				} else {
					if err != nil {
						delete(db.replica.secondary, replicaDB.id)
					} else {
						db.replica.secondary[replicaDB.id] = replicaDB
					}
				}

			case <-ctx.Done():
				break

			}
		}
	}()

	return nil
}
