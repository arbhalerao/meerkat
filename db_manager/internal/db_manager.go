package internal

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/arbhalerao/meerkat/pb/db_server"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type dbServer struct {
	uuid   string
	region string
	addr   string
	conn   *grpc.ClientConn
	client db_server.DBServerClient
}

const ReplicationFactor = 2

type DBManager struct {
	mu      sync.Mutex
	servers map[string]dbServer
	hasher  *ConsistentHasher
}

func NewDBManager() *DBManager {
	return &DBManager{
		servers: make(map[string]dbServer),
		hasher:  NewConsistentHasher(),
	}
}

func (m *DBManager) AddServer(uuid, region, addr string) bool {
	m.mu.Lock()

	if _, exists := m.servers[uuid]; exists {
		m.mu.Unlock()
		return false
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		m.mu.Unlock()
		return false
	}

	client := db_server.NewDBServerClient(conn)

	existingServers := make([]dbServer, 0, len(m.servers))
	for _, s := range m.servers {
		existingServers = append(existingServers, s)
	}

	m.servers[uuid] = dbServer{
		uuid:   uuid,
		region: region,
		addr:   addr,
		conn:   conn,
		client: client,
	}

	m.hasher.AddNode(uuid)
	ActiveServers.Inc()
	m.mu.Unlock()

	if len(existingServers) > 0 {
		go m.migrateKeysOnNodeAdd(uuid, existingServers)
	}

	return true
}

func (m *DBManager) RemoveServer(uuid string) bool {
	m.mu.Lock()
	server, exists := m.servers[uuid]
	if !exists {
		m.mu.Unlock()
		return false
	}
	m.mu.Unlock()

	m.migrateKeysOnNodeRemove(uuid, server)

	m.mu.Lock()
	if server.conn != nil {
		server.conn.Close()
	}
	delete(m.servers, uuid)
	m.hasher.RemoveNode(uuid)
	ActiveServers.Dec()
	m.mu.Unlock()

	return true
}

func (m *DBManager) HealthCheckServers() {
	m.mu.Lock()
	servers := make(map[string]dbServer, len(m.servers))
	for k, v := range m.servers {
		servers[k] = v
	}
	m.mu.Unlock()

	var toRemove []string

	for uuid, server := range servers {
		_, err := server.client.HealthCheck(context.Background(), &db_server.HealthCheckRequest{})
		if err != nil {
			toRemove = append(toRemove, uuid)
		}
	}

	for _, uuid := range toRemove {
		m.mu.Lock()
		server, exists := m.servers[uuid]
		if !exists {
			m.mu.Unlock()
			continue
		}
		m.mu.Unlock()

		m.migrateKeysOnNodeRemove(uuid, server)

		m.mu.Lock()
		if s, ok := m.servers[uuid]; ok {
			if s.conn != nil {
				s.conn.Close()
			}
			delete(m.servers, uuid)
		}
		m.mu.Unlock()
	}

	m.ReconcileServers()
}

func (m *DBManager) getReplicaServers(key string) ([]dbServer, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	uuids := m.hasher.GetReplicaNodes(key, ReplicationFactor)
	if len(uuids) == 0 {
		return nil, fmt.Errorf("no available database servers")
	}

	var servers []dbServer
	for _, uuid := range uuids {
		if server, exists := m.servers[uuid]; exists {
			servers = append(servers, server)
		}
	}

	if len(servers) == 0 {
		return nil, fmt.Errorf("no reachable servers for key %q", key)
	}

	return servers, nil
}

func (m *DBManager) GetKey(key string) (string, error) {
	start := time.Now()
	defer func() {
		RequestDuration.WithLabelValues("get").Observe(time.Since(start).Seconds())
	}()

	servers, err := m.getReplicaServers(key)
	if err != nil {
		RequestsTotal.WithLabelValues("get", "error").Inc()
		return "", err
	}

	var lastErr error
	for _, server := range servers {
		resp, err := server.client.Get(context.Background(), &db_server.GetRequest{Key: key})
		if err == nil {
			RequestsTotal.WithLabelValues("get", "success").Inc()
			return resp.Value, nil
		}
		lastErr = err
	}

	RequestsTotal.WithLabelValues("get", "error").Inc()
	return "", fmt.Errorf("all replicas failed for key %q: %v", key, lastErr)
}

func (m *DBManager) SetKey(key, value string) (bool, error) {
	start := time.Now()
	defer func() {
		RequestDuration.WithLabelValues("set").Observe(time.Since(start).Seconds())
	}()

	servers, err := m.getReplicaServers(key)
	if err != nil {
		RequestsTotal.WithLabelValues("set", "error").Inc()
		return false, err
	}

	successCount := 0
	var lastErr error
	for _, server := range servers {
		_, err := server.client.Set(context.Background(), &db_server.SetRequest{Key: key, Value: value})
		if err != nil {
			lastErr = err
			ReplicationWrites.WithLabelValues("failure").Inc()
			continue
		}
		successCount++
		ReplicationWrites.WithLabelValues("success").Inc()
	}

	if successCount == 0 {
		RequestsTotal.WithLabelValues("set", "error").Inc()
		return false, fmt.Errorf("failed to write to any replica for key %q: %v", key, lastErr)
	}

	RequestsTotal.WithLabelValues("set", "success").Inc()
	return true, nil
}

func (m *DBManager) DeleteKey(key string) (bool, error) {
	start := time.Now()
	defer func() {
		RequestDuration.WithLabelValues("delete").Observe(time.Since(start).Seconds())
	}()

	servers, err := m.getReplicaServers(key)
	if err != nil {
		RequestsTotal.WithLabelValues("delete", "error").Inc()
		return false, err
	}

	successCount := 0
	var lastErr error
	for _, server := range servers {
		_, err := server.client.Delete(context.Background(), &db_server.DeleteRequest{Key: key})
		if err != nil {
			lastErr = err
			continue
		}
		successCount++
	}

	if successCount == 0 {
		RequestsTotal.WithLabelValues("delete", "error").Inc()
		return false, fmt.Errorf("failed to delete from any replica for key %q: %v", key, lastErr)
	}

	RequestsTotal.WithLabelValues("delete", "success").Inc()
	return true, nil
}

type ServerInfo struct {
	UUID   string `json:"uuid"`
	Region string `json:"region"`
	Addr   string `json:"addr"`
}

func (m *DBManager) GetClusterStatus() []ServerInfo {
	m.mu.Lock()
	defer m.mu.Unlock()

	servers := make([]ServerInfo, 0, len(m.servers))
	for _, s := range m.servers {
		servers = append(servers, ServerInfo{
			UUID:   s.uuid,
			Region: s.region,
			Addr:   s.addr,
		})
	}
	return servers
}

func (m *DBManager) ServerCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.servers)
}

func (m *DBManager) ReconcileServers() {
	m.mu.Lock()
	activeNodes := make([]string, 0, len(m.servers))
	for uuid := range m.servers {
		activeNodes = append(activeNodes, uuid)
	}
	m.mu.Unlock()

	m.hasher.Reconcile(activeNodes)
}

func (m *DBManager) migrateKeysOnNodeAdd(newUUID string, existingServers []dbServer) {
	log.Info().Msgf("Starting key migration for new node %s", newUUID)
	migrated := 0

	for _, oldServer := range existingServers {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		resp, err := oldServer.client.ListKeys(ctx, &db_server.ListKeysRequest{})
		cancel()
		if err != nil {
			log.Warn().Err(err).Msgf("Failed to list keys on server %s during migration", oldServer.uuid)
			continue
		}

		for _, pair := range resp.Pairs {
			primaryNode, ok := m.hasher.GetNode(pair.Key)
			if !ok || primaryNode != newUUID {
				replicas := m.hasher.GetReplicaNodes(pair.Key, ReplicationFactor)
				isReplica := false
				for _, r := range replicas {
					if r == newUUID {
						isReplica = true
						break
					}
				}
				if !isReplica {
					continue
				}
			}

			m.mu.Lock()
			newServer, exists := m.servers[newUUID]
			m.mu.Unlock()
			if !exists {
				log.Error().Msgf("New server %s disappeared during migration", newUUID)
				return
			}

			ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
			_, err := newServer.client.Set(ctx2, &db_server.SetRequest{Key: pair.Key, Value: pair.Value})
			cancel2()
			if err != nil {
				log.Warn().Err(err).Msgf("Failed to migrate key %q to new node %s", pair.Key, newUUID)
				continue
			}

			replicas := m.hasher.GetReplicaNodes(pair.Key, ReplicationFactor)
			oldIsReplica := false
			for _, r := range replicas {
				if r == oldServer.uuid {
					oldIsReplica = true
					break
				}
			}
			if !oldIsReplica {
				ctx3, cancel3 := context.WithTimeout(context.Background(), 5*time.Second)
				_, _ = oldServer.client.Delete(ctx3, &db_server.DeleteRequest{Key: pair.Key})
				cancel3()
			}

			migrated++
		}
	}

	KeysMigrated.WithLabelValues("node_add").Add(float64(migrated))
	log.Info().Msgf("Key migration for new node %s completed: %d keys migrated", newUUID, migrated)
}

func (m *DBManager) migrateKeysOnNodeRemove(uuid string, server dbServer) {
	log.Info().Msgf("Draining keys from node %s before removal", uuid)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	resp, err := server.client.ListKeys(ctx, &db_server.ListKeysRequest{})
	cancel()
	if err != nil {
		log.Warn().Err(err).Msgf("Failed to list keys on dying server %s — data may be lost (replicas may still have copies)", uuid)
		return
	}

	if len(resp.Pairs) == 0 {
		log.Info().Msgf("No keys to drain from node %s", uuid)
		return
	}

	migrated := 0
	for _, pair := range resp.Pairs {
		replicas := m.hasher.GetReplicaNodes(pair.Key, ReplicationFactor+1)

		for _, replicaUUID := range replicas {
			if replicaUUID == uuid {
				continue // skip the dying server
			}

			m.mu.Lock()
			target, exists := m.servers[replicaUUID]
			m.mu.Unlock()
			if !exists {
				continue
			}

			ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
			_, err := target.client.Set(ctx2, &db_server.SetRequest{Key: pair.Key, Value: pair.Value})
			cancel2()
			if err != nil {
				log.Warn().Err(err).Msgf("Failed to migrate key %q to server %s", pair.Key, replicaUUID)
				continue
			}
		}
		migrated++
	}

	KeysMigrated.WithLabelValues("node_remove").Add(float64(migrated))
	log.Info().Msgf("Drained %d keys from node %s", migrated, uuid)
}
