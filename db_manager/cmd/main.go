package main

import (
	"context"
	"encoding/json"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/arbhalerao/meerkat/db_manager/internal"
	grpc_server "github.com/arbhalerao/meerkat/db_manager/server/grpc"
	"github.com/arbhalerao/meerkat/utils"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Config struct {
	Manager struct {
		GRPC_Addr string `toml:"grpc_addr"`
		HTTP_Addr string `toml:"http_addr"`
	} `toml:"manager"`
}

type RegisterRequest struct {
	Region   string `json:"region"`
	GRPCAddr string `json:"grpc_addr"`
}

type ManagerServer struct {
	manager    *internal.DBManager
	httpServer *http.Server
}

func NewManagerServer(manager *internal.DBManager, httpAddr string) *ManagerServer {
	ms := &ManagerServer{
		manager: manager,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/register", ms.registerHandler)
	mux.HandleFunc("/health", ms.healthHandler)
	mux.HandleFunc("/cluster", ms.clusterHandler)
	mux.Handle("/metrics", promhttp.Handler())

	ms.httpServer = &http.Server{
		Addr:    httpAddr,
		Handler: mux,
	}

	return ms
}

func (ms *ManagerServer) registerHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req RegisterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		utils.Logger.Error().Err(err).Msg("Failed to decode registration request")
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	serverUUID := uuid.New().String()

	success := ms.manager.AddServer(serverUUID, req.Region, req.GRPCAddr)
	if !success {
		utils.Logger.Error().Msgf("Failed to add server %s", serverUUID)
		http.Error(w, "Failed to register server", http.StatusInternalServerError)
		return
	}

	utils.Logger.Info().Msgf("Successfully registered server %s from region %s at %s",
		serverUUID, req.Region, req.GRPCAddr)

	w.Header().Set("Content-Type", "application/json")
	response := map[string]interface{}{
		"success":     true,
		"server_uuid": serverUUID,
		"message":     "Server registered successfully",
	}
	_ = json.NewEncoder(w).Encode(response)
}

func (ms *ManagerServer) healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	response := map[string]interface{}{
		"status": "healthy",
		"time":   time.Now().Format(time.RFC3339),
	}
	_ = json.NewEncoder(w).Encode(response)
}

func (ms *ManagerServer) clusterHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	servers := ms.manager.GetClusterStatus()
	response := map[string]interface{}{
		"status":             "healthy",
		"server_count":       len(servers),
		"replication_factor": internal.ReplicationFactor,
		"servers":            servers,
		"time":               time.Now().Format(time.RFC3339),
	}
	_ = json.NewEncoder(w).Encode(response)
}

func (ms *ManagerServer) Start() error {
	utils.Logger.Info().Msgf("Starting HTTP server on %s", ms.httpServer.Addr)
	return ms.httpServer.ListenAndServe()
}

func (ms *ManagerServer) Stop() error {
	utils.Logger.Info().Msg("Shutting down HTTP server...")
	return ms.httpServer.Shutdown(context.Background())
}

func main() {
	configPath := flag.String("config", "config/manager.toml", "Path to the config file")
	flag.Parse()

	utils.NewLogger()
	utils.Logger.Info().Msg("meerkat manager starting...")

	var config Config
	err := utils.LoadTomlConfig(&config, *configPath)
	if err != nil {
		utils.Logger.Fatal().Err(err).Msg("Failed to load config file")
		return
	}

	grpcAddr := config.Manager.GRPC_Addr
	httpAddr := config.Manager.HTTP_Addr

	dbManager := internal.NewDBManager()

	grpcService := grpc_server.NewServer(grpcAddr, dbManager)

	httpService := NewManagerServer(dbManager, httpAddr)

	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			utils.Logger.Debug().Msg("Running health check on registered servers")
			dbManager.HealthCheckServers()
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		utils.Logger.Info().Msgf("Starting gRPC server on %s", grpcAddr)
		if err := grpcService.Start(); err != nil {
			utils.Logger.Fatal().Err(err).Msg("gRPC server failed")
		}
	}()

	go func() {
		defer wg.Done()
		if err := httpService.Start(); err != nil && err != http.ErrServerClosed {
			utils.Logger.Fatal().Err(err).Msg("HTTP server failed")
		}
	}()

	<-stop
	utils.Logger.Info().Msg("Shutting down servers...")

	grpcService.Stop()
	_ = httpService.Stop()

	wg.Wait()

	utils.Logger.Info().Msg("Servers stopped successfully.")
}
