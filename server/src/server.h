// ingestion_server.h
#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <grpcpp/grpcpp.h>
#include <librdkafka/rdkafkacpp.h>
#include "streaming.grpc.pb.h"

namespace ingest {

// Configuration for the Ingestion Server
struct ServerConfig {
    // Kafka cluster configuration
    std::string kafka_brokers;
    int producer_queue_size = 100000;
    bool enable_idempotence = true;
    std::string compression = "snappy";
    
    // Server configuration
    int num_threads = 8;
    std::string server_address = "0.0.0.0:50051";
    bool tls_enabled = false;
    std::string cert_path;
    std::string key_path;
    
    // Performance settings
    int batch_size = 10000;
    int linger_ms = 5;
    int retry_backoff_ms = 100;
    int message_timeout_ms = 30000;
};

// Class to manage Kafka producers
class KafkaManager {
public:
    explicit KafkaManager(const ServerConfig& config);
    ~KafkaManager();
    
    // Send a single message to Kafka
    bool SendMessage(const std::string& topic, 
                     const std::string& key,
                     const std::string& payload);
    
    // Send a batch of messages to Kafka
    std::vector<bool> SendBatch(const std::string& topic,
                               const std::vector<std::string>& keys,
                               const std::vector<std::string>& payloads);
    
    // Get producer stats
    std::string GetStats(const std::string& topic);
    
private:
    // Get or create a producer for the given topic
    std::shared_ptr<RdKafka::Producer> GetProducer(const std::string& topic);
    
    ServerConfig config_;
    std::unordered_map<std::string, std::shared_ptr<RdKafka::Producer>> producers_;
    std::mutex producers_mutex_;
};

// gRPC service implementation
class IngestionServiceImpl final : public streaming::IngestionService::Service {
public:
    explicit IngestionServiceImpl(std::shared_ptr<KafkaManager> kafka_manager);
    
    grpc::Status IngestMessage(grpc::ServerContext* context,
                              const streaming::IngestionRequest* request,
                              streaming::IngestionResponse* response) override;
    
    grpc::Status IngestBatch(grpc::ServerContext* context,
                            const streaming::BatchIngestionRequest* request,
                            streaming::BatchIngestionResponse* response) override;
private:
    std::shared_ptr<KafkaManager> kafka_manager_;
};

// Main server class
class IngestionServer {
public:
    explicit IngestionServer(const ServerConfig& config);
    
    // Start the server (blocking call)
    void Start();
    
    // Stop the server
    void Stop();
    
private:
    ServerConfig config_;
    std::unique_ptr<grpc::Server> server_;
    std::shared_ptr<KafkaManager> kafka_manager_;
};

}  // namespace ingest
