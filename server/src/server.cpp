// ingestion_server.cc
#include "ingestion_server.h"

#include <iostream>
#include <thread>

namespace ingest
{

    //
    // KafkaManager implementation
    //
    KafkaManager::KafkaManager(const ServerConfig &config) : config_(config)
    {
        // Global initialization if needed
        std::string errstr;
        RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

        // Set default configuration
        conf->set("bootstrap.servers", config.kafka_brokers, errstr);
        conf->set("queue.buffering.max.messages", std::to_string(config.producer_queue_size), errstr);
        conf->set("enable.idempotence", config.enable_idempotence ? "true" : "false", errstr);
        conf->set("compression.type", config.compression, errstr);
        conf->set("batch.size", std::to_string(config.batch_size), errstr);
        conf->set("linger.ms", std::to_string(config.linger_ms), errstr);
        conf->set("retry.backoff.ms", std::to_string(config.retry_backoff_ms), errstr);
        conf->set("message.timeout.ms", std::to_string(config.message_timeout_ms), errstr);

        delete conf;
    }

    KafkaManager::~KafkaManager()
    {
        // Clean up producers
        producers_.clear();
    }

    std::shared_ptr<RdKafka::Producer> KafkaManager::GetProducer(const std::string &topic)
    {
        std::lock_guard<std::mutex> lock(producers_mutex_);

        // Create producer if it doesn't exist for this topic
        auto it = producers_.find(topic);
        if (it == producers_.end())
        {
            std::string errstr;
            RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

            // Set producer configuration
            conf->set("bootstrap.servers", config_.kafka_brokers, errstr);
            conf->set("queue.buffering.max.messages", std::to_string(config_.producer_queue_size), errstr);
            conf->set("enable.idempotence", config_.enable_idempotence ? "true" : "false", errstr);
            conf->set("compression.type", config_.compression, errstr);
            conf->set("batch.size", std::to_string(config_.batch_size), errstr);
            conf->set("linger.ms", std::to_string(config_.linger_ms), errstr);

            // Create producer
            RdKafka::Producer *producer = RdKafka::Producer::create(conf, errstr);
            if (!producer)
            {
                std::cerr << "Failed to create producer: " << errstr << std::endl;
                delete conf;
                return nullptr;
            }

            // Store in map and return
            auto shared_producer = std::shared_ptr<RdKafka::Producer>(producer);
            producers_[topic] = shared_producer;
            delete conf;
            return shared_producer;
        }

        return it->second;
    }

    bool KafkaManager::SendMessage(const std::string &topic,
                                   const std::string &key,
                                   const std::string &payload)
    {
        auto producer = GetProducer(topic);
        if (!producer)
        {
            return false;
        }

        // Create a delivery report callback
        bool delivered = false;
        class DeliveryReportCb : public RdKafka::DeliveryReportCb
        {
        public:
            explicit DeliveryReportCb(bool *delivered) : delivered_(delivered) {}

            void dr_cb(RdKafka::Message &message) override
            {
                if (message.err() == RdKafka::ERR_NO_ERROR)
                {
                    *delivered_ = true;
                }
                else
                {
                    std::cerr << "Message delivery failed: "
                              << message.errstr() << std::endl;
                }
            }

        private:
            bool *delivered_;
        };

        DeliveryReportCb dr_cb(&delivered);
        producer->set_dr_cb(&dr_cb);

        // Produce message
        RdKafka::ErrorCode err = producer->produce(
            topic,
            RdKafka::Topic::PARTITION_UA,   // Use default partitioner
            RdKafka::Producer::RK_MSG_COPY, // Copy payload
            const_cast<char *>(payload.c_str()),
            payload.size(),
            key.empty() ? nullptr : key.c_str(),
            key.size(),
            0,      // Timestamp (use current time)
            nullptr // Message opaque
        );

        if (err != RdKafka::ERR_NO_ERROR)
        {
            std::cerr << "Failed to produce message: "
                      << RdKafka::err2str(err) << std::endl;
            return false;
        }

        // Poll for delivery callback
        producer->poll(0);

        // Flush to ensure delivery
        producer->flush(500);

        return delivered;
    }

    std::vector<bool> KafkaManager::SendBatch(const std::string &topic,
                                              const std::vector<std::string> &keys,
                                              const std::vector<std::string> &payloads)
    {
        if (keys.size() != payloads.size())
        {
            return std::vector<bool>(payloads.size(), false);
        }

        auto producer = GetProducer(topic);
        if (!producer)
        {
            return std::vector<bool>(payloads.size(), false);
        }

        std::vector<bool> results(payloads.size(), false);

        // Send messages in batch
        for (size_t i = 0; i < payloads.size(); i++)
        {
            RdKafka::ErrorCode err = producer->produce(
                topic,
                RdKafka::Topic::PARTITION_UA,
                RdKafka::Producer::RK_MSG_COPY,
                const_cast<char *>(payloads[i].c_str()),
                payloads[i].size(),
                keys[i].empty() ? nullptr : keys[i].c_str(),
                keys[i].size(),
                0,
                nullptr);

            if (err == RdKafka::ERR_NO_ERROR)
            {
                results[i] = true;
            }
            else
            {
                std::cerr << "Failed to produce message " << i << ": "
                          << RdKafka::err2str(err) << std::endl;
            }

            // Occasionally poll to handle callbacks
            if (i % 1000 == 0)
            {
                producer->poll(0);
            }
        }

        // Final poll and flush
        producer->poll(0);
        producer->flush(1000);

        return results;
    }

    std::string KafkaManager::GetStats(const std::string &topic)
    {
        auto producer = GetProducer(topic);
        if (!producer)
        {
            return "{}";
        }

        return producer->name(); // Would be producer->stats() in a real implementation
    }

    //
    // IngestionServiceImpl implementation
    //
    IngestionServiceImpl::IngestionServiceImpl(std::shared_ptr<KafkaManager> kafka_manager)
        : kafka_manager_(std::move(kafka_manager)) {}

    grpc::Status IngestionServiceImpl::IngestMessage(
        grpc::ServerContext *context,
        const streaming::IngestionRequest *request,
        streaming::IngestionResponse *response)
    {

        // Extract topic, key, and payload
        const std::string &topic = request->topic();
        const std::string &key = request->key();
        const std::string &payload = request->payload();

        // Log client ID for monitoring
        const std::string &client_id = request->client_id();

        // Send to Kafka
        bool success = kafka_manager_->SendMessage(topic, key, payload);

        // Set response
        response->set_success(success);

        return grpc::Status::OK;
    }

    grpc::Status IngestionServiceImpl::IngestBatch(
        grpc::ServerContext *context,
        const streaming::BatchIngestionRequest *request,
        streaming::BatchIngestionResponse *response)
    {

        // Extract topic and entries
        const std::string &topic = request->topic();

        // Prepare batch
        std::vector<std::string> keys;
        std::vector<std::string> payloads;

        for (const auto &entry : request->entries())
        {
            keys.push_back(entry.key());
            payloads.push_back(entry.payload());
        }

        // Send batch to Kafka
        std::vector<bool> results = kafka_manager_->SendBatch(topic, keys, payloads);

        // Set response
        for (bool result : results)
        {
            response->add_success_statuses(result);
        }

        return grpc::Status::OK;
    }

    //
    // IngestionServer implementation
    //
    IngestionServer::IngestionServer(const ServerConfig &config)
        : config_(config), kafka_manager_(std::make_shared<KafkaManager>(config)) {}

    void IngestionServer::Start()
    {
        grpc::ServerBuilder builder;

        // Set up security if enabled
        if (config_.tls_enabled)
        {
            grpc::SslServerCredentialsOptions ssl_opts;
            grpc::SslServerCredentialsOptions::PemKeyCertPair pkcp;

            // Read key and cert from files
            std::ifstream cert_file(config_.cert_path);
            std::ifstream key_file(config_.key_path);

            if (!cert_file.is_open() || !key_file.is_open())
            {
                std::cerr << "Failed to open TLS certificate files" << std::endl;
                return;
            }

            std::stringstream cert_stream, key_stream;
            cert_stream << cert_file.rdbuf();
            key_stream << key_file.rdbuf();

            pkcp.cert_chain = cert_stream.str();
            pkcp.private_key = key_stream.str();
            ssl_opts.pem_key_cert_pairs.push_back(pkcp);

            builder.AddListeningPort(config_.server_address,
                                     grpc::SslServerCredentials(ssl_opts));
        }
        else
        {
            builder.AddListeningPort(config_.server_address,
                                     grpc::InsecureServerCredentials());
        }

        // Create and register service
        IngestionServiceImpl service(kafka_manager_);
        builder.RegisterService(&service);

        // Set thread pool size
        builder.SetSyncServerOption(grpc::ServerBuilder::NUM_CQS, config_.num_threads);
        builder.SetSyncServerOption(grpc::ServerBuilder::MIN_POLLERS, config_.num_threads);
        builder.SetSyncServerOption(grpc::ServerBuilder::MAX_POLLERS, config_.num_threads);

        // Start server
        server_ = builder.BuildAndStart();
        std::cout << "Server listening on " << config_.server_address << std::endl;

        // Wait for server to finish
        server_->Wait();
    }

    void IngestionServer::Stop()
    {
        std::cout << "Shutting down server..." << std::endl;
        server_->Shutdown();
    }

} // namespace ingest
