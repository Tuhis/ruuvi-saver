package config

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
	"go.uber.org/zap"
)

type AuthMechanism string

const (
	AuthMechanismPlain AuthMechanism = "PLAIN"
	AuthMechanismScram AuthMechanism = "SCRAM-SHA-512"
)

type envConfig struct {
	Brokers                string
	StatusTopic            string
	GatewayRepositoryTopic string
	TenantConfigTopic      string
	OwnName                string
	AuthMechanism          AuthMechanism
	Username               string
	Password               string
}

func ReadGatewaysFromKafka(logger *zap.SugaredLogger) (map[string]Gateway, error) {
	config, err := configFromEnvironment()
	if err != nil {
		return nil, fmt.Errorf("failed to read configuration from environment: %v", err)
	}

	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
	}

	if config.AuthMechanism == AuthMechanismScram {
		mechanism, err := scram.Mechanism(scram.SHA512, config.Username, config.Password)
		if err != nil {
			panic(err)
		}

		dialer.SASLMechanism = mechanism
	}

	// Get list of partitions for the topic
	dialCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	partitions, err := dialer.LookupPartitions(dialCtx, "tcp", config.Brokers, config.GatewayRepositoryTopic)
	if err != nil {
		return nil, fmt.Errorf("failed to lookup partitions: %v", err)
	}

	// If there are no partitions, return an error
	if len(partitions) == 0 {
		return nil, fmt.Errorf("no partitions found")
	}

	// Create a reader for each partition
	readers := make([]*kafka.Reader, len(partitions))
	for i, partition := range partitions {
		readers[i] = kafka.NewReader(kafka.ReaderConfig{
			Brokers:     []string{config.Brokers},
			Topic:       config.GatewayRepositoryTopic,
			Partition:   partition.ID,
			MaxWait:     1 * time.Second,
			StartOffset: kafka.FirstOffset, // Start from the beginning
		})
	}

	// Create a channel to receive messages
	msgChan := make(chan GatewayKafkaMessage, 10)

	// Start a goroutine for each reader
	for _, reader := range readers {
		wg.Add(1)
		go readGatewayRepositoryTopicMessages(reader, msgChan, logger)
	}

	// Read messages from the channel and update the gateways map
	gateways := map[string]Gateway{}
	go updateGateways(gateways, msgChan)

	return gateways, nil
}

// TODO: Refactor this (and above) function so that copy-pasta is reduced
func ReadInfluxDBConfigsFromKafka(logger *zap.SugaredLogger) (map[string]InfluxDBConfig, error) {
	config, err := configFromEnvironment()
	if err != nil {
		return nil, fmt.Errorf("failed to read configuration from environment: %v", err)
	}

	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
	}

	if config.AuthMechanism == AuthMechanismScram {
		mechanism, err := scram.Mechanism(scram.SHA512, config.Username, config.Password)
		if err != nil {
			panic(err)
		}

		dialer.SASLMechanism = mechanism
	}

	// Get list of partitions for the topic
	dialCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	partitions, err := dialer.LookupPartitions(dialCtx, "tcp", config.Brokers, config.TenantConfigTopic)
	if err != nil {
		return nil, fmt.Errorf("failed to lookup partitions: %v", err)
	}

	// If there are no partitions, return an error
	if len(partitions) == 0 {
		return nil, fmt.Errorf("no partitions found")
	}

	// Create a reader for each partition
	readers := make([]*kafka.Reader, len(partitions))
	for i, partition := range partitions {
		readers[i] = kafka.NewReader(kafka.ReaderConfig{
			Brokers:     []string{config.Brokers},
			Topic:       config.TenantConfigTopic,
			Partition:   partition.ID,
			MaxWait:     1 * time.Second,
			StartOffset: kafka.FirstOffset, // Start from the beginning
		})
	}

	// Create a channel to receive messages
	msgChan := make(chan InfluxDBConfigKafkaMessage, 10)

	// Start a goroutine for each reader
	for _, reader := range readers {
		wg.Add(1)
		go readTenantConfigTopicMessages(reader, msgChan, logger)
	}

	// Read messages from the channel and update the configs map
	influxDBConfigs := map[string]InfluxDBConfig{}
	go updateInfluxDBConfigs(influxDBConfigs, msgChan)

	return influxDBConfigs, nil
}

func configFromEnvironment() (*envConfig, error) {
	brokers := os.Getenv("KAFKA_BROKERS")
	statusTopic := os.Getenv("KAFKA_STATUS_TOPIC")
	gatewayRepositoryTopic := os.Getenv("KAFKA_GATEWAY_REPOSITORY_TOPIC")
	TenantConfigTopic := os.Getenv("KAFKA_TENANT_CONFIG_TOPIC")
	ownName := os.Getenv("OWN_NAME")
	authMechanismStr := os.Getenv("KAFKA_AUTH_MECHANISM")
	username := os.Getenv("KAFKA_USERNAME")
	password := os.Getenv("KAFKA_PASSWORD")

	if brokers == "" || statusTopic == "" || ownName == "" || gatewayRepositoryTopic == "" || TenantConfigTopic == "" {
		return nil, errors.New("KAFKA_BROKERS, KAFKA_STATUS_TOPIC, KAFKA_GATEWAY_REPOSITORY_TOPIC, KAFKA_TENANT_CONFIG_TOPIC and OWN_NAME must be set")
	}

	authMechanism, err := parseAuthMechanism(authMechanismStr)
	if err != nil {
		return nil, err
	}

	if authMechanism == AuthMechanismScram {
		if username == "" || password == "" {
			return nil, errors.New("KAFKA_USERNAME and KAFKA_PASSWORD must be set for SCRAM authentication")
		}
	}

	return &envConfig{
		Brokers:                brokers,
		StatusTopic:            statusTopic,
		GatewayRepositoryTopic: gatewayRepositoryTopic,
		TenantConfigTopic:      TenantConfigTopic,
		OwnName:                ownName,
		AuthMechanism:          authMechanism,
		Username:               username,
		Password:               password,
	}, nil
}

func parseAuthMechanism(s string) (AuthMechanism, error) {
	switch s {
	case string(AuthMechanismPlain), "":
		return AuthMechanismPlain, nil
	case string(AuthMechanismScram):
		return AuthMechanismScram, nil
	default:
		return "", errors.New("invalid auth mechanism: " + s)
	}
}

func readGatewayRepositoryTopicMessages(reader *kafka.Reader, msgChan chan<- GatewayKafkaMessage, logger *zap.SugaredLogger) {
	for {
		select {
		case <-readerCtx.Done():
			logger.Info("Received quit signal, closing reader")
			reader.Close()
			wg.Done()
			return

		default:
			readCtx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			msg, err := reader.ReadMessage(readCtx)
			cancel()
			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					continue
				}

				panic(err)
			}

			// Unmarshal the message, log it and send it to the channel
			var gatewayMsg GatewayKafkaMessage
			if err := json.Unmarshal(msg.Value, &gatewayMsg); err != nil {
				logger.Errorf("Failed to unmarshal message: %v", err)
				continue
			}

			logger.Debugf("Received message: %v", gatewayMsg)
			msgChan <- gatewayMsg
		}
	}
}

func readTenantConfigTopicMessages(reader *kafka.Reader, msgChan chan<- InfluxDBConfigKafkaMessage, logger *zap.SugaredLogger) {
	for {
		select {
		case <-readerCtx.Done():
			logger.Info("Received quit signal, closing reader")
			reader.Close()
			wg.Done()
			return

		default:
			readCtx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			msg, err := reader.ReadMessage(readCtx)
			cancel()
			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					continue
				}

				panic(err)
			}

			// Unmarshal the message, log it and send it to the channel
			var tenantConfigMsg InfluxDBConfigKafkaMessage
			if err := json.Unmarshal(msg.Value, &tenantConfigMsg); err != nil {
				logger.Errorf("Failed to unmarshal message: %v", err)
				continue
			}

			logger.Debugf("Received message: %v", tenantConfigMsg)
			msgChan <- tenantConfigMsg
		}
	}
}

// updateGateways updates the gateways map with messages coming from the channel
func updateGateways(gateways map[string]Gateway, msgChan <-chan GatewayKafkaMessage) {
	for msg := range msgChan {
		gwLock.Lock()
		// Check if the gateway is already in the map and update it if OwnerSince is newer. Otherwise add it to the map.
		gw, ok := gateways[msg.ID]
		if ok {
			if msg.OwnerSince > gw.OwnerSince {
				gateways[msg.ID] = Gateway{
					ID:         msg.ID,
					Owner:      msg.Owner,
					OwnerSince: msg.OwnerSince,
				}
			}
		} else {
			gateways[msg.ID] = Gateway{
				ID:         msg.ID,
				Owner:      msg.Owner,
				OwnerSince: msg.OwnerSince,
			}
		}
		gwLock.Unlock()
	}
}

// updateInfluxDBConfigs updates the influxDbConfigs map with messages coming from the channel
func updateInfluxDBConfigs(influxDbConfigs map[string]InfluxDBConfig, msgChan <-chan InfluxDBConfigKafkaMessage) {
	for msg := range msgChan {
		influxDbLock.Lock()
		// Check if the config is already in the map and update it if UpdatedAt is newer. Otherwise add it to the map.
		config, ok := influxDbConfigs[msg.TenantID]
		if ok {
			if msg.UpdatedAt > config.UpdatedAt {
				influxDbConfigs[msg.TenantID] = InfluxDBConfig{
					Host:        msg.Host,
					AccessToken: msg.AccessToken,
					Org:         msg.Org,
					Bucket:      msg.Bucket,
					TenantID:    msg.TenantID,
					UpdatedAt:   msg.UpdatedAt,
				}
			}
		} else {
			influxDbConfigs[msg.TenantID] = InfluxDBConfig{
				Host:        msg.Host,
				AccessToken: msg.AccessToken,
				Org:         msg.Org,
				Bucket:      msg.Bucket,
				TenantID:    msg.TenantID,
				UpdatedAt:   msg.UpdatedAt,
			}
		}
		influxDbLock.Unlock()
	}
}
