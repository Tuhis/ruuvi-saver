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

type KafkaConfig struct {
	Brokers                string
	StatusTopic            string
	GatewayRepositoryTopic string
	TenantConfigTopic      string
	RuuviEventTopic        string
	DeviceLocationTopic    string
	OwnName                string
	AuthMechanism          AuthMechanism
	Username               string
	Password               string
}

func ReadGatewaysFromKafka(logger *zap.SugaredLogger) (map[string]Gateway, error) {
	config, err := KafkaConfigFromEnvironment()
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
	config, err := KafkaConfigFromEnvironment()
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

func ReadDeviceLocationsFromKafka(logger *zap.SugaredLogger) (map[TenantId]map[DeviceId][]Location, error) {
	config, err := KafkaConfigFromEnvironment()
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
	partitions, err := dialer.LookupPartitions(dialCtx, "tcp", config.Brokers, config.RuuviEventTopic)
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
			Topic:       config.DeviceLocationTopic,
			Partition:   partition.ID,
			MaxWait:     1 * time.Second,
			StartOffset: kafka.FirstOffset, // Start from the beginning
		})
	}

	// Create a channel to receive messages
	msgChan := make(chan DeviceLocationKafkaMessage, 10)

	// Start a goroutine for each reader
	for _, reader := range readers {
		wg.Add(1)
		go readDeviceLocationTopicMessages(reader, msgChan, logger)
	}

	// Read messages from the channel and update the deviceLocations map
	deviceLocations := map[TenantId]map[DeviceId][]Location{}
	go updateDeviceLocations(deviceLocations, msgChan)

	return deviceLocations, nil
}

func KafkaConfigFromEnvironment() (*KafkaConfig, error) {
	brokers := os.Getenv("KAFKA_BROKERS")
	statusTopic := os.Getenv("KAFKA_STATUS_TOPIC")
	gatewayRepositoryTopic := os.Getenv("KAFKA_GATEWAY_REPOSITORY_TOPIC")
	TenantConfigTopic := os.Getenv("KAFKA_TENANT_CONFIG_TOPIC")
	RuuviEventTopic := os.Getenv("KAFKA_RUUVI_EVENT_TOPIC")
	DeviceLocationTopic := os.Getenv("KAFKA_DEVICE_LOCATION_TOPIC")
	ownName := os.Getenv("OWN_NAME")
	authMechanismStr := os.Getenv("KAFKA_AUTH_MECHANISM")
	username := os.Getenv("KAFKA_USERNAME")
	password := os.Getenv("KAFKA_PASSWORD")

	if brokers == "" || statusTopic == "" || ownName == "" || gatewayRepositoryTopic == "" || TenantConfigTopic == "" || RuuviEventTopic == "" || DeviceLocationTopic == "" {
		return nil, errors.New("KAFKA_BROKERS, KAFKA_STATUS_TOPIC, KAFKA_GATEWAY_REPOSITORY_TOPIC, KAFKA_TENANT_CONFIG_TOPIC, KAFKA_RUUVI_EVENT_TOPIC, KAFKA_DEVICE_LOCATION_TOPIC and OWN_NAME must be set")
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

	return &KafkaConfig{
		Brokers:                brokers,
		StatusTopic:            statusTopic,
		GatewayRepositoryTopic: gatewayRepositoryTopic,
		TenantConfigTopic:      TenantConfigTopic,
		RuuviEventTopic:        RuuviEventTopic,
		DeviceLocationTopic:    DeviceLocationTopic,
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

func readDeviceLocationTopicMessages(reader *kafka.Reader, msgChan chan<- DeviceLocationKafkaMessage, logger *zap.SugaredLogger) {
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
			var deviceLocationMsg DeviceLocationKafkaMessage
			if err := json.Unmarshal(msg.Value, &deviceLocationMsg); err != nil {
				logger.Errorf("Failed to unmarshal message: %v", err)
				continue
			}

			logger.Debugf("Received message: %v", deviceLocationMsg)
			msgChan <- deviceLocationMsg
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
					Username:    msg.Username,
					Password:    msg.Password,
					Org:         msg.Org,
					Bucket:      msg.Bucket,
					Database:    msg.Database,
					TenantID:    msg.TenantID,
					UpdatedAt:   msg.UpdatedAt,
				}
			}
		} else {
			influxDbConfigs[msg.TenantID] = InfluxDBConfig{
				Host:        msg.Host,
				AccessToken: msg.AccessToken,
				Username:    msg.Username,
				Password:    msg.Password,
				Org:         msg.Org,
				Bucket:      msg.Bucket,
				Database:    msg.Database,
				TenantID:    msg.TenantID,
				UpdatedAt:   msg.UpdatedAt,
			}
		}
		influxDbLock.Unlock()
	}
}

// updateDeviceLocations updates the deviceLocations map with messages coming from the channel
func updateDeviceLocations(deviceLocations map[TenantId]map[DeviceId][]Location, msgChan <-chan DeviceLocationKafkaMessage) {
	for msg := range msgChan {
		deviceLocationsLock.Lock()
		// Check if the device is already in the map and update it if ValidFrom is newer. Otherwise add it to the map.
		devices, ok := deviceLocations[TenantId(msg.TenantID)]
		if ok {
			locations, ok := devices[DeviceId(msg.DeviceID)]
			if ok {
				// Check if there is a newer location
				var found bool
				for i, loc := range locations {
					if loc.ValidFrom < msg.ValidFrom {
						devices[DeviceId(msg.DeviceID)] = append(locations[:i+1], locations[i:]...)
						devices[DeviceId(msg.DeviceID)][i] = Location{
							Name:      msg.LocationName,
							ValidFrom: msg.ValidFrom,
						}
						found = true
						break
					}
				}

				// If no newer location was found, append the new location to the end
				if !found {
					devices[DeviceId(msg.DeviceID)] = append(locations, Location{
						Name:      msg.LocationName,
						ValidFrom: msg.ValidFrom,
					})
				}
			} else {
				devices[DeviceId(msg.DeviceID)] = []Location{
					{
						Name:      msg.LocationName,
						ValidFrom: msg.ValidFrom,
					},
				}
			}
		} else {
			deviceLocations[TenantId(msg.TenantID)] = map[DeviceId][]Location{
				DeviceId(msg.DeviceID): {
					{
						Name:      msg.LocationName,
						ValidFrom: msg.ValidFrom,
					},
				},
			}
		}
		deviceLocationsLock.Unlock()
	}
}
