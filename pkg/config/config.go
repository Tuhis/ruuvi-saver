package config

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"go.uber.org/zap"
)

type GatewayRepository interface {
	GetGatewayOwner(gatewayID string) (string, error)
	GetInfluxDBConfig(tenantID string) (InfluxDBConfig, error)
}

type DeviceLocationRepository interface {
	GetDeviceLocation(tenantId string, deviceId string, timestamp int64) (string, error)
}

type InfluxDBRepository interface {
	GetInfluxDBConfig(tenantID string) (InfluxDBConfig, error)
}

type Gateway struct {
	ID         string
	Owner      string
	OwnerSince int
}

type GatewayKafkaMessage struct {
	ID         string `json:"id"`
	Owner      string `json:"owner"`
	OwnerSince int    `json:"owner_since"`
}

type InfluxDBConfig struct {
	Host        string
	AccessToken string
	Username    string
	Password    string
	Org         string
	Bucket      string
	Database    string
	TenantID    string
	UpdatedAt   int
}

type InfluxDBConfigKafkaMessage struct {
	Host        string `json:"host"`
	AccessToken string `json:"access_token"`
	Username    string `json:"username"`
	Password    string `json:"password"`
	Org         string `json:"org"`
	Bucket      string `json:"bucket"`
	Database    string `json:"database"`
	TenantID    string `json:"tenant_id"`
	UpdatedAt   int    `json:"updated_at"`
}

type DeviceLocationKafkaMessage struct {
	TenantID     string `json:"tenant_id"`
	DeviceID     string `json:"device_id"`
	LocationName string `json:"location_name"`
	ValidFrom    int    `json:"valid_from"`
}

type Location struct {
	Name      string
	ValidFrom int
}

type TenantId string

type DeviceId string

type Config struct {
	gateways        map[string]Gateway
	influxDbConfigs map[string]InfluxDBConfig
	deviceLocations map[TenantId]map[DeviceId][]Location
	logger          *zap.SugaredLogger
}

var ErrNoValidLocation = errors.New("no valid location found")

var gwLock = sync.RWMutex{}
var influxDbLock = sync.RWMutex{}
var deviceLocationsLock = sync.RWMutex{}
var wg sync.WaitGroup
var readerCtx, stopKafkaReaders = context.WithCancel(context.Background())

func New(logger *zap.SugaredLogger) *Config {
	config := &Config{
		logger: logger,
	}

	gateways, err := ReadGatewaysFromKafka(logger)
	if err != nil {
		panic(err)
	}

	influxDbConfigs, err := ReadInfluxDBConfigsFromKafka(logger)
	if err != nil {
		panic(err)
	}

	deviceLocations, err := ReadDeviceLocationsFromKafka(logger)
	if err != nil {
		panic(err)
	}

	config.gateways = gateways
	config.influxDbConfigs = influxDbConfigs
	config.deviceLocations = deviceLocations

	return config
}

func (c *Config) GetGatewayOwner(gatewayID string) (string, error) {
	gwLock.RLock()
	defer gwLock.RUnlock()

	gateway, ok := c.gateways[gatewayID]
	if !ok {
		return "", fmt.Errorf("gateway not found")
	}
	return gateway.Owner, nil
}

func (c *Config) GetInfluxDBConfig(tenantID string) (InfluxDBConfig, error) {
	influxDbLock.RLock()
	defer influxDbLock.RUnlock()

	influxDBCfg, ok := c.influxDbConfigs[tenantID]
	if !ok {
		return InfluxDBConfig{}, fmt.Errorf("influxdb config not found")
	}
	return influxDBCfg, nil
}

func (c *Config) GetDeviceLocation(tenantId string, deviceId string, timestamp int64) (string, error) {
	deviceLocationsLock.RLock()
	defer deviceLocationsLock.RUnlock()

	tenantDevices, ok := c.deviceLocations[TenantId(tenantId)]
	if !ok {
		return "", fmt.Errorf("tenant not found")
	}
	locations, ok := tenantDevices[DeviceId(deviceId)]
	if !ok {
		return "", fmt.Errorf("device not found")
	}
	// We know that locations slice is sorted by ValidFrom, so we can just iterate through it
	for _, location := range locations {
		if int64(location.ValidFrom) <= timestamp {
			return location.Name, nil
		}
	}
	return "", ErrNoValidLocation
}

func (c *Config) Close() {
	c.logger.Info("Closing config, stopping kafka readers")
	stopKafkaReaders()
	// Wait until kafkaReaderCount done messages have been received
	wg.Wait()
	c.logger.Info("All readers stopped")
}
