package main

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/joho/godotenv/autoload"
	"github.com/tuhis/ruuvi-saver/pkg/config"
	"go.uber.org/zap"
)

func main() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	// Init logger
	logger, _ := zap.NewProduction()

	logger.Info("Starting ruuvi-saver")

	// Init config
	config := config.New(logger.Sugar())

	// Signal handler, cleanup and exit
	go func() {
		<-c
		logger.Info("Shutting down")
		config.Close()
		os.Exit(1)
	}()

	// Sleep 10 seconds and print owner of gateway with ID "1"
	logger.Info("Sleeping for 10 seconds")
	time.Sleep(10 * time.Second)
	owner, err := config.GetGatewayOwner("1")
	if err != nil {
		logger.Error("Failed to get gateway owner", zap.Error(err))
	}

	logger.Info("Gateway owner", zap.String("owner", owner))

	// Print InfluxDB config for tenant with ID "123"
	configResult, err := config.GetInfluxDBConfig("123")
	if err != nil {
		logger.Error("Failed to get InfluxDB config", zap.Error(err))
	}

	logger.Info("InfluxDB config for tenant with ID 123", zap.Any("config", configResult))

}
