package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/joho/godotenv/autoload"
	"github.com/segmentio/kafka-go"
	"github.com/tuhis/ruuvi-saver/internal"
	"github.com/tuhis/ruuvi-saver/pkg/config"
	"github.com/tuhis/ruuvi-saver/pkg/tsdb"
	"go.uber.org/zap"
)

func main() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	// Main context
	ctx, cancel := context.WithCancel(context.Background())

	// Init logger
	logger, _ := zap.NewProduction()

	logger.Info("Starting ruuvi-saver")

	// Init config
	config := config.New(logger.Sugar())

	// Create TSDBWriter
	tsdbWriter := tsdb.InfluxDBWriter{
		InfluxDBRepository:        config,
		GatewayRepository:         config,
		DeviceLocationReposistory: config,
		Logger:                    logger.Sugar(),
	}

	// Create RuuviConsumer
	consumerCtx, consumerCancel := context.WithCancel(ctx)
	ruuviConsumer := internal.RuuviConsumer{
		Logger:    logger.Sugar(),
		MsgChan:   make(chan kafka.Message, 10), // TODO: Increase buffer size after some testing
		ReaderCtx: consumerCtx,
		CtxCancel: consumerCancel,
	}

	// Signal handler, cleanup and exit
	go func() {
		<-c
		logger.Info("Shutting down")
		config.Close()
		ruuviConsumer.Stop()
		cancel()
		os.Exit(1)
	}()

	// Sleep 10 seconds and print owner of gateway with ID "1"
	logger.Info("Sleeping for 10 seconds")
	time.Sleep(10 * time.Second)

	// Start main consumer
	ruuviConsumer.Start()

	// Start processing of the messages
	tsdbWriter.WriteMeasurementsFromKafkaMessageChannel(ctx, ruuviConsumer.MsgChan)
}
