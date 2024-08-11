package internal

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
	"github.com/tuhis/ruuvi-saver/pkg/config"
	"go.uber.org/zap"
)

// TODO: Figure out whether interface is needed at all
type RuuviReader interface {
	Start() error
	Stop()
}

type RuuviConsumer struct {
	Reader    *kafka.Reader
	MsgChan   chan kafka.Message
	Logger    *zap.SugaredLogger
	wg        sync.WaitGroup
	ReaderCtx context.Context
	CtxCancel context.CancelFunc
}

func (r *RuuviConsumer) Start() error {
	kafkaConfig, err := config.KafkaConfigFromEnvironment()
	if err != nil {
		return fmt.Errorf("failed to read configuration from environment: %v", err)
	}

	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
	}

	if kafkaConfig.AuthMechanism == config.AuthMechanismScram {
		mechanism, err := scram.Mechanism(scram.SHA512, kafkaConfig.Username, kafkaConfig.Password)
		if err != nil {
			panic(err)
		}

		dialer.SASLMechanism = mechanism
	}

	r.Reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaConfig.Brokers},
		GroupID: "ruuvi-saver", // TODO: Support overriding via env var
		Topic:   kafkaConfig.RuuviEventTopic,
		Dialer:  dialer,
	})

	r.wg.Add(1)
	go r.Read()

	return nil
}

func (r *RuuviConsumer) Stop() {
	r.Logger.Info("Stopping RuuviConsumer")
	r.CtxCancel()
	r.wg.Wait()
	r.Logger.Info("RuuviConsumer stopped")
}

func (r *RuuviConsumer) Read() {
	for {
		select {
		case <-r.ReaderCtx.Done():
			r.Logger.Info("Received stop signal, closing RuuviConsumer Kafka reader")
			r.Reader.Close()
			r.wg.Done()
			return

		default:
			// TODO: Switch to FetchMessage and handle commits manually
			m, err := r.Reader.ReadMessage(r.ReaderCtx)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					r.Logger.Info("Received stop signal, closing RuuviConsumer Kafka reader")
					r.Reader.Close()
					r.wg.Done()
					return
				}

				r.Logger.Error("Failed to read message", zap.Error(err))
				panic(err)
			}

			r.MsgChan <- m
		}
	}
}
