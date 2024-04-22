package tsdb

import (
	"time"

	_ "github.com/influxdata/influxdb1-client" // this is important because of the bug in go mod
	client "github.com/influxdata/influxdb1-client/v2"
	"github.com/tuhis/ruuvi-saver/pkg/config"
	"go.uber.org/zap"
)

type TSDBWriter interface {
	WriteMeasurement(TenantID string, measurement string, tags map[string]string, fields map[string]interface{}) error
}

type InfluxDBWriter struct {
	InfluxDBRepository config.InfluxDBRepository
	httpClientCache    map[string]client.Client
	Logger             *zap.SugaredLogger
}

func (w *InfluxDBWriter) WriteMeasurement(TenantID string, measurement string, tags map[string]string, fields map[string]interface{}) error {
	// Get InfluxDB config and create NewHTTPClient
	influxDBCfg, err := w.InfluxDBRepository.GetInfluxDBConfig(TenantID)
	if err != nil {
		w.Logger.Error("Failed to get InfluxDB config", zap.Error(err))
		return err
	}

	// Try to get c from cache
	c, ok := w.httpClientCache[TenantID]
	if !ok {
		c, err = client.NewHTTPClient(client.HTTPConfig{
			Addr:     influxDBCfg.Host,
			Username: influxDBCfg.Username,
			Password: influxDBCfg.Password,
		})
		if err != nil {
			w.Logger.Error("Failed to create InfluxDB client", zap.Error(err))
			return err
		}

		// Check if the cache is nil and init it as a map
		if w.httpClientCache == nil {
			w.httpClientCache = make(map[string]client.Client)
		}

		// Cache the client
		w.httpClientCache[TenantID] = c
	}

	// Create a new point batch
	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  influxDBCfg.Bucket,
		Precision: "s",
	})
	if err != nil {
		w.Logger.Error("Failed to create batch points", zap.Error(err))
		return err
	}

	// Create a new point and add it to the batch
	pt, err := client.NewPoint(measurement, tags, fields, time.Now()) // TODO: Read time from kafka message
	if err != nil {
		w.Logger.Error("Failed to create new point", zap.Error(err))
		return err
	}
	bp.AddPoint(pt)

	// Write the batch
	if err := c.Write(bp); err != nil {
		w.Logger.Error("Failed to write batch", zap.Error(err))
		return err
	}

	return nil
}
