package tsdb

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/Tuhis/edge-receiver/pkg/events"
	_ "github.com/influxdata/influxdb1-client" // this is important because of the bug in go mod
	client "github.com/influxdata/influxdb1-client/v2"
	"github.com/segmentio/kafka-go"
	"github.com/tuhis/ruuvi-saver/pkg/config"
	"go.uber.org/zap"
)

type TSDBWriter interface {
	WriteMeasurement(TenantID string, measurement string, tags map[string]string, fields map[string]interface{}) error
	WriteMeasurementsFromKafkaMessageChannel(ctx context.Context, msgCh chan kafka.Message)
}

type InfluxDBWriter struct {
	InfluxDBRepository        config.InfluxDBRepository
	GatewayRepository         config.GatewayRepository
	DeviceLocationReposistory config.DeviceLocationRepository
	httpClientCache           map[string]client.Client
	Logger                    *zap.SugaredLogger
}

func (w *InfluxDBWriter) WriteMeasurementPoints(tenantId string, measurements []*client.Point) error {
	c, bp, err := w.GetClientAndBatchPointsForTenant(tenantId)
	if err != nil {
		w.Logger.Error("Failed to get client and batch points", zap.Error(err))
		return err
	}

	for _, pt := range measurements {
		bp.AddPoint(pt)
	}

	if err := c.Write(bp); err != nil {
		w.Logger.Error("Failed to write batch", zap.Error(err))
		return err
	}

	return nil
}

func (w *InfluxDBWriter) GetClientAndBatchPointsForTenant(tenantId string) (client.Client, client.BatchPoints, error) {
	influxDBCfg, err := w.InfluxDBRepository.GetInfluxDBConfig(tenantId)
	if err != nil {
		w.Logger.Error("Failed to get InfluxDB config", zap.Error(err))
		return nil, nil, err
	}

	c, ok := w.httpClientCache[tenantId]
	if !ok {
		c, err = client.NewHTTPClient(client.HTTPConfig{
			Addr:     influxDBCfg.Host,
			Username: influxDBCfg.Username,
			Password: influxDBCfg.Password,
		})
		if err != nil {
			w.Logger.Error("Failed to create InfluxDB client", zap.Error(err))
			return nil, nil, err
		}

		if w.httpClientCache == nil {
			w.httpClientCache = make(map[string]client.Client)
		}

		w.httpClientCache[tenantId] = c
	}

	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  influxDBCfg.Bucket,
		Precision: "s",
	})
	if err != nil {
		w.Logger.Error("Failed to create batch points", zap.Error(err))
		return nil, nil, err
	}

	return c, bp, nil
}

func (w *InfluxDBWriter) WriteMeasurementsFromKafkaMessageChannel(ctx context.Context, msgCh chan kafka.Message) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-msgCh:
			var kafkaMsg events.RuuviKafkaEvent
			if err := json.Unmarshal(msg.Value, &kafkaMsg); err != nil {
				w.Logger.Errorf("Failed to unmarshal message: %v", err)
				continue
			}

			w.Logger.Debugf("Received message: %v", kafkaMsg)

			// Get tenantID from kafka message
			tenantID, err := w.GatewayRepository.GetGatewayOwner(kafkaMsg.SourceUuid)
			if err != nil {
				w.Logger.Error("Failed to get gateway owner", zap.Error(err))
				continue
			}

			switch kafkaMsg.Type {
			case events.NewMeasurement:
				// extract NewMeasurementData from the event
				data, err := GetNewMeasurementDataFromEventData(kafkaMsg.Data)
				if err != nil {
					w.Logger.Error("Failed to get NewMeasurementData from event data", zap.Error(err))
					continue
				}

				// get sensor location
				location, err := w.DeviceLocationReposistory.GetDeviceLocation(tenantID, *data.MAC, msg.Time.Unix())
				if err != nil {
					// If no location is found, skip the message
					w.Logger.Error("Failed to get device location", zap.Error(err))
					continue
				}

				var measurementPoints []*client.Point
				tags := map[string]string{
					"address":   *data.Address,
					"source":    kafkaMsg.SourceUuid,
					"localName": *data.LocalName,
					"location":  location,
				}

				point, err := client.NewPoint(
					"temperature",
					tags,
					map[string]interface{}{"value": *data.Temperature},
					msg.Time)
				if err != nil {
					w.Logger.Error("Failed to create new point", zap.Error(err))
				}
				measurementPoints = append(measurementPoints, point)

				point, err = client.NewPoint(
					"humidity",
					tags,
					map[string]interface{}{"value": *data.Humidity},
					msg.Time)
				if err != nil {
					w.Logger.Error("Failed to create new point", zap.Error(err))
				}
				measurementPoints = append(measurementPoints, point)

				point, err = client.NewPoint(
					"pressure",
					tags,
					map[string]interface{}{"value": *data.Pressure},
					msg.Time)
				if err != nil {
					w.Logger.Error("Failed to create new point", zap.Error(err))
				}
				measurementPoints = append(measurementPoints, point)

				point, err = client.NewPoint(
					"accelerationX",
					tags,
					map[string]interface{}{"value": *data.Acceleration.X},
					msg.Time)
				if err != nil {
					w.Logger.Error("Failed to create new point", zap.Error(err))
				}
				measurementPoints = append(measurementPoints, point)

				point, err = client.NewPoint(
					"accelerationY",
					tags,
					map[string]interface{}{"value": *data.Acceleration.Y},
					msg.Time)
				if err != nil {
					w.Logger.Error("Failed to create new point", zap.Error(err))
				}
				measurementPoints = append(measurementPoints, point)

				point, err = client.NewPoint(
					"accelerationZ",
					tags,
					map[string]interface{}{"value": *data.Acceleration.Z},
					msg.Time)
				if err != nil {
					w.Logger.Error("Failed to create new point", zap.Error(err))
				}
				measurementPoints = append(measurementPoints, point)

				point, err = client.NewPoint(
					"battery",
					tags,
					map[string]interface{}{"value": *data.Battery},
					msg.Time)
				if err != nil {
					w.Logger.Error("Failed to create new point", zap.Error(err))
				}
				measurementPoints = append(measurementPoints, point)

				point, err = client.NewPoint(
					"rssi",
					tags,
					map[string]interface{}{"value": *data.RSSI},
					msg.Time)
				if err != nil {
					w.Logger.Error("Failed to create new point", zap.Error(err))
				}
				measurementPoints = append(measurementPoints, point)

				point, err = client.NewPoint(
					"txPower",
					tags,
					map[string]interface{}{"value": *data.TXPower},
					msg.Time)
				if err != nil {
					w.Logger.Error("Failed to create new point", zap.Error(err))
				}
				measurementPoints = append(measurementPoints, point)

				err = w.WriteMeasurementPoints(tenantID, measurementPoints)
				if err != nil {
					w.Logger.Error("Failed to write measurement points", zap.Error(err))
				}

			default:
				w.Logger.Warn("Unknown message type", zap.String("type", string(kafkaMsg.Type)))
				continue
			}
		}
	}
}

func GetNewMeasurementDataFromEventData(eventData interface{}) (*events.NewMeasurementData, error) {
	var data events.NewMeasurementData

	// Firstly, get the data as a map
	dataMap, ok := eventData.(map[string]interface{})
	if !ok {
		return nil, errors.New("failed to cast event data to map")
	}

	// Then, get individual fields from the map
	if address, ok := dataMap["Address"].(string); ok {
		data.Address = &address
	} else {
		return nil, errors.New("failed to get address from event data")
	}

	if mac, ok := dataMap["MAC"].(string); ok {
		data.MAC = &mac
	} else {
		return nil, errors.New("failed to get mac from event data")
	}

	if dataFormat, ok := dataMap["DataFormat"].(float64); ok {
		// cast to int, known to be integer in source
		dataFormat := int(dataFormat)
		data.DataFormat = &dataFormat
	} else {
		return nil, errors.New("failed to get dataFormat from event data")
	}

	if movement, ok := dataMap["Movement"].(float64); ok {
		// cast to int, known to be integer in source
		movement := int(movement)
		data.Movement = &movement
	} else {
		return nil, errors.New("failed to get movement from event data")
	}

	if sequence, ok := dataMap["Sequence"].(float64); ok {
		// cast to int, known to be integer in source
		sequence := int(sequence)
		data.Sequence = &sequence
	} else {
		return nil, errors.New("failed to get sequence from event data")
	}

	if localName, ok := dataMap["LocalName"].(string); ok {
		data.LocalName = &localName
	} else {
		return nil, errors.New("failed to get localName from event data")
	}

	if temperature, ok := dataMap["Temperature"].(float64); ok {
		data.Temperature = &temperature
	} else {
		return nil, errors.New("failed to get temperature from event data")
	}

	if humidity, ok := dataMap["Humidity"].(float64); ok {
		data.Humidity = &humidity
	} else {
		return nil, errors.New("failed to get humidity from event data")
	}

	if pressure, ok := dataMap["Pressure"].(float64); ok {
		pressure := int(pressure) // cast to int, known to be integer in source
		data.Pressure = &pressure
	} else {
		return nil, errors.New("failed to get pressure from event data")
	}

	if acceleration, ok := dataMap["Acceleration"].(map[string]interface{}); ok {
		accelerationStruct := struct {
			X *int `json:"X"`
			Y *int `json:"Y"`
			Z *int `json:"Z"`
		}{}
		data.Acceleration = &accelerationStruct

		if x, ok := acceleration["X"].(float64); ok {
			// cast to int, known to be integer in source
			x := int(x)
			data.Acceleration.X = &x
		} else {
			return nil, errors.New("failed to get acceleration.X from event data")
		}

		if y, ok := acceleration["Y"].(float64); ok {
			// cast to int, known to be integer in source
			y := int(y)
			data.Acceleration.Y = &y
		} else {
			return nil, errors.New("failed to get acceleration.Y from event data")
		}

		if z, ok := acceleration["Z"].(float64); ok {
			// cast to int, known to be integer in source
			z := int(z)
			data.Acceleration.Z = &z
		} else {
			return nil, errors.New("failed to get acceleration.Z from event data")
		}
	} else {
		return nil, errors.New("failed to get acceleration from event data")
	}

	if battery, ok := dataMap["Battery"].(float64); ok {
		// cast to int, known to be integer in source
		battery := int(battery)
		data.Battery = &battery
	} else {
		return nil, errors.New("failed to get battery from event data")
	}

	if rssi, ok := dataMap["RSSI"].(float64); ok {
		// cast to int, known to be integer in source
		rssi := int(rssi)
		data.RSSI = &rssi
	} else {
		return nil, errors.New("failed to get rssi from event data")
	}

	if txPower, ok := dataMap["TXPower"].(float64); ok {
		// cast to int, known to be integer in source
		txPower := int(txPower)
		data.TXPower = &txPower
	} else {
		return nil, errors.New("failed to get txPower from event data")
	}

	return &data, nil
}
