package tsdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetNewMeasurementDataFromEventData(t *testing.T) {
	t.Run("valid input", func(t *testing.T) {
		eventData := map[string]interface{}{
			"Address":     "test address",
			"LocalName":   "test localName",
			"Temperature": 25.5,
			"Humidity":    60.5,
			"Pressure":    1013.0,
			"Acceleration": map[string]interface{}{
				"X": 1.0,
				"Y": 2.0,
				"Z": 3.0,
			},
			"Battery":    95.0,
			"RSSI":       -60.0,
			"TXPower":    -40.0,
			"MAC":        "test mac",
			"Movement":   float64(101),
			"Sequence":   float64(202),
			"DataFormat": float64(3),
		}

		data, err := GetNewMeasurementDataFromEventData(eventData)

		assert.NoError(t, err)
		assert.Equal(t, "test address", *data.Address)
		assert.Equal(t, "test localName", *data.LocalName)
		assert.Equal(t, 25.5, *data.Temperature)
		assert.Equal(t, 60.5, *data.Humidity)
		assert.Equal(t, 1013, *data.Pressure)
		assert.Equal(t, 1, *data.Acceleration.X)
		assert.Equal(t, 2, *data.Acceleration.Y)
		assert.Equal(t, 3, *data.Acceleration.Z)
		assert.Equal(t, 95, *data.Battery)
		assert.Equal(t, -60, *data.RSSI)
		assert.Equal(t, -40, *data.TXPower)
		assert.Equal(t, "test mac", *data.MAC)
		assert.Equal(t, 101, *data.Movement)
		assert.Equal(t, 202, *data.Sequence)
		assert.Equal(t, 3, *data.DataFormat)
	})

	t.Run("valid input with extra fields", func(t *testing.T) {
		eventData := map[string]interface{}{
			"Address":     "test address",
			"LocalName":   "test localName",
			"Temperature": 25.5,
			"Humidity":    60.5,
			"Pressure":    1013.0,
			"Acceleration": map[string]interface{}{
				"X": 1.0,
				"Y": 2.0,
				"Z": 3.0,
			},
			"Battery":    95.0,
			"RSSI":       -60.0,
			"TXPower":    -40.0,
			"MAC":        "test mac",
			"Movement":   float64(101),
			"Sequence":   float64(202),
			"DataFormat": float64(3),
			"ExtraField": "extra",
		}

		data, err := GetNewMeasurementDataFromEventData(eventData)

		assert.NoError(t, err)
		assert.Equal(t, "test address", *data.Address)
		assert.Equal(t, "test localName", *data.LocalName)
		assert.Equal(t, 25.5, *data.Temperature)
		assert.Equal(t, 60.5, *data.Humidity)
		assert.Equal(t, 1013, *data.Pressure)
		assert.Equal(t, 1, *data.Acceleration.X)
		assert.Equal(t, 2, *data.Acceleration.Y)
		assert.Equal(t, 3, *data.Acceleration.Z)
		assert.Equal(t, 95, *data.Battery)
		assert.Equal(t, -60, *data.RSSI)
		assert.Equal(t, -40, *data.TXPower)
	})

	t.Run("invalid input", func(t *testing.T) {
		eventData := "invalid"

		data, err := GetNewMeasurementDataFromEventData(eventData)

		assert.Error(t, err)
		assert.Nil(t, data)
	})

	t.Run("missing field", func(t *testing.T) {
		eventData := map[string]interface{}{
			"Address": "test address",
			// "LocalName" field is missing
			"Temperature": 25.5,
			"Humidity":    60.5,
			"Pressure":    1013.0,
			"Acceleration": map[string]interface{}{
				"X": 1.0,
				"Y": 2.0,
				"Z": 3.0,
			},
			"Battery":    95.0,
			"RSSI":       -60.0,
			"TXPower":    -40.0,
			"MAC":        "test mac",
			"Movement":   float64(101),
			"Sequence":   float64(202),
			"DataFormat": float64(3),
		}

		data, err := GetNewMeasurementDataFromEventData(eventData)

		assert.Error(t, err)
		assert.Nil(t, data)
	})
}
