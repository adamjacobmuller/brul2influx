package main

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/adamjacobmuller/brul2influx/lib"
	"github.com/influxdata/influxdb/client/v2"
	log "github.com/sirupsen/logrus"
)

type EnergySample struct {
	WattHours float64
	Watts     float64
	Amps      float64
}

type PulseSample struct {
	Pulses int64
}

type TemperatureSample struct {
	Temperature float64
}

func main() {
	conn, err := net.Dial("tcp", "10.0.8.107:8001")
	if err != nil {
		log.Fatal("failed connecting")
	}

	influxClient, err := client.NewHTTPClient(client.HTTPConfig{
		Addr: "https://influxdb.adam.gs",
	})
	if err != nil {
		log.WithFields(log.Fields{
			"error":   err,
			"address": "https://influxdb.adam.gs",
		}).Panic("unable to create new influx HTTP client")
	}
	bgChannel, err := influxbg.NewInfluxBGWriter(influxClient, "gem")

	go func() {
		for range time.Tick(time.Second) {
			conn.Write([]byte("^^^APISPK"))
		}
	}()

	for {
		bl := make([]byte, 5000)

		b, err := conn.Read(bl)
		if err != nil {
			log.Fatal("failed reading")
		}
		if b == 0 {
			continue
		}

		var volts float64
		var serial string

		energy_channels := make(map[int64]*EnergySample)
		pulse_channels := make(map[int64]*PulseSample)
		temperature_channels := make(map[int64]*TemperatureSample)

		dataTrim := string(bl[0 : b-2])
		pairs := strings.Split(dataTrim, "&")
		for _, dataPoint := range pairs {
			dataPointSplit := strings.Split(dataPoint, "=")
			if len(dataPointSplit) != 2 {
				log.WithFields(log.Fields{
					"dataPointSplit": dataPointSplit,
					"dataPoint":      dataPoint,
					"dataTrim":       dataTrim,
				}).Error("len(dataPointSplit) != 2")
				continue
			}
			dataPointKey := dataPointSplit[0]
			dataPointValue := dataPointSplit[1]

			switch dataPointKey {
			case "v":
				volts, err = strconv.ParseFloat(dataPointValue, 64)
				if err != nil {
					log.WithFields(log.Fields{
						"dataPoint":      dataPoint,
						"dataPointValue": dataPointValue,
						"dataTrim":       dataTrim,
					}).Error("unable to parseint for dataPointChannel")
					continue
				}
			case "n":
				serial = dataPointValue
			case "m":
				continue
			default:
				dataPointKeySplit := strings.Split(dataPointKey, "_")
				if len(dataPointKeySplit) != 2 {
					log.WithFields(log.Fields{
						"dataPointKeySplit": dataPointKeySplit,
						"dataPointSplit":    dataPointSplit,
						"dataPointKey":      dataPointKey,
						"dataPoint":         dataPoint,
						"dataTrim":          dataTrim,
					}).Error("len(dataPointKeySplit) != 2")
					continue
				}
				dataPointType := dataPointKeySplit[0]
				dataPointChannel := dataPointKeySplit[1]
				channel, err := strconv.ParseInt(dataPointChannel, 10, 64)
				if err != nil {
					log.WithFields(log.Fields{
						"dataPoint":        dataPoint,
						"dataPointKey":     dataPointSplit,
						"dataPointType":    dataPointType,
						"dataPointChannel": dataPointChannel,
						"dataTrim":         dataTrim,
					}).Error("unable to parseint for dataPointChannel")
					continue
				}
				switch dataPointType {
				case "wh":
					val, err := strconv.ParseFloat(dataPointValue, 64)
					if err != nil {
						log.WithFields(log.Fields{
							"dataPoint":        dataPoint,
							"dataPointKey":     dataPointSplit,
							"dataPointType":    dataPointType,
							"dataPointValue":   dataPointValue,
							"dataPointChannel": dataPointChannel,
							"dataTrim":         dataTrim,
						}).Error("unable to parseint for dataPointValue")
						continue
					}
					_, ok := energy_channels[channel]
					if !ok {
						energy_channels[channel] = &EnergySample{}
					}
					energy_channels[channel].WattHours = val
				case "p":
					val, err := strconv.ParseFloat(dataPointValue, 64)
					if err != nil {
						log.WithFields(log.Fields{
							"dataPoint":        dataPoint,
							"dataPointKey":     dataPointSplit,
							"dataPointType":    dataPointType,
							"dataPointValue":   dataPointValue,
							"dataPointChannel": dataPointChannel,
							"dataTrim":         dataTrim,
						}).Error("unable to parseint for dataPointValue")
						continue
					}
					_, ok := energy_channels[channel]
					if !ok {
						energy_channels[channel] = &EnergySample{}
					}
					energy_channels[channel].Watts = val
				case "a":
					val, err := strconv.ParseFloat(dataPointValue, 64)
					if err != nil {
						log.WithFields(log.Fields{
							"dataPoint":        dataPoint,
							"dataPointKey":     dataPointSplit,
							"dataPointType":    dataPointType,
							"dataPointValue":   dataPointValue,
							"dataPointChannel": dataPointChannel,
							"dataTrim":         dataTrim,
						}).Error("unable to parseint for dataPointValue")
						continue
					}
					_, ok := energy_channels[channel]
					if !ok {
						energy_channels[channel] = &EnergySample{}
					}
					energy_channels[channel].Amps = val
				case "t":
					if dataPointValue == "nc" {
						continue
					}
					if dataPointValue == "x" {
						continue
					}
					val, err := strconv.ParseFloat(dataPointValue, 64)
					if err != nil {
						log.WithFields(log.Fields{
							"dataPoint":        dataPoint,
							"dataPointKey":     dataPointSplit,
							"dataPointChannel": dataPointChannel,
							"dataTrim":         dataTrim,
						}).Error("unable to parseint for dataPointValue")
						continue
					}
					temperature_channels[channel] = &TemperatureSample{Temperature: val}
				case "c":
					val, err := strconv.ParseInt(dataPointValue, 10, 64)
					if err != nil {
						log.WithFields(log.Fields{
							"dataPoint":        dataPoint,
							"dataPointKey":     dataPointSplit,
							"dataPointChannel": dataPointChannel,
							"dataTrim":         dataTrim,
						}).Error("unable to parseint for dataPointValue")
						continue
					}
					pulse_channels[channel] = &PulseSample{Pulses: val}
				}
			}
		}
		ts := time.Now()

		voltage_tags := map[string]string{
			"serial": serial,
		}
		voltage_fields := map[string]interface{}{
			"volts": volts,
		}

		voltagePoint, err := client.NewPoint("voltage", voltage_tags, voltage_fields, ts)
		if err != nil {
			log.WithFields(log.Fields{
				"error":  err,
				"tags":   voltage_tags,
				"fields": voltage_fields,
			}).Error("unable to create point for voltage")
		} else {
			bgChannel <- voltagePoint
		}

		for channel, value := range energy_channels {
			//fmt.Printf("Energy %d = %#v\n", channel, value)
			energy_tags := map[string]string{
				"serial":  serial,
				"channel": fmt.Sprintf("%d", channel),
			}
			energy_fields := map[string]interface{}{
				"watt-hours": value.WattHours,
				"watts":      value.Watts,
				"amps":       value.Amps,
			}

			energyPoint, err := client.NewPoint("energy", energy_tags, energy_fields, ts)
			if err != nil {
				log.WithFields(log.Fields{
					"error":  err,
					"tags":   energy_tags,
					"fields": energy_fields,
				}).Error("unable to create point for energy")
			} else {
				bgChannel <- energyPoint
			}
		}
		for channel, value := range temperature_channels {
			//fmt.Printf("Temperature %d = %#v\n", channel, value)
			temperature_tags := map[string]string{
				"serial":  serial,
				"channel": fmt.Sprintf("%d", channel),
			}
			temperature_fields := map[string]interface{}{
				"temperature": value.Temperature,
			}

			temperaturePoint, err := client.NewPoint("temperature", temperature_tags, temperature_fields, ts)
			if err != nil {
				log.WithFields(log.Fields{
					"error":  err,
					"tags":   temperature_tags,
					"fields": temperature_fields,
				}).Error("unable to create point for temperature")
			} else {
				bgChannel <- temperaturePoint
			}
		}
		for channel, value := range pulse_channels {
			//fmt.Printf("Pulse %d = %#v\n", channel, value)
			pulse_tags := map[string]string{
				"serial":  serial,
				"channel": fmt.Sprintf("%d", channel),
			}
			pulse_fields := map[string]interface{}{
				"pulses": value.Pulses,
			}

			pulsePoint, err := client.NewPoint("pulses", pulse_tags, pulse_fields, ts)
			if err != nil {
				log.WithFields(log.Fields{
					"error":  err,
					"tags":   pulse_tags,
					"fields": pulse_fields,
				}).Error("unable to create point for pulses")
			} else {
				bgChannel <- pulsePoint
			}
		}
	}
}
