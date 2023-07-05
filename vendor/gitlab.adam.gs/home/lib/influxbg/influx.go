package influxbg

import (
	"time"

	"github.com/influxdata/influxdb/client/v2"
	log "github.com/sirupsen/logrus"
)

type InfluxBGWriter struct {
	pointChannel chan *client.Point
	influxClient client.Client
	database     string
}

func (w *InfluxBGWriter) writer() {
	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  w.database,
		Precision: "s",
	})
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Panic("unable to create new influx batch points")
	}
	timer := time.Tick(time.Second)
	for {
		select {
		case v := <-w.pointChannel:
			bp.AddPoint(v)
		case <-timer:
			bpc := len(bp.Points())
			if bpc == 0 {
				continue
			} else {
				log.WithFields(log.Fields{
					"points": bpc,
				}).Info("writing to influxdb")
			}

			err = w.influxClient.Write(bp)
			if err != nil {
				log.WithFields(log.Fields{
					"error": err,
				}).Error("writing to influxdb failed")
				continue
			}
			bp, err = client.NewBatchPoints(client.BatchPointsConfig{
				Database:  w.database,
				Precision: "s",
			})
			if err != nil {
				log.WithFields(log.Fields{
					"error":   err,
					"address": w.database,
				}).Error("unable to create new influx batch points")
				continue
			}
		}
	}
}

func (w *InfluxBGWriter) Write(measurement string, tags map[string]string, fields map[string]interface{}, ts time.Time) error {
	point, err := client.NewPoint(measurement, tags, fields, ts)
	if err != nil {
		return err
	}

	w.pointChannel <- point

	return nil
}

func NewInfluxBGWriter(httpConfig client.HTTPConfig, database string) (*InfluxBGWriter, error) {
	influxClient, err := client.NewHTTPClient(httpConfig)
	if err != nil {
		log.WithFields(log.Fields{
			"error":   err,
			"address": httpConfig.Addr,
		}).Error("unable to create new influx HTTP client")
		return nil, err
	}

	ibw := &InfluxBGWriter{
		pointChannel: make(chan *client.Point, 100000),
		influxClient: influxClient,
		database:     database,
	}

	go ibw.writer()

	return ibw, nil
}
