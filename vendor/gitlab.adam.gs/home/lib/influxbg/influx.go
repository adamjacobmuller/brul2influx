package influxbg

import (
	"strings"
	"time"

	"github.com/influxdata/influxdb/client/v2"
	log "github.com/sirupsen/logrus"
	"gitlab.adam.gs/home/lib/ticker"
)

type InfluxBGWriter struct {
	pointChannel chan *client.Point
	influxClient client.Client
	database     string
	MaxPoints    int
}

func (w *InfluxBGWriter) writer() {
	bp, err := createNewBatchPoints(w.database)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Panic("unable to create new influx batch points")
	}

	dynamicTicker := ticker.NewDynamicTicker(time.Second)
	defer dynamicTicker.Stop()
	var overflowPoints []*client.Point

	for {
		select {
		case v := <-w.pointChannel:
			bp.AddPoint(v)
		case <-dynamicTicker.C:
			points := append(overflowPoints, bp.Points()...)
			overflowPoints = nil // reset overflowPoints

			bpc := len(points)
			if bpc == 0 {
				continue
			}

			pointsToSend := points
			if w.MaxPoints > 0 && bpc > w.MaxPoints {
				pointsToSend = points[:w.MaxPoints]
				overflowPoints = points[w.MaxPoints:]
			}

			log.WithFields(log.Fields{
				"points-to-send":  len(pointsToSend),
				"points-overflow": len(overflowPoints),
			}).Info("writing to influxdb")

			err = w.influxClient.Write(createBatchPointsWithGivenPoints(w.database, pointsToSend))
			if err != nil {
				if isInfluxError413(err) {
					log.WithFields(log.Fields{
						"error":           err,
						"points-to-send":  len(pointsToSend),
						"points-overflow": len(overflowPoints),
					}).Info("content too large, adjusting batch size and saving the rest for later")

					w.MaxPoints = len(pointsToSend) / 2
					overflowPoints = points[w.MaxPoints:]
					w.influxClient.Write(createBatchPointsWithGivenPoints(w.database, points[:w.MaxPoints]))
				} else {
					log.WithFields(log.Fields{
						"error":           err,
						"points-to-send":  len(pointsToSend),
						"points-overflow": len(overflowPoints),
					}).Error("writing to influxdb failed")
					overflowPoints = append(overflowPoints, pointsToSend...)
				}
			}

			// Adjust the write interval based on the number of overflow points
			if len(overflowPoints) > w.MaxPoints {
				dynamicTicker.SetInterval(100 * time.Millisecond)
			} else {
				dynamicTicker.SetInterval(time.Second)
			}

			bp, err = createNewBatchPoints(w.database)
			if err != nil {
				log.WithFields(log.Fields{
					"error": err,
				}).Error("unable to create new influx batch points")
				continue
			}
		}
	}
}

func isInfluxError413(err error) bool {
	return strings.Contains(err.Error(), "413 Request Entity Too Large")
}

func createNewBatchPoints(database string) (client.BatchPoints, error) {
	return client.NewBatchPoints(client.BatchPointsConfig{
		Database:  database,
		Precision: "s",
	})
}

func createBatchPointsWithGivenPoints(database string, points []*client.Point) client.BatchPoints {
	bp, err := createNewBatchPoints(database)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Error("unable to create new influx batch points")
		return bp
	}
	for _, point := range points {
		bp.AddPoint(point)
	}
	return bp
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
