package influxbg

import (
	"time"

	"github.com/influxdata/influxdb/client/v2"
	log "github.com/sirupsen/logrus"
)

func NewInfluxBGWriter(influxClient client.Client, database string) (chan<- *client.Point, error) {
	pointChannel := make(chan *client.Point, 100000)

	go func(pointChannel <-chan *client.Point, influxClient client.Client, database string) {
		bp, err := client.NewBatchPoints(client.BatchPointsConfig{
			Database:  database,
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
			case v := <-pointChannel:
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

				err = influxClient.Write(bp)
				if err != nil {
					log.WithFields(log.Fields{
						"error": err,
					}).Error("writing to influxdb failed")
					continue
				}
				bp, err = client.NewBatchPoints(client.BatchPointsConfig{
					Database:  database,
					Precision: "s",
				})
				if err != nil {
					log.WithFields(log.Fields{
						"error":   err,
						"address": database,
					}).Error("unable to create new influx batch points")
					continue
				}
			}
		}
	}(pointChannel, influxClient, database)

	return pointChannel, nil
}
