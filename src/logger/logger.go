/*
 * Copyright (c) 2016 Yanko Bolanos
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */
package logger

import (
	"bytes"
	"encoding/csv"
	log "github.com/Sirupsen/logrus"
)

const (
	ISO_8601 = "2006-01-02T15:04:05.999Z"
)

type CSVFormatter struct {
}

func (f *CSVFormatter) Format(entry *log.Entry) ([]byte, error) {

	file := ""
	if a, ok := entry.Data["file"].(string); ok {
		file = a
	}

	stream := ""
	if a, ok := entry.Data["stream"].(string); ok {
		stream = a
	}

	record := []string{
		entry.Time.Format(ISO_8601),
		entry.Level.String(),
		file,
		stream,
		entry.Message,
	}

	buf := &bytes.Buffer{}
	w := csv.NewWriter(buf)
	w.Write(record)
	w.Flush()

	return buf.Bytes(), nil
}

// func main() {
// 	log.SetLevel(log.WarnLevel)
// 	log.SetFormatter(new(CSVFormatter))
// 	log.WithFields(log.Fields{"field": "fdsa"}).Info("sup info")
// 	log.WithFields(log.Fields{"field": "fdsa"}).Warn("watchout")
// 	log.WithFields(log.Fields{"field": "fdsa"}).Error("This is error!")
// 	log.WithFields(log.Fields{"field": "fdsa"}).Fatal("NOO")
// 	log.Printf("something?")
// }
