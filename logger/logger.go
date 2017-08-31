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

	logfileName := ""
	if a, ok := entry.Data["logfile_name"].(string); ok {
		logfileName = a
	}

	record := []string{
		entry.Time.Format(ISO_8601),
		entry.Level.String(),
		logfileName,
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
