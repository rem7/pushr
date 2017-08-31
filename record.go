/*
 * Copyright (c) 2016 Yanko Bolanos
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */
package main

import (
	"bytes"
	"crypto/md5"
	"encoding/csv"
	"strconv"
)

type Streamer interface {
	Stream(*Record) error
	RecordFormat() []Attribute
	Close()
}

type Record struct {
	EventAttributes map[string]string
	recordFormat    []Attribute
	rawLine         string
}

func NewRecord(line string, recordFormat []Attribute, attributes map[string]string) *Record {
	r := Record{
		recordFormat:    recordFormat,
		EventAttributes: attributes,
		rawLine:         line,
	}
	return &r
}

func (r *Record) Hash() []byte {
	h := md5.New()
	h.Write([]byte(r.rawLine))
	return h.Sum(nil)
}

func (r *Record) RecordToCSV() []byte {

	record := []string{}

	for _, attr := range r.recordFormat {
		val := r.EventAttributes[attr.Key]

		if attr.Type == "timestamp" {
		} else if attr.Type == "integer" {
			if _, err := strconv.Atoi(val); err != nil {
				val = "\\N"
			}
		} else if attr.Type == "double" {
			if _, err := strconv.ParseFloat(val, 64); err != nil {
				val = "\\N"
			}
		} else if attr.Type == "string" {
			val = ConvertToUTF8(val, attr.Length)
			val = string(bytes.Replace([]byte(val), []byte("\x00"), nil, -1))
		}

		record = append(record, val)

	}

	var csvData bytes.Buffer
	csvWriter := csv.NewWriter(&csvData)
	csvWriter.Write(record)
	csvWriter.Flush()

	return csvData.Bytes()
}
