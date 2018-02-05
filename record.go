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
	//"log"
	log "github.com/Sirupsen/logrus"
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
	var convVal interface{}
	var err error

	for _, attr := range r.recordFormat {

		val := r.EventAttributes[attr.Key]
		switch {
		case attr.Key == "_uuid":
			convVal, _ = GenerateUUID()

		case val == "\\N" :
			convVal = "\\N"

		case attr.Type == "string", attr.Type == "timestamp":
			if isNull(val) {
				convVal = "\\N"
			} else {
				convVal = ConvertToUTF8(val, attr.Length)
				convVal = string(bytes.Replace([]byte(convVal.(string)), []byte("\x00"), nil, -1))
			}

		case attr.Type == "integer":
			if convVal, err = strconv.Atoi(val); err != nil {
				convVal = "\\N"
				log.Warnf("conversion err '%s', to %s", val, attr.Type)
			} else {
				convVal = strconv.Itoa(convVal.(int))
			}

		case attr.Type == "float32":
			if convVal, err = strconv.ParseFloat(val, 64); err != nil {
				convVal = "\\N"
				log.Warnf("conversion err '%s', to %s: %s", val, attr.Type, err.Error())
			} else {
				convVal = strconv.FormatFloat(convVal.(float64), 'f', -1, 32)
			}

		case attr.Type == "float64", attr.Type =="double":
			if convVal, err = strconv.ParseFloat(val, 64); err != nil {
				convVal = "\\N"
				log.Warnf("conversion err '%s', to %s: %s", val, attr.Type, err.Error())
			} else {
				convVal = strconv.FormatFloat(convVal.(float64), 'f', -1, 64)
			}

		case attr.Type == "bool":
			if convVal, err = strconv.ParseBool(val); err != nil {
				convVal = "\\N"
				log.Warnf("conversion err '%s', to %s", val, attr.Type)
			} else {
				convVal = strconv.FormatBool(convVal.(bool))
			}

		default:
			convVal = "\\N"
			log.Warnf("unknown attritbute type: %s", attr.Type)

		}

		record = append(record, convVal.(string))
	}

	var csvData bytes.Buffer
	csvWriter := csv.NewWriter(&csvData)
	csvWriter.Write(record)
	csvWriter.Flush()

	return csvData.Bytes()
}

