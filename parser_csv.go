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
	"encoding/csv"
	"log"
	"strings"
	"time"
)

type CSVParser struct {
	App         string
	AppVer      string
	Filename    string
	Hostname    string
	FieldsOrder []string
	Table       []Attribute
}

func NewCSVParser(app, appVer, filename, hostname string, fieldsOrder []string, defaultTable []Attribute) *CSVParser {
	return &CSVParser{
		App:         app,
		AppVer:      appVer,
		Filename:    filename,
		Hostname:    hostname,
		FieldsOrder: fieldsOrder,
		Table:       defaultTable,
	}
}

func (p *CSVParser) Init(defaults, fieldMappings map[string]string, FieldsOrder []string, defaultTable []Attribute) {
}

func (p *CSVParser) GetTable() []Attribute {
	return p.Table
}

func (p *CSVParser) Defaults() map[string]string {

	d := make(map[string]string)
	for _, k := range p.Table {
		d[k.Key] = "\\N"
	}

	d["app"] = p.App
	d["app_ver"] = p.AppVer
	d["filename"] = p.Filename
	d["hostname"] = p.Hostname
	d["ingest_datetime"] = time.Now().UTC().Format(ISO_8601)
	d["event_datetime"] = d["ingest_datetime"]

	return d
}

func (p *CSVParser) Parse(line string) (map[string]string, error) {

	result := p.Defaults()
	r := csv.NewReader(strings.NewReader(line))
	var cleanLogLine bytes.Buffer

	record, err := r.Read()
	if err != nil {
		log.Printf(err.Error())
		return result, err
	}

	if len(record) != len(p.FieldsOrder) {
		return result, ErrCSVFieldsOrderDoNotMatch
	}

	for i, field := range p.FieldsOrder {
		value := record[i]

		skipField := false
		if field == "" {
			skipField = true
		}
		_, ok := result[field]

		if skipField || !ok {
			cleanLogLine.WriteString(value)
			cleanLogLine.WriteString(" ")
		}

		if isNull(value) {
			result[field] = "\\N"
		} else {
			result[field] = value
		}
	}

	srcByte := cleanupPairs.ReplaceAll(cleanLogLine.Bytes(), []byte{})
	srcByte = cleanupSpaces.ReplaceAll(srcByte, []byte{})
	result["log_line"] = strings.TrimSpace(string(srcByte))

	return result, err

}
