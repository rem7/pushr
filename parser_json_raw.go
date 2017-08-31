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
	"encoding/json"
	"strings"
	"time"
)

type JSONRawParser struct {
	App      string
	AppVer   string
	Filename string
	Hostname string
	Table    []Attribute
}

func NewJSONRawParser(app, appVer, filename, hostname string, defaultTable []Attribute) *JSONRawParser {
	return &JSONRawParser{
		App:      app,
		AppVer:   appVer,
		Filename: filename,
		Hostname: hostname,
		Table:    defaultTable,
	}
}
func (p *JSONRawParser) Init(defaults, fieldMappings map[string]string, FieldsOrder []string, defaultTable []Attribute) {
}
func (p *JSONRawParser) GetTable() []Attribute {
	return p.Table
}

func (p *JSONRawParser) Defaults() map[string]string {

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

func (p *JSONRawParser) Parse(line string) (map[string]string, error) {

	matches := make(map[string]interface{})
	result := make(map[string]string)
	err := json.Unmarshal([]byte(line), &matches)
	if err != nil {
		return result, err
	}

	for k, v := range matches {
		if value, ok := v.(string); ok {
			result[k] = value
		}
	}

	result["event_datetime"] = result["timestamp"] // 2016-07-20T20:59:38.012Z
	result["log_line"] = strings.TrimSpace(line)

	return result, err

}
