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

type JSONParser struct {
	App           string
	AppVer        string
	Filename      string
	Hostname      string
	FieldMappings map[string]string
	Table         []Attribute
}

func NewJSONParser(app, appVer, filename, hostname string, fieldMappings map[string]string, defaultTable []Attribute) *JSONParser {
	return &JSONParser{
		App:           app,
		AppVer:        appVer,
		Filename:      filename,
		Hostname:      hostname,
		FieldMappings: fieldMappings,
		Table:         defaultTable,
	}
}
func (p *JSONParser) Init(defaults, fieldMappings map[string]string, FieldsOrder []string, defaultTable []Attribute) {
}
func (p *JSONParser) GetTable() []Attribute {
	return p.Table
}
func (p *JSONParser) Defaults() map[string]string {

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

func (p *JSONParser) Parse(line string) (map[string]string, error) {

	matches := make(map[string]interface{})
	result := p.Defaults()
	err := json.Unmarshal([]byte(line), &matches)
	if err != nil {
		return result, err
	}

	for k, v := range p.FieldMappings {
		// result[k] = matches[v]
		if value, ok := matches[v].(string); ok {
			if isNull(value) {
				result[k] = "\\N"
			} else {
				result[k] = value
			}
		}

		delete(matches, v)

	}

	cleanLogLine := line
	if newJson, err := json.Marshal(matches); err == nil {
		cleanLogLine = string(newJson)
	}

	result["log_line"] = strings.TrimSpace(cleanLogLine)

	return result, err

}
