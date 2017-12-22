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
	log "github.com/Sirupsen/logrus"
	"strings"
	"time"
	"fmt"
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
		if inf, ok := matches[v]; ok {
			switch t := inf.(type) {
			case nil :
				result[k] = "\\N"
			case string :
				if isNull(inf.(string)) {
					result[k] = "\\N"
				} else {
					result[k] = inf.(string)
				}
			case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64 :
				result[k] = fmt.Sprintf("%9.f", inf)
			case float32, float64, complex64, complex128:
				result[k] = fmt.Sprintf("%9.f", inf)
			case bool:
				result[k] = fmt.Sprintf("%v", inf)
			default:
				result[k] = fmt.Sprintf("%v", inf)
				log.Warnf("Coercing string conversion from non string, numeric or bool value: %v of type: %T in key: %v", inf, t, v)
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
