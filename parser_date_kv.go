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
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

var keyValueRegex = regexp.MustCompile(`([^=]*)=\"([^\"]*)\"\s?`)

type DateKVParser struct {
	App           string
	AppVer        string
	Filename      string
	Hostname      string
	FieldMappings map[string]string
	Table         []Attribute
	LogLineFormat string
}

func NewDateKVParser(app, appVer, filename, hostname string, fieldMappings map[string]string, defaultTable []Attribute, options []string) *DateKVParser {

	logLineFmt := "kv"
	parsedOptions := ParseOptions(options)
	for k, v := range parsedOptions {
		if k == "log_line_format" {
			if v == "json" {
				logLineFmt = v
			}
		}
	}

	return &DateKVParser{
		App:           app,
		AppVer:        appVer,
		Filename:      filename,
		Hostname:      hostname,
		FieldMappings: fieldMappings,
		Table:         defaultTable,
		LogLineFormat: logLineFmt,
	}
}
func (p *DateKVParser) Init(defaults, fieldMappings map[string]string, FieldsOrder []string, defaultTable []Attribute) {
}

func (p *DateKVParser) GetTable() []Attribute {
	return p.Table
}
func (p *DateKVParser) Defaults() map[string]string {

	d := make(map[string]string)
	for _, k := range p.Table {
		d[k.Key] = "\\N"
	}

	d["app"] = p.App
	d["app_ver"] = p.AppVer
	d["filename"] = p.Filename
	d["hostname"] = p.Hostname
	d["ingest_datetime"] = time.Now().UTC().Format(ISO_8601)

	return d
}

func (p *DateKVParser) Parse(line string) (map[string]string, error) {

	matches := make(map[string]string)
	result := p.Defaults()

	if len(line) < 24 {
		return nil, errors.New("failed to parse")
	}

	vals := keyValueRegex.FindAllStringSubmatch(line[24:], -1)
	for _, item := range vals {
		matches[item[1]] = item[2]
	}

	for k, v := range p.FieldMappings {
		if value, ok := matches[v]; ok {
			if isNull(value) {
				result[k] = "\\N"
			} else {
				result[k] = value
			}
		}
		delete(matches, v)
	}

	cleanLogLine := ""
	switch p.LogLineFormat {
	case "kv":
		for k, v := range matches {
			cleanLogLine = fmt.Sprintf("%s %s=%s", cleanLogLine, k, v)
		}
	case "json":
		if json, err := json.Marshal(matches); err == nil {
			cleanLogLine = string(json)
		}
	}
	result["log_line"] = strings.TrimSpace(cleanLogLine)

	return result, nil
}
