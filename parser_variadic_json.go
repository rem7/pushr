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
	"time"
)

type VariadicJSONParser struct {
	App      string
	AppVer   string
	Filename string
	Hostname string
	Table    []Attribute
}

func NewVariadicJSONParser(app, appVer, filename, hostname string, defaultTable []Attribute, options []string) *VariadicJSONParser {
	return &VariadicJSONParser{
		App:      app,
		AppVer:   appVer,
		Filename: filename,
		Hostname: hostname,
	}
}

func (p *VariadicJSONParser) Init(defaults, fieldMappings map[string]string, FieldsOrder []string, defaultTable []Attribute) {
}

func (p *VariadicJSONParser) GetTable() []Attribute {
	return p.Table
}

func (p *VariadicJSONParser) Defaults() map[string]string {
	d := make(map[string]string)
	for _, k := range p.Table {
		d[k.Key] = "\\N"
	}

	d["app"] = p.App
	d["app_ver"] = p.AppVer
	d["filename"] = p.Filename
	d["hostname"] = p.Hostname
	d["event"] = "" // init to avoid forcing runtime to increase map capacity in Parse
	d["ingest_datetime"] = time.Now().UTC().Format(ISO_8601)
	d["log_line"] = ""

	return d
}

func (p *VariadicJSONParser) Parse(line string) (map[string]string, error) {
	result := p.Defaults()
	result["event"] = line
	return result, nil
}
