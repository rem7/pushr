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
	"regexp"
	"strings"
	"time"
)

type RegexParser struct {
	App      string
	AppVer   string
	Filename string
	Hostname string
	re       *regexp.Regexp
	Table    []Attribute
}

func NewRegexParser(app, appVer, filename, hostname string, re *regexp.Regexp, defaultTable []Attribute) *RegexParser {
	return &RegexParser{
		App:      app,
		AppVer:   appVer,
		Filename: filename,
		Hostname: hostname,
		re:       re,
		Table:    defaultTable,
	}
}
func (p *RegexParser) Init(defaults, fieldMappings map[string]string, FieldsOrder []string, defaultTable []Attribute) {
}
func (p *RegexParser) GetTable() []Attribute {
	return p.Table
}

func (p *RegexParser) Defaults() map[string]string {

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

func (p *RegexParser) Parse(line string) (map[string]string, error) {

	var err error = nil
	result := p.Defaults()
	match := p.re.FindStringSubmatch(line)
	if len(match) > 1 {
		for i, name := range p.re.SubexpNames() {
			if i != 0 {
				value := match[i]
				if isNull(value) {
					result[name] = "\\N"
				} else {
					result[name] = value
				}

			}
		}
	} else {
		err = ErrParseNotMatched
	}

	result["log_line"] = strings.TrimSpace(cleanUpLogline(line, p.re))
	return result, err
}
