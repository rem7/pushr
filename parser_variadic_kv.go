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
	"regexp"
	"strconv"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/iancoleman/strcase"
)

type VariadicKVParser struct {
	App              string
	AppVer           string
	Filename         string
	Hostname         string
	Table            []Attribute
	datePrefixLength int
	cleanKeys        bool
	keyValueRegex    *regexp.Regexp
}

func NewVariadicKVParser(app, appVer, filename, hostname string, re *regexp.Regexp, defaultTable []Attribute, options []string) *VariadicKVParser {
	datePrefixLength := 24
	cleanKeys := false
	parsedOptions := ParseOptions(options)
	for k, v := range parsedOptions {
		if k == "date_prefix_length" {
			var valInt int
			var err error
			if valInt, err = strconv.Atoi(v); err != nil {
				log.Fatalf("invalid date_prefix_length option: %v", v)
			}
			datePrefixLength = valInt
		}
		if k == "clean_keys" && v == "true" {
			cleanKeys = true
		}
	}

	initRE := regexp.MustCompile(`([^=]*)=\"([^\"]*)\"\s?`)
	if re != nil {
		log.Warnf("Using custom regex: %s", re.String())
		initRE = re
	}

	return &VariadicKVParser{
		App:              app,
		AppVer:           appVer,
		Filename:         filename,
		Hostname:         hostname,
		datePrefixLength: datePrefixLength,
		cleanKeys:        cleanKeys,
		keyValueRegex:    initRE,
	}
}

func (p *VariadicKVParser) Init(defaults, fieldMappings map[string]string, FieldsOrder []string, defaultTable []Attribute) {
}

func (p *VariadicKVParser) GetTable() []Attribute {
	return p.Table
}

func (p *VariadicKVParser) Defaults() map[string]string {
	d := make(map[string]string)
	for _, k := range p.Table {
		d[k.Key] = "\\N"
	}

	d["app"] = p.App
	d["app_ver"] = p.AppVer
	d["filename"] = p.Filename
	d["hostname"] = p.Hostname
	d["event"] = "" // init to avoid forcing runtime to increase map capacity in Parse
	d["meta"] = ""
	d["ingest_datetime"] = time.Now().UTC().Format(ISO_8601)

	return d
}

func (p *VariadicKVParser) Parse(line string) (map[string]string, error) {
	if len(line) < 24 {
		return nil, errors.New("failed to parse")
	}
	matches := make(map[string]string)
	result := p.Defaults()

	vals := p.keyValueRegex.FindAllStringSubmatch(line[p.datePrefixLength:], -1)
	if p.cleanKeys {
		for _, item := range vals {
			item[1] = strcase.ToLowerCamel(item[1])
		}
	}
	for _, item := range vals {
		switch len(item) {
		case 3:
			matches[item[1]] = item[2]
		case 0:
			continue
		default:
			for i := 2; i < len(item); i++ {
				if item[i] != "" && item[i] != " " {
					matches[item[1]] = item[i]
				}
			}
			if _, ok := matches[item[1]]; !ok {
				matches[item[1]] = ""
			}
		}
	}

	parsedJson, err := json.Marshal(matches)
	if err == nil {
		result["event"] = string(parsedJson)
	}
	result["log_line"] = truncateString(line, 16777216)

	return result, nil
}
