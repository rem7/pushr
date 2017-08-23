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
	"fmt"
	"gopkg.in/ini.v1"
	"io"
	"io/ioutil"
	"os"
	"regexp"
	"strconv"
	"strings"

	log "github.com/Sirupsen/logrus"
)

var typeLengthRegex = regexp.MustCompile(`^([^,]*),?(.*)?`)
var skipSections = regexp.MustCompile(`(record_format|DEFAULT|\w+\.\w)`)                      // only for parsing loglifes
var streamRecordFormatSection = regexp.MustCompile(`^stream\.(?P<stream>.*)\.record_format$`) // only streams record format

func parseConfig(src io.Reader) ConfigFile {

	data, err := ioutil.ReadAll(src)
	if err != nil {
		log.Fatalf(err.Error())
	}

	cfg, err := ini.Load(data)
	if err != nil {
		log.Fatal(err.Error())
	}

	var config ConfigFile
	err = cfg.Section("DEFAULT").MapTo(&config)
	if err != nil {
		log.Fatal(err.Error())
	}

	sections := cfg.SectionStrings()

	for _, section := range sections {
		matches := streamRecordFormatSection.FindStringSubmatch(section)
		if len(matches) > 1 {
			log.Printf("Parsing streamSection: %s", section)
			var stream StreamConfig
			stream.StreamName = matches[1]
			err := cfg.Section(section).MapTo(&stream)
			if err != nil {
				log.Fatal(err.Error())
			}
			config.Streams = append(config.Streams, stream)
		}
	}

	for _, section := range sections {
		matches := streamRecordFormatSection.FindStringSubmatch(section)
		if len(matches) > 1 {
			streamName := matches[1]
			if stream, ok := config.GetStream(streamName); ok {
				stream.RecordFormat = parseRecordFormat(cfg, section)
				log.Infof("Parsing streamRecordFormatSection: %s", section)
				for i := 0; i < len(stream.RecordFormat); i++ {
					log.Infof("%v -> %+v", stream.Name, stream.RecordFormat[i])
				}
			} else {
				log.Fatalf("stream not found. %s", streamName)
			}

		}
	}

	for _, section := range sections {
		if !skipSections.MatchString(section) {
			logfile := parseLogfileSection(cfg, section)
			config.Logfiles = append(config.Logfiles, logfile)
		}
	}

	gApp = config.App
	setAppVer(config.AppVer)

	if config.Hostname == "" {
		var err error
		config.Hostname, err = os.Hostname()
		if err != nil {
			log.Fatalf("Error getting hostname. %v", err)
		}
	}

	return config

}

func parseLogfileSection(cfg *ini.File, sectionName string) Logfile {

	var n Logfile
	err := cfg.Section(sectionName).MapTo(&n)
	if err != nil {
		log.Fatal(err.Error())
	}

	if n.FrontSplitRegexStr != "" {
		n.FrontSplitRegex = regexp.MustCompile(n.FrontSplitRegexStr)
	}

	if n.ParseMode == "regex" {
		n.Regex = regexp.MustCompile(n.LineRegex)
	} else if n.ParseMode == "json" || n.ParseMode == "date_keyvalue" {
		subsectionName := fmt.Sprintf("%s.field_mappings", sectionName)
		subsection, err := cfg.GetSection(subsectionName)
		if err != nil {
			log.Fatalf("json needs subsection with field_mappings")
		}
		keyNames := subsection.KeyStrings()
		keyValues := subsection.Keys()
		n.FieldMappings = make(map[string]string, len(keyNames))

		for i := 0; i < len(keyNames); i++ {
			n.FieldMappings[keyNames[i]] = keyValues[i].String()
		}
	} else if n.ParseMode == "csv" {
		n.FieldsOrder = parseFieldOrder(n.FieldsOrderStr)
		if len(n.FieldsOrder) == 0 {
			log.Fatalf("csv needs subsection with field_mappings")
		}
	}

	return n

}

func parseFieldOrder(fieldOrder string) []string {
	return strings.Split(fieldOrder, ",")
}

func parseRecordFormat(cfg *ini.File, section string) []Attribute {

	recordFormat := []Attribute{}
	record, err := cfg.GetSection(section)
	if err != nil {
		return defaultAttributes
	}

	keyNames := record.KeyStrings()
	keyValues := record.Keys()

	for i := 0; i < len(keyNames); i++ {
		typeLength := typeLengthRegex.FindStringSubmatch(keyValues[i].String())
		val := Attribute{Key: keyNames[i]}
		if len(typeLength) > 0 {
			val.Type = typeLength[1]
			val.Length, _ = strconv.Atoi(typeLength[2])
		}
		recordFormat = append(recordFormat, val)
	}

	return recordFormat

}
