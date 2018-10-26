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
	"context"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
)

type Attribute struct {
	Key                        string `yaml:"key"`
	Type                       string `yaml:"type"`
	Length                     int    `yaml:"length"`
	SourceTimestampFormat      string `yaml:"source_ts_fmt"`
	DestinationTimestampFormat string `yaml:"destination_ts_fmt"`
}

type ConfigFile struct {
	App                string    `yaml:"app" ini:"app"`
	AppVer             string    `yaml:"app_ver" ini:"app_ver"`
	AwsAccessKey       string    `yaml:"aws_access_key" ini:"aws_access_key"`
	AwsSecretAccessKey string    `yaml:"aws_secret_access_key" ini:"aws_secret_access_key"`
	AwsRegion          string    `yaml:"aws_region" ini:"aws_region"`
	AwsSTSRole         string    `yaml:"aws_sts_role"`
	EC2Host            bool      `yaml:"ec2host"`
	Hostname           string    `yaml:"hostname" ini:"hostname"`
	Logfiles           []Logfile `yaml:"files"`
	Streams            []StreamConfig
	Server             LiveServerConfig `yaml:"live_server"`
}

type Logfile struct {
	Name               string            `yaml:"name" json:"name"`
	Filename           string            `yaml:"file" ini:"file" json:"file"`
	Directory          string            `yaml:"directory" ini:"directory" json:"directory"`
	StreamName         string            `yaml:"stream" ini:"stream" json:"stream"`
	TimeFormat         string            `yaml:"time_format" ini:"time_format" json:"time_format"`
	LineRegex          string            `yaml:"line_regex" ini:"line_regex"  json:"line_regex"`
	FrontSplitRegexStr string            `yaml:"front_split_regex" ini:"front_split_regex"  json:"front_split_regex,omitempty"` // option used to split at the begining of the line instead
	ParseMode          string            `yaml:"parse_mode" ini:"parse_mode"json:"parse_mode"`
	ParserOptions      []string          `yaml:"parser_options"`
	RetryFileOpen      bool              `yaml:"retry_file_open" ini:"retry_file_open" json:"retry_file_open,omitempty"`
	FieldMappings      map[string]string `yaml:"field_mappings" json:"field_mappings,omitempty"`
	BufferMultiLines   bool              `yaml:"buffer_multi_lines" ini:"buffer_multi_lines" json:"buffer_multi_lines,omitempty"`
	FieldsOrder        []string          `yaml:"fields_order" json:"fields_order,omitempty"`
	FieldsOrderStr     string            `ini:"fields_order" json:"-"`
	ParserPluginPath   string            `yaml:"parser_plugin_path"`
	LastTimestamp      time.Time         `json:"-"`
	Regex              *regexp.Regexp    `json:"-"`
	FrontSplitRegex    *regexp.Regexp    `json:"-"`
	SkipHeaderLine     bool              `yaml:"skip_header_line"`
	SkipToEnd          bool              `yaml:"skip_to_end"`
}

type StreamConfig struct {
	StreamName         string      `yaml:"stream_name"`
	Name               string      `yaml:"name" ini:"name"`
	Type               string      `yaml:"type" ini:"type"`
	Url                string      `yaml:"url" ini:"url"`
	StreamApiKey       string      `yaml:"stream_api_key" ini:"stream_api_key"`
	RecordFormatString string      `ini:"record_format"`
	RecordFormat       []Attribute `yaml:"record_format"`
	Options            []string    `yaml:"options"`
}

type LiveServerConfig struct {
	Enabled bool     `yaml:"enabled"`
	Port    int      `yaml:"port"`
	ApiKeys []string `yaml:"api_keys"`
}

var defaultAttributes = []Attribute{
	{"app", "string", 16, "", ""},
	{"app_ver", "string", 16, "", ""},
	{"ingest_datetime", "timestamp", 0, "", ""},
	{"event_datetime", "timestamp", 0, "", ""},
	{"hostname", "string", 64, "", ""},
	{"filename", "string", 256, "", ""},
	{"log_level", "string", 16, "", ""},
	{"device_tag", "string", 64, "", ""},
	{"user_tag", "string", 64, "", ""},
	{"remote_address", "string", 64, "", ""},
	{"response_bytes", "integer", 0, "", ""},
	{"response_ms", "double", 0, "", ""},
	{"device_type", "string", 32, "", ""},
	{"os", "string", 16, "", ""},
	{"os_ver", "string", 16, "", ""},
	{"browser", "string", 32, "", ""},
	{"browser_ver", "string", 16, "", ""},
	{"country", "string", 64, "", ""},
	{"language", "string", 16, "", ""},
	{"log_line", "string", 0, "", ""},
}

func (c *ConfigFile) GetStream(name string) (*StreamConfig, bool) {
	for i := 0; i < len(c.Streams); i++ {
		if c.Streams[i].StreamName == name {
			return &c.Streams[i], true
		}
	}
	return nil, false
}

func parseConfig(configPath string) ConfigFile {

	configFile, err := os.Open(configPath)
	if err != nil {
		log.WithField("file", configPath).Fatalf(err.Error())
	}
	defer configFile.Close()

	var config ConfigFile
	if strings.Contains(configPath, ".yaml") {
		log.WithField("file", configPath).Infof("loading yaml config")
		config = parseYamlConfig(configFile)
	} else {
		log.WithField("file", configPath).Infof("loading ini config")
		config = parseConfigINI(configFile)
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

	if err := config.validate(); err != nil {
		log.Fatalf("Error validating config. %v", err)
	}

	gHostname = config.Hostname
	config.EC2Host = gEC2host

	return config
}

func (config *ConfigFile) validate() error {

	for _, stream := range config.Streams {
		for _, recordFormat := range stream.RecordFormat {
			switch recordFormat.Type {
			case "timestamp":
				return recordFormat.validateTimestampFormat()
			}
		}
	}
	return nil
}

func (attr *Attribute) validateTimestampFormat() error {
	if attr.SourceTimestampFormat == "" && attr.DestinationTimestampFormat != "" {
		return fmt.Errorf("must specify Source Timestamp Format when datatype is Timestamp")
	}
	if attr.SourceTimestampFormat != "" && attr.DestinationTimestampFormat == "" {
		return fmt.Errorf("must specify Source & Destination Timestamp Format when datatype is Timestamp")
	}

	fullDatetime := time.Date(2008, time.March, 15, 1, 2, 3, 4, time.UTC)
	shortDatetime := time.Date(2008, time.March, 15, 0, 0, 0, 0, time.UTC)

	if attr.SourceTimestampFormat != "" {
		timeFormatted := fullDatetime.Format(attr.SourceTimestampFormat)
		timeReParsed, err := time.Parse(attr.SourceTimestampFormat, timeFormatted)

		if err != nil || (fullDatetime.Unix() != timeReParsed.Unix() && shortDatetime.Unix() != timeReParsed.Unix()) {
			return fmt.Errorf("source timestamp format is invalid " + attr.SourceTimestampFormat)
		}
	}

	if attr.DestinationTimestampFormat != "" {
		timeFormatted := fullDatetime.Format(attr.DestinationTimestampFormat)
		timeReParsed, err := time.Parse(attr.DestinationTimestampFormat, timeFormatted)

		if err != nil || (fullDatetime.Unix() != timeReParsed.Unix() && shortDatetime.Unix() != timeReParsed.Unix()) {
			return fmt.Errorf("destination timestamp format is invalid " + attr.DestinationTimestampFormat)
		}
	}
	return nil
}

func testParseConfig(configPath string) {
	config := parseConfig(configPath)
	log.Printf("%+v", config)
}

func configureStreams(ctx context.Context, config ConfigFile) map[string]Streamer {
	// create all streamers from the config

	allStreams := make(map[string]Streamer)
	for _, conf := range config.Streams {

		streamName := conf.StreamName

		var stream Streamer
		switch conf.Type {
		case "firehose":
			log.WithField("stream", streamName).Infof("streaming to firehose: %s", conf.Name)
			stream = NewFirehoseStream(ctx, conf.RecordFormat, config.AwsAccessKey,
				config.AwsSecretAccessKey, config.AwsRegion, config.AwsSTSRole, conf.Name)
		case "s3":
			log.WithField("stream", streamName).Info("streaming to s3")
			stream = NewS3Stream(ctx, conf.RecordFormat, config.AwsAccessKey,
				config.AwsSecretAccessKey, config.AwsRegion, config.AwsSTSRole, conf.Name, conf.Options)
		case "csv":
			filename := conf.Name + ".csv"
			log.WithField("stream", streamName).Infof("streaming to csv %s", filename)
			stream = NewCSVStream(conf.RecordFormat, filename)
			break
		case "http":
			log.WithField("stream", streamName).Info("streaming to http")
			stream = NewDCHTTPStream(conf.RecordFormat, conf.Url, conf.StreamApiKey, 125000)
			break
		default:
			log.Fatalf("stream type: %s not supported", conf.Type)
		}

		allStreams[streamName] = stream

	}

	return allStreams
}
