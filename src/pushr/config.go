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
	"os"
	"regexp"
	"time"

	log "github.com/Sirupsen/logrus"
)

type Attribute struct {
	Key    string
	Type   string
	Length int
}

type ConfigFile struct {
	App                string    `yaml:"app" ini:"app"`
	AppVer             string    `yaml:"app_ver" ini:"app_ver"`
	AwsAccessKey       string    `yaml:"aws_access_key" ini:"aws_access_key"`
	AwsSecretAccessKey string    `yaml:"aws_secret_access_key" ini:"aws_secret_access_key"`
	AwsRegion          string    `yaml:"aws_region" ini:"aws_region"`
	Hostname           string    `yaml:"hostname" ini:"hostname"`
	Logfiles           []Logfile `yaml:"files"`
	Streams            []StreamConfig
}

type Logfile struct {
	Name               string            `yaml:"name"`
	Filename           string            `yaml:"file" ini:"file"`
	Directory          string            `yaml:"directory" ini:"directory"`
	StreamName         string            `yaml:"stream" ini:"stream"`
	TimeFormat         string            `yaml:"time_format" ini:"time_format"`
	LineRegex          string            `yaml:"line_regex" ini:"line_regex"`
	FrontSplitRegexStr string            `yaml:"front_split_regex" ini:"front_split_regex"` // option used to split at the begining of the line instead
	ParseMode          string            `yaml:"parse_mode" ini:"parse_mode"`
	RetryFileOpen      bool              `yaml:"retry_file_open" ini:"retry_file_open"`
	FieldMappings      map[string]string `yaml:"field_mappings"`
	BufferMultiLines   bool              `yaml:"buffer_multi_lines" ini:"buffer_multi_lines"`
	FieldsOrder        []string
	FieldsOrderStr     string `ini:"fields_order"`
	LastTimestamp      time.Time
	Regex              *regexp.Regexp
	FrontSplitRegex    *regexp.Regexp
}

type StreamConfig struct {
	StreamName         string      `yaml:"stream_name"`
	Name               string      `yaml:"name" ini:"name"`
	Type               string      `yaml:"type" ini:"type"`
	Url                string      `yaml:"url" ini:"url"`
	StreamApiKey       string      `yaml:"stream_api_key" ini:"stream_api_key"`
	RecordFormatString string      `ini:"record_format"`
	RecordFormat       []Attribute `yaml:"record_format"`
}

var defaultAttributes = []Attribute{
	Attribute{"app", "string", 16},
	Attribute{"app_ver", "string", 16},
	Attribute{"ingest_datetime", "timestamp", 0},
	Attribute{"event_datetime", "timestamp", 0},
	Attribute{"hostname", "string", 64},
	Attribute{"filename", "string", 256},
	Attribute{"log_level", "string", 16},
	Attribute{"device_tag", "string", 64},
	Attribute{"user_tag", "string", 64},
	Attribute{"remote_address", "string", 64},
	Attribute{"response_bytes", "integer", 0},
	Attribute{"response_ms", "double", 0},
	Attribute{"device_type", "string", 32},
	Attribute{"os", "string", 16},
	Attribute{"os_ver", "string", 16},
	Attribute{"browser", "string", 32},
	Attribute{"browser_ver", "string", 16},
	Attribute{"country", "string", 64},
	Attribute{"language", "string", 16},
	Attribute{"log_line", "string", 0},
}

func (c *ConfigFile) GetStream(name string) (*StreamConfig, bool) {
	for _, s := range c.Streams {
		if s.StreamName == name {
			return &s, true
		}
	}
	return nil, false
}

func testParseConfig(configPath string) {

	configFile, err := os.Open(configPath)
	if err != nil {
		log.WithField("file", configPath).Fatalf(err.Error())
	}

	config := parseConfig(configFile)
	log.Printf("%+v", config)
}

func configureStreams(ctx context.Context, config ConfigFile) map[string]Streamer {
	// create all streamers from the config

	allStreams := make(map[string]Streamer)
	for _, conf := range config.Streams {

		streamName := conf.StreamName

		var stream Streamer
		switch {
		case conf.Type == "firehose":
			log.WithField("stream", streamName).Info("streaming to firehose: %s", conf.Name)
			stream = NewFirehoseStream(ctx, conf.RecordFormat, config.AwsAccessKey,
				config.AwsSecretAccessKey, config.AwsRegion, conf.Name)
		case conf.Type == "csv":
			filename := conf.Name + ".csv"
			log.WithField("stream", streamName).Infof("streaming to csv %s", filename)
			stream = NewCSVStream(conf.RecordFormat, filename)
			break
		case conf.Type == "http":
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
