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
	"os"
	"reflect"
	"regexp"
	"strings"
	"testing"
)

var expectedConfig = ConfigFile{
	App:                "my-web-app",
	AppVer:             "1.0",
	AwsAccessKey:       "AAA",
	AwsSecretAccessKey: "ZZZ",
	AwsRegion:          "us-west-2",
	Stream:             "my-firehose",
	RecordFormat: []Attribute{
		{Key: "app", Type: "string", Length: 16},
		{Key: "app_ver", Type: "string", Length: 16},
		{Key: "ingest_datetime", Type: "timestamp", Length: 0},
		{Key: "event_datetime", Type: "timestamp", Length: 0},
		{Key: "hostname", Type: "string", Length: 64},
		{Key: "filename", Type: "string", Length: 256},
		{Key: "log_level", Type: "string", Length: 16},
		{Key: "device_tag", Type: "string", Length: 64},
		{Key: "user_tag", Type: "string", Length: 64},
		{Key: "remote_address", Type: "string", Length: 64},
		{Key: "response_bytes", Type: "integer", Length: 0},
		{Key: "response_ms", Type: "double", Length: 0},
		{Key: "device_type", Type: "string", Length: 32},
		{Key: "os", Type: "string", Length: 16},
		{Key: "os_ver", Type: "string", Length: 16},
		{Key: "browser", Type: "string", Length: 32},
		{Key: "browser_ver", Type: "string", Length: 16},
		{Key: "country", Type: "string", Length: 64},
		{Key: "language", Type: "string", Length: 16},
		{Key: "log_line", Type: "string", Length: 0},
	},
	Logfiles: []Logfile{

		{
			Filename:   "/var/log/nginx/access.log",
			TimeFormat: "02/Jan/2006:15:04:05 +0000",
			LineRegex:  `^(?P<remote_address>[^ ]*)\ \-\ (?P<remote_user>[^ ]*)\ \[(?P<event_datetime>[^\]]*)\] \"[^\"]*\"\ (?P<log_level>[\d]*)\ (?P<response_bytes>[-\d]*)\ \"(?P<http_referer>[^\"]*)\"\ \"(?P<http_user_agent>[^\"]*)\"\s?(?P<response_s>[-\d\.]+)?`,
			ParseMode:  "regex",
			Regex:      regexp.MustCompile(`^(?P<remote_address>[^ ]*)\ \-\ (?P<remote_user>[^ ]*)\ \[(?P<event_datetime>[^\]]*)\] \"[^\"]*\"\ (?P<log_level>[\d]*)\ (?P<response_bytes>[-\d]*)\ \"(?P<http_referer>[^\"]*)\"\ \"(?P<http_user_agent>[^\"]*)\"\s?(?P<response_s>[-\d\.]+)?`),
		},
		{
			Filename:   "/var/log/nginx/error.log",
			TimeFormat: "2006/01/02 15:04:05",
			LineRegex:  `^(?P<event_datetime>^[^ ]*\ [^ ]*)\ \[(?P<log_level>[^\]]*)\]\ [^\:]*\:\ [^ ]*\ .*`,
			ParseMode:  "regex",
			Regex:      regexp.MustCompile(`^(?P<event_datetime>^[^ ]*\ [^ ]*)\ \[(?P<log_level>[^\]]*)\]\ [^\:]*\:\ [^ ]*\ .*`),
		},
		{
			Filename:   "/var/log/node/node.log",
			TimeFormat: "2006-01-02T15:04:05.000Z",
			ParseMode:  "json",
			FieldMappings: map[string]string{
				"log_level":      "severity",
				"event_datetime": "timestamp",
				"remote_address": "remoteIp",
				"device_type":    "deviceModel",
				"device_tag":     "deviceId",
				"user_tag":       "userId",
				"country":        "country",
				"os":             "platform",
			},
		},
	},
}

var configString = `
aws_region=us-west-2
aws_access_key = AAA
aws_secret_access_key = ZZZ
stream = my-firehose
app = my-web-app
app_ver = 1.0

[nginx-access]
file = /var/log/nginx/access.log
parse_mode = regex
line_regex = ^(?P<remote_address>[^ ]*)\ \-\ (?P<remote_user>[^ ]*)\ \[(?P<event_datetime>[^\]]*)\] \"[^\"]*\"\ (?P<log_level>[\d]*)\ (?P<response_bytes>[-\d]*)\ \"(?P<http_referer>[^\"]*)\"\ \"(?P<http_user_agent>[^\"]*)\"\s?(?P<response_s>[-\d\.]+)?
time_format = 02/Jan/2006:15:04:05 +0000

[nginx-error]
file = /var/log/nginx/error.log
parse_mode = regex
line_regex = ^(?P<event_datetime>^[^ ]*\ [^ ]*)\ \[(?P<log_level>[^\]]*)\]\ [^\:]*\:\ [^ ]*\ .*
time_format = 2006/01/02 15:04:05

[jsonapp]
file = /var/log/node/node.log
parse_mode = json
time_format = 2006-01-02T15:04:05.000Z

[jsonapp.field_mappings]
log_level = severity
event_datetime = timestamp
remote_address = remoteIp
device_type = deviceModel
device_tag = deviceId
user_tag = userId
country = country
os = platform

[record_format]
app = string,16
app_ver = string,16
ingest_datetime = timestamp
event_datetime = timestamp
hostname = string,64
filename = string,256
log_level = string,16
device_tag = string,64
user_tag = string,64
remote_address = string,64
response_bytes = integer
response_ms = double
device_type = string,32
os = string,16
os_ver = string,16
browser = string,32
browser_ver = string,16
country = string,64
language = string,16
log_line = string

`

func TestConfigParse(t *testing.T) {

	expectedConfig.Hostname, _ = os.Hostname()

	reader := strings.NewReader(configString)
	config := parseConfig(reader)

	if !reflect.DeepEqual(config, expectedConfig) {
		t.Fatal("recordformat does not match")
	}

}
