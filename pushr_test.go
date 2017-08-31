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
	"testing"
	"time"
	"unicode/utf8"
)

var utcLocation, _ = time.LoadLocation("UTC")
var ProcessLineTestData = []struct {
	Mode             string
	Line             string
	RegexString      string
	TimeFormat       string
	FieldsOrderStr   string
	FieldMappings    map[string]string
	ExpectedRecord   []string
	ExpectedDatetime time.Time
}{
	{
		Mode:             "regex",
		Line:             `172.28.251.156 - - [04/May/2016:13:54:21 +0000] "GET /client/1/channel/70/poll?channel_video_id=74 HTTP/1.1" 200 3311 "http://192.168.1.9:3022/channel/70/video/74" "Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko" 0.123`,
		RegexString:      `^(?P<remote_address>[^ ]*)\ \-\ (?P<remote_user>[^ ]*)\ \[(?P<event_datetime>[^\]]*)\] \"[^\"]*\"\ (?P<log_level>[\d]*)\ (?P<response_bytes>[-\d]*)\ \"(?P<http_referer>[^\"]*)\"\ \"(?P<user_agent>[^\"]*)\"\s?(?P<response_s>[-\d\.]+)?`,
		TimeFormat:       "02/Jan/2006:15:04:05 +0000",
		ExpectedRecord:   []string{`go-testing`, `1.0`, `2016-05-18T19:30:54.267Z`, `2016-05-04T13:54:21Z`, `tester-box`, `test-input`, `200`, `\N`, `\N`, `172.28.251.156`, `3311`, `123.00`, `\N`, `windows`, `6.1`, `ie`, `11.0`, `\N`, `\N`, `- "GET /client/1/channel/70/poll?channel_video_id=74 HTTP/1.1"`},
		ExpectedDatetime: time.Date(2016, 05, 4, 13, 54, 21, 0, utcLocation),
	},
	{
		Mode:             "csv",
		Line:             `go-testing,1.0,\N,1463528404955,\N,\N,ERROR,2ED09219-1361-4777-A3AD-B433F7FD950A,e0c9170c841362ff7df807ea77425c4dd59f,52.39.163.198,\N,\N,"iPhone8,2",\N,\N,\N,\N,\N,\N,[HTTP FAIL] http://prod.honorbound.api.juicebox-games.com/log 502: bad gateway`,
		TimeFormat:       "epochmillisecs",
		ExpectedRecord:   []string{`go-testing`, `1.0`, `2016-05-18T19:30:54.267Z`, `2016-05-17T23:40:04.955Z`, `tester-box`, `test-input`, `ERROR`, `2ED09219-1361-4777-A3AD-B433F7FD950A`, `e0c9170c841362ff7df807ea77425c4dd59f`, `52.39.163.198`, `\N`, `\N`, `\N`, `iPhone8,2`, `\N`, `\N`, `\N`, `\N`, `\N`, `[HTTP FAIL] http://prod.honorbound.api.juicebox-games.com/log 502: bad gateway`},
		ExpectedDatetime: time.Date(2016, 05, 17, 23, 40, 04, 955000000, utcLocation),
		FieldsOrderStr:   "app,app_ver,,event_datetime,,,log_level,device_tag,user_tag,remote_address,,,os,,,,,,,",
	},
	{
		Mode:             "json",
		Line:             `{"timestamp":"2016-05-01T03:09:19.449Z","severity":"WARN","userId":"e777777c6b2b634dc6a4eb52406ebb269bb4","screenName":"rem7","remoteIp":"123.123.123.123","message":"not eligible for w2e  ","country":"mx","state":"none","appVersion":"4.31.13","deviceModel":"iPhone7,2","city":"none","platform":"IPhonePlayer","manifestVersion":"4.41.70","deviceId":"1B77D89A-FFFF-DDDD-AFAF-B770582498DF","firstSession":false,"sessionId":"9jWu9p0hVUWBAuyA2FN4Pw==","logicalVersion":"none","revRecorded":0,"clientSequenceNumber":"none","serverSequenceNumber":"none","serviceInvoked":"none","serviceMethodInvoked":"none","idfa":"none"}`,
		TimeFormat:       "2006-01-02T15:04:05.000Z",
		ExpectedRecord:   []string{`go-testing`, `1.0`, `2016-05-18T19:30:54.267Z`, `2016-05-01T03:09:19.449Z`, `tester-box`, `test-input`, `WARN`, `1B77D89A-FFFF-DDDD-AFAF-B770582498DF`, `e777777c6b2b634dc6a4eb52406ebb269bb4`, `123.123.123.123`, `\N`, `\N`, `iPhone7,2`, `IPhonePlayer`, `\N`, `\N`, `\N`, `mx`, `\N`, `{"appVersion":"4.31.13","city":"none","clientSequenceNumber":"none","firstSession":false,"idfa":"none","logicalVersion":"none","manifestVersion":"4.41.70","message":"not eligible for w2e  ","revRecorded":0,"screenName":"rem7","serverSequenceNumber":"none","serviceInvoked":"none","serviceMethodInvoked":"none","sessionId":"9jWu9p0hVUWBAuyA2FN4Pw==","state":"none"}`},
		ExpectedDatetime: time.Date(2016, 5, 1, 3, 9, 19, 449000000, utcLocation),
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
}

var (
	chromeUA  = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.29 Safari/537.36"
	androidUA = "Dalvik/1.6.0 (Linux; U; Android 4.4.2; GT-N7100 Build/KOT49H)"
	ieUA      = "Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko"
	elbUA     = "ELB-HealthChecker/1.0"
)

var ua_tests = []struct {
	ua                   string
	expected_browser     string
	expected_browser_ver string
	expected_os          string
	expected_os_ver      string
}{
	{chromeUA, "chrome", "51.0.2704.29", "mac", "10.11.4"},
	{androidUA, "android", "\\N", "android", "4.4.2"},
	{ieUA, "ie", "11.0", "windows", "6.3"},
	{elbUA, "aws-elb", "1.0", "aws-elb", "1.0"},
}

func TestLineParse(t *testing.T) {

	for _, p := range ProcessLineTestData {

		gApp = "go-testing"
		gHostname = "tester-box"
		setAppVer("1.0")
		gRecordFormat = defaultAttributes

		l := Logfile{
			Filename:      "test-input",
			TimeFormat:    p.TimeFormat,
			LineRegex:     p.RegexString,
			ParseMode:     p.Mode,
			LastTimestamp: p.ExpectedDatetime.AddDate(0, 0, -1),
		}

		var parseFunction func(string) (map[string]string, error)
		if l.ParseMode == "regex" {
			l.Regex = regexp.MustCompile(p.RegexString)
			parseFunction = func(line string) (map[string]string, error) {
				return parseRegex(l, line)
			}
		} else if l.ParseMode == "json" {
			l.FieldMappings = p.FieldMappings
			parseFunction = func(line string) (map[string]string, error) {
				return parseJson(l, line)
			}
		} else if l.ParseMode == "csv" {
			l.FieldsOrder = parseFieldOrder(p.FieldsOrderStr)
			parseFunction = func(line string) (map[string]string, error) {
				return parseCSV(l, line)
			}
		} else {
			t.Fatalf("%s parse_mode not supported", l.ParseMode)
		}

		record, eventDatetime := processLine(l, parseFunction, p.Line)
		if eventDatetime == nil || !eventDatetime.Equal(p.ExpectedDatetime) {
			t.Fatalf("Time did not match!\nParsed: %s\nExpected: %s", eventDatetime, p.ExpectedDatetime)
		}

		if len(record) != len(gRecordFormat) {
			t.Fatal("Output Record does not match record_format")
		}

		for i := 0; i < len(record); i++ {
			if i == 2 {
				t.Logf("skipping ingest datetime")
				continue
			}
			if p.ExpectedRecord[i] == record[i] && utf8.ValidString(record[i]) {
				t.Logf("PASS %s: '%s' == '%s'", gRecordFormat[i].Key, p.ExpectedRecord[i], record[i])
			} else {
				t.Fatalf("FAIL %s: '%s' != '%s'", gRecordFormat[i].Key, p.ExpectedRecord[i], record[i])
			}

		}
		t.Log("\n")
	}

}

func TestParseBrowser(t *testing.T) {
	for _, ua := range ua_tests {
		browser, browser_ver := parseBrowser(ua.ua)
		if browser != ua.expected_browser || browser_ver != ua.expected_browser_ver {
			t.Errorf("parseBrowser(%s) returned %s,%s, expected: %s,%s",
				ua.ua, browser, browser_ver,
				ua.expected_browser, ua.expected_browser_ver)
		}
	}
}

func TestParseOS(t *testing.T) {
	for _, ua := range ua_tests {
		os, os_ver := parseOS(ua.ua)
		if os != ua.expected_os || os_ver != ua.expected_os_ver {
			t.Errorf("parseOS(%s) returned %s,%s, expected: %s,%s",
				ua.ua, os, os_ver,
				ua.expected_os, ua.expected_os_ver)
		}
	}
}
