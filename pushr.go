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
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/rem7/pushr/logger"
	"github.com/rem7/pushr/tail"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
)

const (
	ISO_8601          string = "2006-01-02T15:04:05.000Z"
	MAX_BUFFERED_LINE int    = 65535 // redshift VARCHAR(MAX)
)

var (
	gVersion         = ""
	gHostname        = "HOSTNAME"
	gFollow          = true
	gScanDir         = true
	gEC2host         = true
	gStopChans       = []chan bool{}
	gApp             string
	gAppVer          string
	gAppVerMutex     *sync.RWMutex
	gUpdateCacheChan chan UpdateMessage
	gStateFilePath   = "/etc/pushr.state"
	gTimeThreshold   time.Time
	gAllStreams      = map[string]Streamer{}
	gVerboseLevel    = 3
	gRecords         chan *Record

	appVerRegex      = regexp.MustCompile(`^----\sapp_ver\:\s(?P<app_ver>.*)$`)
	chromeVersion    = regexp.MustCompile(`Chrome\/([^ ;\)]*)`)
	criVersion       = regexp.MustCompile(`CriOS\/([^ ;)]*)`)
	firefoxVersion   = regexp.MustCompile(`Firefox\/([^ ;)]*)`)
	elbVersion       = regexp.MustCompile(`ELB-HealthChecker\/([^ ;\)]*)`)
	androidVersion   = regexp.MustCompile(`Version\/([^ ;)]*)`)
	safariVersion    = regexp.MustCompile(`Version\/([^ ;)]*)`)
	ieVersion        = regexp.MustCompile(`rv:([^ ;)]*)`)
	msieVersion      = regexp.MustCompile(`MSIE ([^ ;)]*)`)
	windowsNTVersion = regexp.MustCompile(`Windows NT ([^ ;)]*)`)
	iPhoneOSVersion  = regexp.MustCompile(`iPhone OS ([^ ;)]*)`)
	macVersion       = regexp.MustCompile(`Mac OS X ([^ ;)]*)`)
	androidOSVersion = regexp.MustCompile(`Android ([^ ;)]*)`)
	macRepl          = regexp.MustCompile(`\.0$`)
	cleanupPairs     = regexp.MustCompile(`(\[\]|\(\)|-\ |\"\"|\(ms\)|\\N)`)
	cleanupSpaces    = regexp.MustCompile(`\ {2,}`)

	// typechecking
	isISO8601Date = regexp.MustCompile(`^\d{4}\-\d{2}\-\d{2}T\d{2}\:\d{2}\:\d{2}\.\d{3}Z$`)

	// errors
	ErrParseNotMatched          = errors.New("ParseNotMatched")
	ErrCSVFieldsOrderDoNotMatch = errors.New("csv parsed and fieldsOrder do not match")
)

func init() {
	log.SetFormatter(new(logger.CSVFormatter))
	log.SetOutput(os.Stdout)
	gUpdateCacheChan = make(chan UpdateMessage, 1028)
	gAppVerMutex = new(sync.RWMutex)
}

func MonitorFile(ctx context.Context, logfile Logfile) error {

	infof, warnf, errorf, fatalf := LogFuncs(logfile)

	infof("monitoring start")

	stream, ok := gAllStreams[logfile.StreamName]
	if !ok {
		errStr := fmt.Sprintf("stream %s not found to fail file %s", logfile.StreamName, logfile.Filename)
		return errors.New(errStr)
	}

	fastForward := false
	if !logfile.LastTimestamp.IsZero() {
		warnf("found cached time of last scan at %s", logfile.LastTimestamp)
		fastForward = true
	}

	var parser Parser
	switch logfile.ParseMode {
	case "regex":
		parser = NewRegexParser(gApp, appVer(), logfile.Filename, gHostname, logfile.Regex, stream.RecordFormat())
		break
	case "json":
		parser = NewJSONParser(gApp, appVer(), logfile.Filename, gHostname, logfile.FieldMappings, stream.RecordFormat())
		break
	case "csv":
		parser = NewCSVParser(gApp, appVer(), logfile.Filename, gHostname, logfile.FieldsOrder, stream.RecordFormat(), logfile.ParserOptions)
		break
	case "json_raw":
		parser = NewJSONRawParser(gApp, appVer(), logfile.Filename, gHostname, stream.RecordFormat())
		break
	case "date_keyvalue":
		parser = NewDateKVParser(gApp, appVer(), logfile.Filename, gHostname, logfile.FieldMappings, stream.RecordFormat())
		break
	case "plugin":
		defaults := map[string]string{
			"app":      gApp,
			"app_ver":  appVer(),
			"filename": logfile.Filename,
			"hostname": gHostname,
		}
		parser = LoadParserPlugin(logfile.ParserPluginPath)
		parser.Init(defaults, logfile.FieldMappings, logfile.FieldsOrder, stream.RecordFormat())
	default:
		fatalf("%s parse_mode not supported", logfile.ParseMode)
	}

	// delim := regexp.MustCompile(`\d{4}/\d{2}/\d{2}\s\d{2}\:\d{2}\:\d{2}\.\d{3}\s`)
	var t *tail.Tail
	if logfile.FrontSplitRegexStr != "" {
		t = tail.NewTailWithCtx(ctx, logfile.Filename, gFollow, logfile.RetryFileOpen, logfile.FrontSplitRegex, true)
	} else {
		t = tail.NewTailWithCtx(ctx, logfile.Filename, gFollow, logfile.RetryFileOpen, nil, false)
	}

	stringBuffer := bytes.NewBufferString("")
	flushTimer := time.NewTicker(time.Second * 30)
	var streamed_lines_ctr uint64 = 0
	var lines_ctr uint64 = 0
	bufferMultiLines := logfile.BufferMultiLines
	skipHeader := false
	if logfile.SkipHeaderLine {
		skipHeader = true
	}

LOOP:
	for {
		select {
		case <-flushTimer.C:
			if stringBuffer.Len() > 0 {
				infof("flushing...")
				flush(stringBuffer.String(), parser, stream)
				stringBuffer.Reset()
			}
			break
		case line, ok := <-t.LineChan:

			if !ok {
				break LOOP
			}

			if skipHeader {
				skipHeader = false
				continue
			}

			lines_ctr += 1

			record, eventDatetime := processLine(logfile, parser, line, stream.RecordFormat())
			if fastForward && eventDatetime == nil {
				// when fastforwarding skip lines without event_datetime
				// log.Printf("skip 1")
				continue
			}

			if fastForward && (eventDatetime.Before(logfile.LastTimestamp) || eventDatetime.Equal(logfile.LastTimestamp)) {
				// log.Printf("skip 2")
				continue
			}

			if eventDatetime != nil && eventDatetime.Before(gTimeThreshold) {
				// log.Printf("skip 3")
				continue
			}

			if bufferMultiLines {
				// bufferMultiLines adds the lines that couldn't be parsed to a buffer
				// and it will stream the buffer once a line has been able to be parsed
				// or if the MAX_BUFFERED_LINE is reached.
				if record == nil && stringBuffer.Len() < MAX_BUFFERED_LINE {
					stringBuffer.WriteString(line)
					stringBuffer.WriteString("\\n")
					// log.Printf("skip 4")
					continue
				}

			} else if record == nil && eventDatetime == nil { // this means that processLine could not parse the line
				errorf("unable to parse line %d: %s", lines_ctr, line)
				// log.Printf("skip 5")
				continue
			}

			fastForward = false

			if bufferMultiLines {
				if (record != nil && stringBuffer.Len() > 0) || stringBuffer.Len() >= MAX_BUFFERED_LINE {
					flush(stringBuffer.String(), parser, stream)
					stringBuffer.Reset()
					if record == nil {
						// log.Printf("skip 6")
						continue
					}
				}
			}

			err := stream.Stream(record)
			if err != nil {
				errorf("error streaming:\n%s", err.Error())
			}
			streamed_lines_ctr += 1
		}
	}

	infof("monitoring stop. streamed %d lines of %d", streamed_lines_ctr, lines_ctr)

	return nil
}

func MonitorDir(ctx context.Context, logfile Logfile, files []string) error {

	infof, _, errorf, _ := LogFuncs(logfile)
	infof("monitoring dir start")

	monitorDirCtx, monitorCancel := context.WithCancel(ctx)
	newFiles, removedFiles, err := monitorDir(monitorDirCtx, logfile.Directory)
	if err != nil {
		errorf("Error monitoring: %s", err.Error())
		return err
	}

	wg := sync.WaitGroup{}
	ctxs := make(map[string]context.CancelFunc)

	if len(files) > 0 {
		go func() {
			for _, file := range files {
				newFiles <- file
			}
		}()
	}

LOOP:
	for {
		select {
		case <-monitorDirCtx.Done():
			infof("Stopped monitoring directory")
			monitorCancel()
			break LOOP
		case newFile := <-newFiles:
			logfile.Filename = newFile
			ctx, cancel := context.WithCancel(monitorDirCtx)
			ctxs[logfile.Filename] = cancel
			wg.Add(1)
			go func(l Logfile) {
				MonitorFile(ctx, l)
				wg.Done()
			}(logfile)
			break
		case removedFile := <-removedFiles:
			if cancel, ok := ctxs[removedFile]; ok {
				log.WithField("file", removedFile).
					WithField("stream", logfile.StreamName).
					Info("removed from filesystem.")
				cancel()
			}
			break
		case <-ctx.Done():
			infof("Context Stopped triggered. Stopped monitoring directory")
			break LOOP
		}
	}

	wg.Wait()
	return nil
}

func flush(data string, parser Parser, stream Streamer) error {

	m := parser.Defaults()
	r := NewRecord(data, parser.GetTable(), m)
	m["log_line"] = data
	err := stream.Stream(r)
	return err

}

func processLine(logfile Logfile, parser Parser, line string, recordFormat []Attribute) (*Record, *time.Time) {

	infof, _, _, _ := LogFuncs(logfile)

	var err error
	var eventDatetime *time.Time = nil

	appVerMatches := appVerRegex.FindStringSubmatch(line)
	if len(appVerMatches) > 1 {
		infof("Found app version: %s", appVerMatches[1])
		setAppVer(appVerMatches[1])
	}

	eventAttributes, err := parser.Parse(line)
	if err != nil {
		eventAttributes = parser.Defaults()
		eventAttributes["log_line"] = line
		return nil, nil
	}

	if val_float, err := strconv.ParseFloat(eventAttributes["response_s"], 64); err == nil {
		eventAttributes["response_ms"] = fmt.Sprintf("%.2f", val_float*1000)
	}

	userAgent := eventAttributes["user_agent"]
	if eventAttributes["browser"] == "\\N" {
		eventAttributes["browser"], eventAttributes["browser_ver"] = parseBrowser(userAgent)
	}
	if eventAttributes["os"] == "\\N" {
		eventAttributes["os"], eventAttributes["os_ver"] = parseOS(userAgent)
	}

	stringTimestamp := eventAttributes["event_datetime"]
	eventDatetime, err = parseTimestamp(stringTimestamp, logfile.TimeFormat)
	if err != nil {
		delete(eventAttributes, "event_datetime")
	} else {
		eventAttributes["event_datetime"] = eventDatetime.Format(ISO_8601)
	}

	if eventDatetime != nil {
		logfile.LastTimestamp = *eventDatetime
		gUpdateCacheChan <- UpdateMessage{logfile.Filename, *eventDatetime}
	}

	if _, ok := eventAttributes["event_datetime"]; !ok {
		eventAttributes["event_datetime"] = eventAttributes["ingest_datetime"]
	}

	r := NewRecord(line, recordFormat, eventAttributes)

	return r, eventDatetime
}

func parseTimestamp(stringTimestamp, timeFormat string) (*time.Time, error) {

	var eventDatetime *time.Time = nil

	var parsedTime time.Time
	if timeFormat == "epochmillisecs" {
		if val_ms, err := strconv.ParseInt(stringTimestamp, 10, 64); err == nil {
			val_ns := val_ms * 1000000
			parsedTime = time.Unix(0, val_ns).UTC()
		}
	} else {
		var err error
		parsedTime, err = time.Parse(timeFormat, stringTimestamp)
		if err != nil {
			return nil, err
		}
	}

	if !parsedTime.IsZero() {
		eventDatetimeUTC := parsedTime.UTC()
		eventDatetime = &eventDatetimeUTC
	}

	return eventDatetime, nil
}

func parseBrowser(ua string) (string, string) {

	browser := "\\N"
	browserVer := "\\N"

	if strings.Index(ua, "Chrome") != -1 {
		browser = "chrome"
		browserVer = regexGet(ua, chromeVersion)
		return browser, browserVer
	} else if strings.Index(ua, "CriOS") != -1 {
		browser = "chrome"
		browserVer = regexGet(ua, criVersion)
		return browser, browserVer
	} else if strings.Index(ua, "Firefox") != -1 {
		browser = "firefox"
		browserVer = regexGet(ua, firefoxVersion)
		return browser, browserVer
	} else if strings.Index(ua, "Android") != -1 {
		browser = "android"
		browserVer = regexGet(ua, androidVersion)
		return browser, browserVer
	} else if strings.Index(ua, "Safari") != -1 {
		browser = "safari"
		browserVer = regexGet(ua, safariVersion)
		return browser, browserVer
	} else if strings.Index(ua, "Trident") != -1 {
		browser = "ie"
		browserVer = regexGet(ua, ieVersion)
		return browser, browserVer
	} else if strings.Index(ua, "MSIE") != -1 {
		browser = "ie"
		browserVer = regexGet(ua, msieVersion)
		return browser, browserVer
	} else if strings.Index(ua, "ELB-HealthChecker") != -1 {
		browser = "aws-elb"
		browserVer = regexGet(ua, elbVersion)
		return browser, browserVer
	} else if strings.Index(ua, "Mozilla") != -1 && strings.Index(ua, "AppleWebKit") != -1 {
		browser = "ios_cna"
		return browser, browserVer
	}

	return browser, browserVer

}

func parseOS(ua string) (string, string) {

	os := "\\N"
	osVer := "\\N"

	if strings.Index(ua, "Win") != -1 {
		os = "windows"
		osVer = regexGet(ua, windowsNTVersion)
	} else if strings.Index(ua, "iPhone OS") != -1 {
		os = "ios"
		osVer = regexGet(ua, iPhoneOSVersion)
		osVer = strings.Replace(osVer, "_", ".", -1)
	} else if strings.Index(ua, "Mac OS X") != -1 {
		os = "mac"
		osVer = regexGet(ua, macVersion)
		osVer = strings.Replace(osVer, "_", ".", -1)
		osVer = macRepl.ReplaceAllString(osVer, "")
	} else if strings.Index(ua, "Android") != -1 {
		os = "android"
		osVer = regexGet(ua, androidOSVersion)
		osVer = strings.Replace(osVer, "_", ".", -1)
	} else if strings.Index(ua, "X11") != -1 {
		os = "unix"
	} else if strings.Index(ua, "Linux") != -1 {
		os = "linux"
	} else if strings.Index(ua, "ELB-HealthChecker") != -1 {
		os = "aws-elb"
		osVer = regexGet(ua, elbVersion)
	}

	return os, osVer
}

func cleanUpLogline(src string, r *regexp.Regexp) string {

	srcByte := []byte(src)

	vals := r.FindAllSubmatchIndex(srcByte, -1)
	if len(vals) > 0 {
		idxs := vals[0]
		for i := 2; i < len(idxs)-1; i += 2 {
			for j := idxs[i]; j < idxs[i+1]; j++ {
				srcByte[j] = 0x20
			}
		}
	}

	srcByte = cleanupSpaces.ReplaceAll(srcByte, []byte{})
	srcByte = cleanupPairs.ReplaceAll(srcByte, []byte{})

	return string(srcByte)
}
