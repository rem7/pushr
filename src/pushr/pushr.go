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
	"logger"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"tail"
	"time"

	log "github.com/Sirupsen/logrus"
)

const (
	ISO_8601          string = "2006-01-02T15:04:05.999Z"
	MAX_BUFFERED_LINE int    = 65535 // redshift VARCHAR(MAX)
)

var (
	gVersion         = ""
	gHostname        = "HOSTNAME"
	gFollow          = true
	gStopChans       = []chan bool{}
	gApp             string
	gAppVer          string
	gAppVerMutex     *sync.RWMutex
	gUpdateCacheChan chan UpdateMessage
	gStateFilePath   = "/etc/pushr.state"
	gTimeThreshold   time.Time
	gAllStreams      = map[string]Streamer{}
	gVerboseLevel    = 2

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
	gUpdateCacheChan = make(chan UpdateMessage, 1028)
	gAppVerMutex = new(sync.RWMutex)
}

func MonitorFile(ctx context.Context, logfile Logfile) error {

	stream, ok := gAllStreams[logfile.StreamName]
	if !ok {
		errStr := fmt.Sprintf("Stream %s not found to fail file %s", logfile.StreamName, logfile.Filename)
		return errors.New(errStr)
	}

	fastForward := false
	var parser Parser

	if !logfile.LastTimestamp.IsZero() {
		log.WithField("file", logfile.Filename).Warnf("Found cached time of last scan at %s", logfile.LastTimestamp)
		fastForward = true
	}
	if logfile.ParseMode == "regex" {
		parser = NewRegexParser(gApp, appVer(), logfile.Filename, gHostname, logfile.Regex, stream.RecordFormat())
	} else if logfile.ParseMode == "json" {
		parser = NewJSONParser(gApp, appVer(), logfile.Filename, gHostname, logfile.FieldMappings, stream.RecordFormat())
	} else if logfile.ParseMode == "csv" {
		parser = NewCSVParser(gApp, appVer(), logfile.Filename, gHostname, logfile.FieldsOrder, stream.RecordFormat())
	} else if logfile.ParseMode == "json_raw" {
		parser = NewJSONRawParser(gApp, appVer(), logfile.Filename, gHostname, stream.RecordFormat())
	} else if logfile.ParseMode == "date_keyvalue" {
		parser = NewDateKVParser(gApp, appVer(), logfile.Filename, gHostname, logfile.FieldMappings, stream.RecordFormat())
	} else {
		log.WithField("file", logfile.Filename).Fatalf("%s parse_mode not supported", logfile.ParseMode)
	}

	t := tail.NewTailWithCtx(ctx, logfile.Filename, gFollow, logfile.RetryFileOpen)
	stringBuffer := bytes.NewBufferString("")

	flushTimer := time.NewTicker(time.Second * 30)

LOOP:
	for {
		select {
		case <-flushTimer.C:
			if stringBuffer.Len() > 0 {
				log.Printf("flushing...")
				flush(stringBuffer.String(), parser, stream)
				stringBuffer.Reset()
			}
			break
		case line, ok := <-t.LineChan:

			if !ok {
				break LOOP
			}

			record, eventDatetime := processLine(logfile, parser, line, stream.RecordFormat())
			if fastForward && eventDatetime == nil {
				// when fastforwarding skip lines without event_datetime
				continue
			}

			if fastForward && (eventDatetime.Before(logfile.LastTimestamp) || eventDatetime.Equal(logfile.LastTimestamp)) {
				continue
			}

			if eventDatetime != nil && eventDatetime.Before(gTimeThreshold) {
				continue
			}

			if record == nil && stringBuffer.Len() < MAX_BUFFERED_LINE {
				stringBuffer.WriteString(line)
				stringBuffer.WriteString("\\n")
				continue
			}

			fastForward = false

			if (record != nil && stringBuffer.Len() > 0) || stringBuffer.Len() >= MAX_BUFFERED_LINE {
				flush(stringBuffer.String(), parser, stream)
				stringBuffer.Reset()
				if record == nil {
					continue
				}
			}

			err := stream.Stream(record)
			if err != nil {
				log.WithField("file", logfile.Filename).Errorf("Error streaming:\n%s", err.Error())
			}
		}
	}

	log.WithField("file", logfile.Filename).Infof("Reached EOF. follow=%v", gFollow)

	return nil
}

func MonitorDir(ctx context.Context, logfile Logfile, files []string) error {

	monitorDirCtx, monitorCancel := context.WithCancel(ctx)
	newFiles, removedFiles, err := monitorDir(monitorDirCtx, logfile.Directory)
	if err != nil {
		log.WithField("file", logfile.Filename).Errorf("Error monitoring: %s", err.Error())
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
			log.WithField("file", logfile.Filename).Warnf("Stopped monitoring directory")
			monitorCancel()
			break LOOP
		case newFile := <-newFiles:
			logfile.Filename = newFile
			wg.Add(1)
			go func(l Logfile) {
				log.WithField("file", l.Filename).Warnf("Pushing to Firehose %v", l.StreamName)
				ctx, cancel := context.WithCancel(monitorDirCtx)
				ctxs[l.Filename] = cancel
				err := MonitorFile(ctx, l)
				if err != nil {
					log.WithField("file", l.Filename).Errorf("Error pushing file %v", l.Filename)
				}
				wg.Done()
			}(logfile)
			break
		case removedFile := <-removedFiles:
			log.WithField("file", removedFile).Warnf("Removed from filesystem. (finished)")
			if cancel, ok := ctxs[removedFile]; ok {
				cancel()
			}
			break
		case <-ctx.Done():
			log.WithField("file", logfile.Filename).Warnf("MonitorDir quitting.")
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

	var err error
	var eventDatetime *time.Time = nil

	appVerMatches := appVerRegex.FindStringSubmatch(line)
	if len(appVerMatches) > 1 {
		log.WithField("file", logfile.Filename).Warnf("Found app version: %s", appVerMatches[1])
		setAppVer(appVerMatches[1])
	}

	eventAttributes, err := parser.Parse(line)
	if err != nil {
		// log.WithField("file", logfile.Filename).Warnf("Unable to parse line: %s", line)
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
	} else if strings.Index(ua, "CriOS") != -1 {
		browser = "chrome"
		browserVer = regexGet(ua, criVersion)
	} else if strings.Index(ua, "Firefox") != -1 {
		browser = "firefox"
		browserVer = regexGet(ua, firefoxVersion)
	} else if strings.Index(ua, "Android") != -1 {
		browser = "android"
		browserVer = regexGet(ua, androidVersion)
	} else if strings.Index(ua, "Safari") != -1 {
		browser = "safari"
		browserVer = regexGet(ua, safariVersion)
	} else if strings.Index(ua, "Trident") != -1 {
		browser = "ie"
		browserVer = regexGet(ua, ieVersion)
	} else if strings.Index(ua, "MSIE") != -1 {
		browser = "ie"
		browserVer = regexGet(ua, msieVersion)
	} else if strings.Index(ua, "ELB-HealthChecker") != -1 {
		browser = "aws-elb"
		browserVer = regexGet(ua, elbVersion)
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
