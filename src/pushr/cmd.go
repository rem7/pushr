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
	"path/filepath"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/codegangsta/cli"
)

func main() {

	var configPath string

	app := cli.NewApp()
	app.Name = "pushr"
	app.Usage = "stream logs to firehose"
	app.Version = gVersion
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:        "config,c",
			Value:       "/etc/pushr.conf",
			Usage:       "--config <file>",
			Destination: &configPath,
		},
		cli.StringFlag{
			Name:        "state,s",
			Value:       "/etc/pushr.state",
			Usage:       "--state <file>",
			Destination: &gStateFilePath,
		},
		cli.IntFlag{
			Name:        "verbose",
			Value:       2,
			Usage:       "--verbose <1|2|3> Default 2, 1. Error, 2. Warn, 3. Info",
			Destination: &gVerboseLevel,
		},
		cli.BoolFlag{
			Name:        "follow",
			Usage:       "--follow <true|false>",
			Destination: &gFollow,
		},
		cli.IntFlag{
			Name:  "limit-days-ago",
			Usage: "--limit-days-ago <number of days>",
			Value: 10,
		},
	}

	app.Action = func(c *cli.Context) error {
		if gVerboseLevel == 1 {
			log.SetLevel(log.ErrorLevel)
		} else if gVerboseLevel == 2 {
			log.SetLevel(log.WarnLevel)
		}

		days := c.Int("limit-days-ago")
		gTimeThreshold = time.Now().UTC().AddDate(0, 0, -days)
		log.Infof("ignoring everything earlier than: %s", gTimeThreshold.Format(ISO_8601))

		start(configPath)
		return nil
	}

	app.Commands = []cli.Command{
		{
			Name:  "convert-regex",
			Usage: "escape regex for JSON config",
			Action: func(c *cli.Context) error {
				escapeRegex()
				return nil
			},
		},
		{
			Name:  "test-time-format",
			Usage: "test golang time format parsing",
			Action: func(c *cli.Context) error {
				testTimeformat()
				return nil
			},
		},
		{
			Name:  "test-regex",
			Usage: "test regular expression agains a string",
			Action: func(c *cli.Context) error {
				testRegexExp()
				return nil
			},
		},
		{
			Name:  "parse-config",
			Usage: "check config",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:        "config,c",
					Value:       "/etc/pushr.conf",
					Usage:       "--config <file>",
					Destination: &configPath,
				},
			},
			Action: func(c *cli.Context) error {
				testParseConfig(configPath)
				return nil
			},
		},
	}

	app.Run(os.Args)

}

func start(configPath string) {

	configFile, err := os.Open(configPath)
	if err != nil {
		log.WithField("file", configPath).Fatalf(err.Error())
	}

	config := parseConfig(configFile)
	gHostname = config.Hostname
	gAllStreams = make(map[string]Streamer)

	// create all streamers from the config
	for streamName, conf := range config.StreamConfigs {

		var stream Streamer
		switch {
		case conf.Type == "firehose":
			log.Warn("stream_type: firehose")
			stream = NewFirehoseStream(conf.RecordFormat, config.AwsAccessKey,
				config.AwsSecretAccessKey, config.AwsRegion, conf.Name)
		case conf.Type == "csv":
			filename := conf.Name + ".csv"
			log.WithField("file", filename).Warn("Streaming to csv")
			stream = NewCSVStream(conf.RecordFormat, filename)
			break
		case conf.Type == "http":
			log.Warn("stream_type: http")
			stream = NewDCHTTPStream(conf.RecordFormat, conf.Url, conf.StreamApiKey, 125000)
			break
		default:
			log.Fatalf("stream type: %s not supported", conf.Type)
		}

		gAllStreams[streamName] = stream

	}

	// TODO: Handle different type of streams correctly
	// stream = NewS3Stream(config.AwsAccessKey,
	// 	config.AwsSecretAccessKey, config.AwsRegion,
	// 	"dc-firehose-logs", "s3_test", 1024, time.Second*60)
	lastState := loadStateFile(gStateFilePath)

	allFiles := []Logfile{}
	for _, logfile := range config.Logfiles {

		if _, err := os.Stat(logfile.Filename); !os.IsNotExist(err) {
			// file exists, add it
			allFiles = append(allFiles, logfile)
		} else {
			// directory, monitor it
			stopCh := make(chan bool)
			gStopChans = append(gStopChans, stopCh)
			wildcard := logfile.Filename
			go MonitorDir(logfile)
			logfile.Filename = filepath.Dir(logfile.Filename)

			// list all files
			matches, err := filepath.Glob(wildcard)
			if err != nil {
				log.Fatal(err.Error())
			}
			for _, m := range matches {

				if isDir, err := IsDir(m); err == nil && !isDir {
					// append all files
					logfile.Filename = m
					allFiles = append(allFiles, logfile)
				}

			}

		}
	}

	for i := 0; i < len(allFiles); i++ {
		log.Printf("%+v", allFiles[i].Filename)
	}

	wg := sync.WaitGroup{}
	for _, logfile := range allFiles {

		if savedState, ok := lastState[logfile.Filename]; ok {
			logfile.LastTimestamp = savedState.LastTimestamp
		}

		wg.Add(1)
		_stopCh := make(chan bool)
		gStopChans = append(gStopChans, _stopCh)

		go func(l Logfile, stopCh chan bool) {

			defer func() {
				wg.Done()
			}()

			log.WithField("file", l.Filename).Warnf("Pushing to Firehose %v", l.StreamName)
			err := MonitorFile(context.Background(), l)
			if err != nil {
				log.WithField("file", l.Filename).Errorf("Error pushing file %v", l.Filename)
			}

		}(logfile, _stopCh)

	}

	go updateStateFileInterval()
	wg.Wait()
	done <- true

}
