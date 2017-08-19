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
	// "github.com/pkg/profile"
	"os"
	"path/filepath"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/codegangsta/cli"
)

func main() {

	// defer profile.Start(profile.CPUProfile).Stop()

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
		cli.BoolFlag{
			Name:        "scan-dir",
			Usage:       "--scan-dir <true|false>",
			Destination: &gScanDir,
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

	ctx, cancel := context.WithCancel(context.Background())
	handleSignal(cancel)

	configFile, err := os.Open(configPath)
	if err != nil {
		log.WithField("file", configPath).Fatalf(err.Error())
	}

	config := parseConfig(configFile)
	gHostname = config.Hostname
	gAllStreams = configureStreams(ctx, config)

	// TODO: Handle different type of streams correctly
	// stream = NewS3Stream(config.AwsAccessKey,
	// 	config.AwsSecretAccessKey, config.AwsRegion,
	// 	"dc-firehose-logs", "s3_test", 1024, time.Second*60)
	lastState := loadStateFile(gStateFilePath)
	wg := sync.WaitGroup{}

	allFiles := []Logfile{}
	for _, logfile := range config.Logfiles {

		if logfile.Directory != "" {

			// since we will have a monitor, just send the strings to the monitor
			wildcard := logfile.Directory
			var files []string

			// list all files
			if gScanDir {
				matches, err := filepath.Glob(logfile.Directory)
				if err != nil {
					log.Fatal(err.Error())
				}
				for _, m := range matches {
					if isDir, err := IsDir(m); err == nil && !isDir {
						files = append(files, m)
					}

				}
			}

			// directory, monitor it
			logfile.Filename = wildcard
			wg.Add(1)
			go func() {
				defer wg.Done()
				MonitorDir(ctx, logfile, files)
			}()
		} else {
			allFiles = append(allFiles, logfile)
		}

	}

	for _, logfile := range allFiles {

		if savedState, ok := lastState[logfile.Filename]; ok {
			logfile.LastTimestamp = savedState.LastTimestamp
		}

		wg.Add(1)
		go func(l Logfile) {
			defer wg.Done()
			MonitorFile(ctx, l)
		}(logfile)

	}

	go updateStateFileInterval(ctx)
	wg.Wait()
	cancel()

	for streamName, stream := range gAllStreams {
		log.WithField("stream", streamName).Infof("Waiting for stream")
		stream.Close()
	}
}

func configureStreams(ctx context.Context, config ConfigFile) map[string]Streamer {
	// create all streamers from the config

	allStreams := make(map[string]Streamer)
	for streamName, conf := range config.StreamConfigs {

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
