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
	"bufio"
	"bytes"
	"io"
	"io/ioutil"
	"regexp"

	log "github.com/Sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

func parseYamlConfig(src io.Reader) ConfigFile {

	data, err := ioutil.ReadAll(src)
	if err != nil {
		log.Fatalf(err.Error())
	}

	config := ConfigFile{}
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	// scan for regexes
	exp1 := regexp.MustCompile(`line_regex\:\s?\#(?P<logfile>[^\s]+)\s(?P<exp>.*)`)
	exp2 := regexp.MustCompile(`front_split_regex\:\s?\#(?P<logfile>[^\s]+)\s(?P<exp>.*)`)
	r := bufio.NewReader(bytes.NewReader(data))
	for {

		line, _, err := r.ReadLine()
		if err != nil && err == io.EOF {
			break
		} else if err != nil {
			log.Fatalf(err.Error())
		}

		matches := exp1.FindSubmatch(line)
		if len(matches) == 3 {
			logfileName := string(matches[1])
			expstr := string(matches[2])
			exp := regexp.MustCompile(expstr)
			for n, logfile := range config.Logfiles {
				if logfile.Name == logfileName {
					config.Logfiles[n].Regex = exp
					config.Logfiles[n].LineRegex = expstr
				}
			}
		}

		matches = exp2.FindSubmatch(line)
		if len(matches) == 3 {
			logfileName := string(matches[1])
			expstr := string(matches[2])
			exp := regexp.MustCompile(expstr)
			for n, logfile := range config.Logfiles {
				if logfile.Name == logfileName {
					config.Logfiles[n].FrontSplitRegex = exp
					config.Logfiles[n].FrontSplitRegexStr = expstr
				}
			}
		}
	}

	gApp = config.App
	setAppVer(config.AppVer)

	return config
}
