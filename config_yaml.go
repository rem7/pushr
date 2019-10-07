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

	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

// scan for regexes
var exp1 = regexp.MustCompile(`line_regex\:\s?\#(?P<logfile>[^\s]+)\s(?P<exp>.*)`)
var exp2 = regexp.MustCompile(`front_split_regex\:\s?\#(?P<logfile>[^\s]+)\s(?P<exp>.*)`)
var exp3 = regexp.MustCompile(`kv_regex\:\s?\#(?P<logfile>[^\s]+)\s(?P<exp>.*)`)

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

	r := bufio.NewReader(bytes.NewReader(data))
	for {

		line, _, err := r.ReadLine()
		if err != nil && err == io.EOF {
			break
		} else if err != nil {
			log.Fatalf(err.Error())
		}

		setConfigLogfileRegex(config, exp1, line)
		setConfigLogfileRegex(config, exp2, line)
		setConfigLogfileRegex(config, exp3, line)
	}

	gApp = config.App
	setAppVer(config.AppVer)

	return config
}

func setConfigLogfileRegex(config ConfigFile, regex *regexp.Regexp, line []byte) {
	matches := regex.FindSubmatch(line)
	if len(matches) == 3 {
		logfileName := string(matches[1])
		expstr := string(matches[2])
		exp := regexp.MustCompile(expstr)
		for n, logfile := range config.Logfiles {
			if logfile.Name == logfileName {
				switch regex {
				case exp1:
					config.Logfiles[n].KvRegex = exp
					config.Logfiles[n].KvRegexStr = expstr
					break
				case exp2:
					config.Logfiles[n].FrontSplitRegex = exp
					config.Logfiles[n].FrontSplitRegexStr = expstr
					break
				case exp3:
					config.Logfiles[n].KvRegex = exp
					config.Logfiles[n].KvRegexStr = expstr
					break
				}
			}
		}
	}
}
