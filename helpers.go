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
	"context"
	"crypto/rand"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"gopkg.in/fsnotify.v1"
	"io"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

func GenerateUUID() (string, error) {

	uuid := make([]byte, 16)
	n, err := io.ReadFull(rand.Reader, uuid)
	if n != len(uuid) || err != nil {
		return "uuid-error", err
	}

	// variant bits; see section 4.1.1
	uuid[8] = uuid[8]&^0xc0 | 0x80
	// version 4 (pseudo-random); see section 4.1.3
	uuid[6] = uuid[6]&^0xf0 | 0x40
	return fmt.Sprintf("%x-%x-%x-%x-%x", uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:]), nil
}

func ParseOptions(options []string) map[string]string {
	optionsProps := make(map[string]string)
	for i := 0; i < len(options); i++ {
		optionsSplit := strings.Split(options[i], ":")
		if len(optionsSplit) > 1 {
			key, value := optionsSplit[0], optionsSplit[1]
			optionsProps[key] = strings.TrimSpace(value)
		}
	}
	return optionsProps
}

func LogFuncs(logfile Logfile) (func(msg string, args ...interface{}),
	func(msg string, args ...interface{}),
	func(msg string, args ...interface{}),
	func(msg string, args ...interface{})) {

	infof := func(msg string, args ...interface{}) {
		log.WithField("file", logfile.Filename).
			WithField("stream", logfile.StreamName).
			WithField("logfile_name", logfile.Name).
			Infof(msg, args...)
	}

	warnf := func(msg string, args ...interface{}) {
		log.WithField("file", logfile.Filename).
			WithField("stream", logfile.StreamName).
			WithField("logfile_name", logfile.Name).
			Warnf(msg, args...)
	}

	errorf := func(msg string, args ...interface{}) {
		log.WithField("file", logfile.Filename).
			WithField("stream", logfile.StreamName).
			WithField("logfile_name", logfile.Name).
			Errorf(msg, args...)
	}

	fatalf := func(msg string, args ...interface{}) {
		log.WithField("file", logfile.Filename).
			WithField("stream", logfile.StreamName).
			WithField("logfile_name", logfile.Name).
			Fatalf(msg, args...)
	}

	return infof, warnf, errorf, fatalf
}

func regexGet(str string, r *regexp.Regexp) string {

	matches := r.FindStringSubmatch(str)
	if len(matches) > 1 {
		return matches[1]
	} else {
		return "\\N"
	}

}

func IsDir(path string) (bool, error) {

	if info, err := os.Stat(path); err == nil && info.IsDir() {
		return true, nil
	} else {
		return false, err
	}
	return false, nil

}

func monitorDir(ctx context.Context, path string) (chan string, chan string, error) {

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}
	// defer
	dirPath, ext_wildcard := filepath.Split(path)
	ext := filepath.Ext(ext_wildcard)

	newFiles := make(chan string)
	removedFiles := make(chan string)
	go func() {
		for {
			select {
			case <-ctx.Done():
				close(newFiles)
				close(removedFiles)
				watcher.Close()
				log.Printf("closing monitorFiles")
				return
			case event := <-watcher.Events:
				switch event.Op {
				case fsnotify.Create:
					if strings.Index(event.Name, ext) > 0 {
						newFiles <- event.Name
					}
					break
				case fsnotify.Remove, fsnotify.Rename:
					if strings.Index(event.Name, ext) > 0 {
						removedFiles <- event.Name
					}
					break
				default:
					// log.WithField("file", event.Name).Warnf("else: event.Op:  %v", event.Op)
					break
				}
			case err := <-watcher.Errors:
				log.Printf("error: %s", err)
			}
		}
	}()

	err = watcher.Add(dirPath)
	if err != nil {
		log.Fatal(err)
	}

	return newFiles, removedFiles, err

}

func ConvertToUTF8(s string, length int) string {
	// truncates string if length > 0
	r := []rune(s)
	if length > 0 && len(r) > length {
		r = r[0:length]
	}

	return string(r)

}

func getFileSize(path string) (int64, error) {

	file, err := os.Open(path)
	if err != nil {
		return 0, err
	}

	fi, err := file.Stat()
	if err != nil {
		return 0, err
	}

	return fi.Size(), nil
}

func isNull(value string) (isnull bool) {
	isnull = false
	for _, n := range []string{" ", "null", "none", "-", "empty", ""} {
		if value == n {
			isnull = true
			return
		}
	}
	return
}

func testTimeformat() {

	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Enter timestamp: ")
	timestamp, _ := reader.ReadString('\n')

	fmt.Print("\nEnter time format: ")
	format, _ := reader.ReadString('\n')

	t, err := time.Parse(format, timestamp)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("\nParsed time:\n%v\n", t.String())
}

func testRegexExp() {

	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Enter string: ")
	str, _ := reader.ReadString('\n')
	str = strings.TrimRight(str, "\n")

	fmt.Print("\nEnter regular expression: ")
	regExStr, _ := reader.ReadString('\n')
	regExStr = strings.TrimRight(regExStr, "\n")

	lineRegex, err := regexp.Compile(regExStr)
	if err != nil {
		log.Fatal(err)
	}

	parser := NewRegexParser("", "", "", "", lineRegex, defaultAttributes)

	log.Info(regExStr)

	fmt.Println("-------")
	result := parser.Defaults()
	match := lineRegex.FindStringSubmatch(str)
	if len(match) > 1 {
		log.Info("Matches...")
		for i, name := range lineRegex.SubexpNames() {
			if i != 0 {
				value := match[i]
				if isNull(value) {
					result[name] = "\\N"
				} else {
					result[name] = value
				}

			}
		}
	} else {
		log.Info("no matches :(")
	}

	if val_float, err := strconv.ParseFloat(result["response_s"], 64); err == nil {
		result["response_ms"] = fmt.Sprintf("%.2f", val_float*1000)
	}

	for k, v := range result {
		fmt.Printf("%v -> %v\n", k, v)
	}

}

func escapeRegex() {
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Enter Regex: ")
	s, _ := reader.ReadString('\n')
	r := strconv.QuoteToASCII(s[:len(s)-1])
	fmt.Printf("\nInsert into Json config:\n%v\n", r)
}

func isLocalIP(ip string, addresses []string) bool {
	for _, local_ip := range addresses {
		if local_ip == ip {
			return true
		}
	}
	return false
}

func getIPs() []string {

	addresses := []string{}
	ifaces, err := net.Interfaces()
	if err != nil {
		return addresses
	}

	for _, i := range ifaces {
		addrs, err := i.Addrs()
		if err != nil {
			return addresses
		}

		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			address := fmt.Sprintf("%s", ip)
			addresses = append(addresses, address)
			// process IP address
		}
	}
	return addresses
}

func interfaceToString(inf interface{}) (string, error) {
	switch t := inf.(type) {
	case nil :
		return "\\N", nil
	case string :
		if isNull(inf.(string)) {
			return "\\N", nil
		} else {
			return inf.(string), nil
		}
	case int :
		return strconv.Itoa(inf.(int)), nil
	case bool :
		return strconv.FormatBool(inf.(bool)), nil
	case float32 :
		return strconv.FormatFloat(inf.(float64), 'G', -1, 32), nil
	case float64 :
		return strconv.FormatFloat(inf.(float64), 'G', -1, 64), nil
	case uint64 :
		return strconv.FormatUint(inf.(uint64), 10), nil
	case error :
		return inf.(error).Error(), nil
	default :
		return fmt.Sprintf("%v", t), fmt.Errorf("Coercing string conversion on: '%v' from unknown type: %T", t, t)
	}
}
