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
	"encoding/csv"
	log "github.com/Sirupsen/logrus"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func appVer() string {
	gAppVerMutex.RLock()
	defer gAppVerMutex.RUnlock()
	return gAppVer
}

func setAppVer(newVal string) {
	gAppVerMutex.Lock()
	defer gAppVerMutex.Unlock()
	gAppVer = newVal
}

func loadStateFile(path string) map[string]Logfile {

	state := make(map[string]Logfile)
	r, err := os.Open(path)
	if err != nil {
		return state
	}
	defer r.Close()
	csvReader := csv.NewReader(r)

	for {

		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}

		if err != nil {
			log.WithField("file", path).Error(err)
			break
		}

		if len(record) == 3 {

			timeParsed, err := time.Parse(ISO_8601, record[1])
			if err != nil {
				log.WithField("file", path).Error("Unable to parse state time for line: %v", record)
				continue
			}

			l := Logfile{
				Filename:      record[0],
				LastTimestamp: timeParsed,
			}
			state[record[0]] = l
			setAppVer(record[2])

			gUpdateCacheChan <- UpdateMessage{record[0], timeParsed}
		}
	}

	return state
}

func handleSignal(cancel context.CancelFunc) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	go func() {
		<-c
		cancel()
	}()

}

type UpdateMessage struct {
	Filename           string
	LastEventTimestamp time.Time
}

func updateStateFileInterval(ctx context.Context) {

	t := time.NewTicker(time.Second * 5)

	logfilesMap := make(map[string]*UpdateMessage)

LOOP:
	for {
		select {
		case updateMessage := <-gUpdateCacheChan:
			if l, ok := logfilesMap[updateMessage.Filename]; ok {
				if !updateMessage.LastEventTimestamp.IsZero() {
					l.LastEventTimestamp = updateMessage.LastEventTimestamp
				}
			} else {
				logfilesMap[updateMessage.Filename] = &updateMessage
			}
		case <-t.C:
			if len(logfilesMap) > 0 {
				saveStateFile(logfilesMap)
			}
		case <-ctx.Done():
			break LOOP
		}
	}
	saveStateFile(logfilesMap)
}

func saveStateFile(logfiles map[string]*UpdateMessage) {

	f, err := os.Create(gStateFilePath)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	csvWriter := csv.NewWriter(f)

	for logFilepath, logFile := range logfiles {

		timestr := logFile.LastEventTimestamp.Format(ISO_8601)
		if logFile.LastEventTimestamp.IsZero() {
			timestr = ""
		}

		vals := []string{
			logFilepath,
			timestr,
			appVer(),
		}
		csvWriter.Write(vals)
	}

	csvWriter.Flush()

}
