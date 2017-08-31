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
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	log "github.com/Sirupsen/logrus"
	"hash"
	"math"
	"net/http"
	"sync"
	"time"
)

type DCHTTPStream struct {
	apiKey       string
	endpoint     string
	eventsBuffer []interface{}
	mutex        *sync.RWMutex
	sizeLimit    int
	dataChan     chan *Record
	lastUpload   time.Time
	hasher       hash.Hash
	recordFormat []Attribute
}

func (s *DCHTTPStream) Close() {
	log.Printf("DCHTTPStream Close not implemented")
}

func NewDCHTTPStream(recordFormat []Attribute, endpoint, apiKey string, sizeLimit int) *DCHTTPStream {

	s := &DCHTTPStream{}

	s.apiKey = apiKey
	s.mutex = new(sync.RWMutex)
	s.sizeLimit = sizeLimit
	s.endpoint = endpoint
	s.dataChan = make(chan *Record)
	s.lastUpload = time.Now()
	s.hasher = md5.New()
	s.recordFormat = recordFormat

	go s.IntervalStreamer()

	return s

}

func (s *DCHTTPStream) Stream(data *Record) error {

	s.dataChan <- data
	return nil

}

func (s *DCHTTPStream) IntervalStreamer() {

	timer := time.NewTicker(time.Second)
	for {
		select {
		case data := <-s.dataChan:
			s.writeData(data)
		case <-timer.C:
			s.writeData(nil)
		}
	}
}

func (s *DCHTTPStream) writeData(record *Record) {

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if record != nil {

		s.hasher.Write(record.Hash())
		delete(record.EventAttributes, "log_line")
		delete(record.EventAttributes, "ingest_datetime")
		delete(record.EventAttributes, "event_datetime")
		s.eventsBuffer = append(s.eventsBuffer, record.EventAttributes)
	}

	forceUpload := false
	if time.Now().After(s.lastUpload.Add(time.Second * 30)) {
		forceUpload = true
	}

	if len(s.eventsBuffer) > 0 && (len(s.eventsBuffer) >= s.sizeLimit || forceUpload) {

		s.hasher.Write([]byte("20160727"))
		txid := hex.EncodeToString(s.hasher.Sum(nil))

		postData := DCHTTPPost{
			ApiKey:        s.apiKey,
			TransactionId: txid,
			Events:        s.eventsBuffer,
		}

		data := &bytes.Buffer{}
		json.NewEncoder(data).Encode(&postData)
		body := bytes.NewReader(data.Bytes())

		request, err := http.NewRequest("POST", s.endpoint, body)
		if err != nil {
			log.Error(err.Error())
		}
		request.Header.Set("Content-Type", "application/json")

		// upload
		tryCount := 0
		for {

			client := &http.Client{}
			var sleepTime = time.Duration(math.Min(60.0, float64(5*tryCount))) * time.Second
			if sleepTime > time.Duration(0) {
				log.Warnf("Retrying txid: %s in %v seconds", txid, sleepTime)
			}
			time.Sleep(sleepTime)
			tryCount += 1

			log.Warnf("pushing txid: %s (%d bytes)", txid, body.Len())

			res, err := client.Do(request)
			if err != nil {
				log.Warnf("http err: %s. retrying", err.Error())
			} else {

				res.Body.Close()

				if res.StatusCode == 200 {
					log.Warnf("http 200 txid: %s", txid)
					break
				} else if res.StatusCode == 400 { // bad_event
					log.Error("http 400")
					break
				} else if res.StatusCode == 409 { // transaction_id already used
					log.Errorf("txid dup, skipping. %s", txid)
					break
				} else if res.StatusCode == 500 {
					log.Error("http 500")
					break
				} else if res.StatusCode == 200 {
					log.Warnf("http 200 txid: %s", txid)
					break
				} else if res.StatusCode == 429 { // rate limit exceeded
					log.Warnf("http %d. retrying", res.StatusCode)
				} else if res.StatusCode == 504 { // gateway timed out.
					log.Warnf("http %d. retrying", res.StatusCode)
				} else {
					log.Warnf("http %d. retrying", res.StatusCode)
				}
				body.Seek(0, 0)
			}
		}

		s.eventsBuffer = []interface{}{}
		s.lastUpload = time.Now()
		s.hasher.Reset()
	}

}

type DCHTTPPost struct {
	ApiKey        string        `json:"api_key"`
	TransactionId string        `json:"transaction_id"`
	Events        []interface{} `json:"events"`
}

func (s *DCHTTPStream) RecordFormat() []Attribute {
	return s.recordFormat
}
