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
	log "github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/firehose"
	"math"
	"sync"
	"time"
)

var (
	REQUEST_SIZE_LIMIT int = 2e+6 // 2MB
	BATCH_LIMIT            = 500
)

type FirehoseStream struct {
	svc          *firehose.Firehose
	stream       string
	dataChan     chan []byte
	interval     time.Duration
	recordFormat []Attribute
	ctx          context.Context
	wg           sync.WaitGroup
}

func NewFirehoseStream(ctx context.Context, recordFormat []Attribute, accessKey, secretAccessKey, awsRegion, awsSTSRole, streamName string) *FirehoseStream {

	if awsRegion == "" {
		log.Fatal("Please Specify the region your firehose is.")
	}

	s := &FirehoseStream{}
	sess := &session.Session{}
	awsConfig := &aws.Config{Region: aws.String(awsRegion)}

	if awsSTSRole != "" {
		sess = session.Must(session.NewSession())
		creds := stscreds.NewCredentials(sess, awsSTSRole)
		awsConfig.Credentials = creds
	} else if accessKey == "" || secretAccessKey == "" {
		// try IAM
		sess = session.New(nil)
	} else {
		creds := credentials.NewStaticCredentials(accessKey, secretAccessKey, "")
		config := &aws.Config{
			Region:      aws.String(awsRegion),
			Credentials: creds,
		}
		sess = session.New(config)
	}

	s.svc = firehose.New(sess, awsConfig)
	s.stream = streamName
	s.dataChan = make(chan []byte, BATCH_LIMIT*5)
	s.interval = 5 * time.Second
	s.recordFormat = recordFormat
	s.ctx = ctx

	go s.intervalStreamer()

	return s
}

func (s *FirehoseStream) Stream(r *Record) error {
	s.dataChan <- r.RecordToCSV()
	return nil
}

func (s *FirehoseStream) Close() {
	s.wg.Wait()
}

func (s *FirehoseStream) intervalStreamer() {

	accum := []*firehose.Record{}
	sizeAccumulator := 0
	timer := time.NewTicker(s.interval)
	exit := false
LOOP:
	for {

		data := []byte{}
		flush := false

		select {
		case incomingData := <-s.dataChan:
			data = incomingData
		case <-timer.C:
			flush = true
			data = nil
		case <-s.ctx.Done():
			flush = true
			data = nil
			log.Printf("context done. Force Flush")
			exit = true
		}

		if data != nil {

			if len(data) > 1000000 {
				log.Printf("Data recorded exceeded 1MB. skipping: \n--\n%s\n--", data)
				continue
			}

			sizeAccumulator += len(data)
			record := &firehose.Record{
				Data: data,
			}
			accum = append(accum, record)
		}

		if (len(accum) == BATCH_LIMIT || sizeAccumulator > REQUEST_SIZE_LIMIT || flush) && len(accum) > 0 {

			dataCopy := make([]*firehose.Record, len(accum))
			copy(dataCopy, accum)

			s.wg.Add(1)
			go s.uploadRecords(dataCopy, 0)

			accum = []*firehose.Record{}
			sizeAccumulator = 0

		}

		if exit {
			break LOOP
		}

	}
}

func (s *FirehoseStream) uploadRecords(data []*firehose.Record, failCount int) {

	defer s.wg.Done()

	var sleepTime = time.Duration(math.Min(60.0, float64(5*failCount))) * time.Second
	if sleepTime > time.Duration(0) {
		log.Warnf("Retrying %v records in %v seconds", len(data), sleepTime)
	}
	time.Sleep(sleepTime)

	params := &firehose.PutRecordBatchInput{
		DeliveryStreamName: aws.String(s.stream),
		Records:            data,
	}

	r, err := s.svc.PutRecordBatch(params)
	if err != nil {
		log.Error(err.Error())
		s.wg.Add(1)
		go s.uploadRecords(data, failCount+1)
		return
	}

	if *r.FailedPutCount > 0 {

		// Build a new array with the records that failed.
		newData := []*firehose.Record{}
		for i, req := range r.RequestResponses {
			if req.ErrorCode != nil {
				newData = append(newData, data[i])
			}
		}

		s.wg.Add(1)
		go s.uploadRecords(newData, failCount+1)
	}

}

func (s *FirehoseStream) RecordFormat() []Attribute {
	return s.recordFormat
}
