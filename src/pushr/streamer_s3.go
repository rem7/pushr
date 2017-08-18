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
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"path/filepath"
	"sync"
	"time"
)

type S3Stream struct {
	buf          bytes.Buffer
	mutex        *sync.RWMutex
	sizeLimit    int
	interval     time.Duration
	svc          *s3.S3
	bucket       *string
	prefix       string
	dataChan     chan []byte
	recordFormat []Attribute
}

func (s *S3Stream) Close() {
	log.Printf("S3Stream Close not implemented")
}

func NewS3Stream(recordFormat []Attribute, accessKey, secretAccessKey, awsRegion,
	bucket, prefix string, sizeLimit int,
	interval time.Duration) *S3Stream {

	s := &S3Stream{}

	sess := &session.Session{}

	if accessKey == "" || secretAccessKey == "" {
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

	s.svc = s3.New(sess)
	s.buf = bytes.Buffer{}
	s.mutex = new(sync.RWMutex)
	s.interval = interval
	s.sizeLimit = sizeLimit
	s.bucket = aws.String(bucket)
	s.dataChan = make(chan []byte, sizeLimit*2)
	s.recordFormat = recordFormat
	go s.IntervalStreamer()

	return s

}

func (s *S3Stream) Stream(data []byte) error {

	s.dataChan <- data
	return nil

}

func (s *S3Stream) IntervalStreamer() {

	timer := time.NewTicker(s.interval)
	for {
		select {
		case data := <-s.dataChan:
			s.writeData(data, false)
		case <-timer.C:
			s.writeData(nil, true)
		}
	}
}

func (s *S3Stream) writeData(data []byte, forceUpload bool) {

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if data != nil {
		s.buf.Write(data)
	}

	if s.buf.Len() >= s.sizeLimit || forceUpload {
		dataCopy := make([]byte, s.buf.Len())
		copy(dataCopy, s.buf.Bytes())
		go s.uploadBuffer(dataCopy)
		s.buf.Reset()
	}

}

func (s *S3Stream) uploadBuffer(data []byte) {

	now := time.Now().UTC()

	folders := fmt.Sprintf("%04d/%02d/%02d/%02d/%02d",
		now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute())
	key := filepath.Join(s.prefix, folders, "asdf")

	_, err := s.svc.PutObject(&s3.PutObjectInput{
		Bucket: s.bucket,
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})

	if err != nil {
		log.Error(err.Error())
	}

}

func (s *S3Stream) RecordFormat() []Attribute {
	return s.recordFormat
}
