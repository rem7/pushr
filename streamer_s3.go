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
	"encoding/json"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"math"
	"net/http"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

type S3Stream struct {
	ctx            context.Context
	buf            bytes.Buffer
	wg             sync.WaitGroup
	recordCount    int
	mutex          *sync.RWMutex
	bufferSize     int
	bufferInterval time.Duration
	svc            *s3.S3
	bucket         string
	prefix         string
	dataChan       chan []byte
	recordFormat   []Attribute
	apiUrl         string
	apiKey         string
	apiHeaderKey   string
	stream         string
	ddlVersion     string
	s3Owner        string
}

func (s *S3Stream) Close() {
	s.wg.Wait()
}

func NewS3Stream(ctx context.Context, recordFormat []Attribute, accessKey, secretAccessKey,
	awsRegion, awsSTSRole, streamName string, options []string) *S3Stream {

	opts := ParseOptions(options)
	s := parseS3Options(opts)

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

	s.ctx = ctx
	s.svc = s3.New(sess, awsConfig)
	s.stream = streamName
	s.buf = bytes.Buffer{}
	s.mutex = new(sync.RWMutex)
	s.dataChan = make(chan []byte, s.bufferSize*2)
	s.recordFormat = recordFormat

	s.wg.Add(1)
	go s.IntervalStreamer()

	return s

}

func (s *S3Stream) Stream(r *Record) error {
	s.dataChan <- r.RecordToCSV()
	return nil
}

func (s *S3Stream) IntervalStreamer() {

	exit := false
	timer := time.NewTicker(s.bufferInterval)
	for {
		select {
		case data := <-s.dataChan:
			s.writeData(data, false)
		case <-timer.C:
			s.writeData(nil, true)
		case <-s.ctx.Done():
			log.Printf("context done. Force Flush")
			s.writeData(nil, true)
			s.wg.Done()
			exit = true
		}

		if exit {
			s.wg.Wait()
			break
		}

	}
}

func (s *S3Stream) writeData(data []byte, forceUpload bool) {

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if data != nil {
		s.buf.Write(data)
		s.recordCount += 1
	}

	if s.buf.Len() >= s.bufferSize || forceUpload {
		dataCopy := make([]byte, s.buf.Len())
		copy(dataCopy, s.buf.Bytes())
		s.uploadBuffer(dataCopy, s.recordCount, 0)
		s.buf.Reset()
		s.recordCount = 0
	}

}

func (s *S3Stream) uploadBuffer(data []byte, recordCount, retryCount int) {
	s.wg.Add(1)
	go s._uploadBuffer(data, recordCount, retryCount)
}

func (s *S3Stream) _uploadBuffer(data []byte, recordCount, retryCount int) {

	defer s.wg.Done()

	var sleepTime = time.Duration(math.Min(60.0, float64(5*retryCount))) * time.Second
	if sleepTime > time.Duration(0) {
		log.Warnf("Retrying %v records in %v seconds", len(data), sleepTime)
	}
	time.Sleep(sleepTime)

	now := time.Now().UTC()

	folders := fmt.Sprintf("%04d/%02d/%02d/%02d/%02d",
		now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute())

	filename := getHash(data)

	key := filepath.Join(s.prefix, s.stream, folders, filename)

	s3PutOpts := &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	}

	if s.s3Owner != "" {
		s3PutOpts.GrantFullControl = aws.String(s.s3Owner)
	}

	_, err := s.svc.PutObject(s3PutOpts)

	if err != nil {
		log.Printf("Error uploading to S3: \n%v\nretrying...", err.Error())
		s.uploadBuffer(data, recordCount, retryCount+1)
		return
	}

	if retryCount > 0 {
		log.Warnf("S3 copy succeeded after %v retries", retryCount)
	}

	if s.apiUrl != "" {
		opts := AddFileRequest{
			Fullpath:    key,
			TableName:   s.stream,
			DDLVersion:  s.ddlVersion,
			RecordCount: recordCount,
		}

		err = s.updateAPI(opts)
		if err != nil {
			log.Printf("failed to update API -- handle retry")
			log.Printf(err.Error())
		}
	}

}

func (s *S3Stream) updateAPI(opts AddFileRequest) error {

	jsonBodyEncoded := new(bytes.Buffer)

	err := json.NewEncoder(jsonBodyEncoded).Encode(opts)
	if err != nil {
		log.Printf(err.Error())
		return err
	}

	client := &http.Client{}
	req, err := http.NewRequest("POST", s.apiUrl, jsonBodyEncoded)
	if err != nil {
		log.Printf("error creating request")
		return err
	}
	req.Header.Add(s.apiHeaderKey, s.apiKey)
	req.Header.Add("content-type", "application/json")
	_, err = client.Do(req)
	// if resp.StatusCode == 201
	if err != nil {
		log.Printf("error updating API")
		return err
	}

	return nil

}

type AddFileRequest struct {
	Fullpath    string `json:"fullpath"`
	TableName   string `json:"table_name"`
	DDLVersion  string `json:"ddl_version"`
	RecordCount int    `json:"record_count"`
}

func (s *S3Stream) RecordFormat() []Attribute {
	return s.recordFormat
}

func parseS3Options(opts map[string]string) *S3Stream {

	s := &S3Stream{}

	for key, val := range opts {
		switch key {
		case "bucket":
			s.bucket = val
		case "prefix":
			s.prefix = val
		case "buffer_size":
			i, err := strconv.Atoi(val)
			if err != nil {
				log.Fatal(err.Error())
			}
			s.bufferSize = i
		case "buffer_interval":
			i, err := strconv.Atoi(val)
			if err != nil {
				log.Fatal(err.Error())
			}
			s.bufferInterval = time.Duration(i) * time.Second
		case "api_url":
			s.apiUrl = val
		case "api_key":
			s.apiKey = val
		case "api_header_key":
			s.apiHeaderKey = val
		case "ddl_version":
			s.ddlVersion = val
		case "s3_owner":
			s.s3Owner = val
		default:
			break
		}
	}

	return s
}
