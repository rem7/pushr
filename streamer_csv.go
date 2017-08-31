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
	"os"
	"sync"
)

type CSVStream struct {
	file         *os.File
	mutex        *sync.RWMutex
	recordFormat []Attribute
}

func NewCSVStream(recordFormat []Attribute, file string) *CSVStream {

	s := &CSVStream{}

	f, err := os.Create(file)
	if err != nil {
		panic(err)
	}

	m := new(sync.RWMutex)

	s.file = f
	s.mutex = m
	s.recordFormat = recordFormat

	return s

}

func (s *CSVStream) Close() {
	s.file.Close()
}

func (s *CSVStream) Stream(data *Record) error {

	s.mutex.Lock()
	_, err := s.file.Write(data.RecordToCSV())
	s.mutex.Unlock()

	return err

}

func (s *CSVStream) RecordFormat() []Attribute {
	return s.recordFormat
}
