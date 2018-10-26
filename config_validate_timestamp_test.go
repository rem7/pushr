package main

import (
	"fmt"
	"testing"
)

var config ConfigFile = ConfigFile{}
var strmCfg = StreamConfig{
	"streamName",
	"name",
	"type",
	"url",
	"StreamApiKey",
	"RecordFormatString",
	nil,
	nil,
}

/*
 * Valid configuration - take timestamp as is
 */
func TestValidateEmtpySrcDest(t *testing.T) {
	var attrs = []Attribute{{"key", "timestamp", 16, "", ""}}
	strmCfg.RecordFormat = attrs
	config.Streams = []StreamConfig{strmCfg}

	err := config.validate()
	if err != nil {
		fmt.Println(err)
		t.Fail()
	}
}

/*
 * Valid configuration - valid timestamp formats
 */
func TestValidateCorrectSrcDest(t *testing.T) {
	var attrs = []Attribute{{"key", "timestamp", 16, "2006-01-02T15:04:05.999Z", "2006-01-02T15:04:05.999Z"}}
	strmCfg.RecordFormat = attrs
	config.Streams = []StreamConfig{strmCfg}

	err := config.validate()
	if err != nil {
		fmt.Println(err)
		t.Fail()
	}
}

/*
 * Valid configuration - slash formats to dash formats
 */
func TestValidateVariousDelimiterDate(t *testing.T) {
	var attrs = []Attribute{{"key", "timestamp", 16, "2006/01/02 15:04:05", "2006-01-02T15:04:05.999Z"}}
	strmCfg.RecordFormat = attrs
	config.Streams = []StreamConfig{strmCfg}

	err := config.validate()
	if err != nil {
		t.Fail()
		fmt.Println(err)
	}
}

/*
 * Invalid Config - valid source but no destination format
 */
func TestValidateNoDestinationFormat(t *testing.T) {
	var attrs = []Attribute{{"key", "timestamp", 16, "2006-01-02T15:04:05.999Z", ""}}
	strmCfg.RecordFormat = attrs
	config.Streams = []StreamConfig{strmCfg}

	err := config.validate()
	expected := "must specify Source & Destination Timestamp Format when datatype is Timestamp"
	if err.Error() != expected {
		t.Fail()
	}
}

/*
 * Invalid configuration - valid destination but no source format
 */
func TestValidateNoSourceFormat(t *testing.T) {
	var attrs = []Attribute{{"key", "timestamp", 16, "", "2006-01-02T15:04:05.999Z"}}
	strmCfg.RecordFormat = attrs
	config.Streams = []StreamConfig{strmCfg}

	err := config.validate()
	expected := "must specify Source Timestamp Format when datatype is Timestamp"
	if err.Error() != expected {
		t.Fail()
		fmt.Println(err)
	}
}

/*
 * Invalid configuration - invalid source format
 */
func TestValidateInvalidSource(t *testing.T) {
	var attrs = []Attribute{{"key", "timestamp", 16, "2006-01-02T15::05.999Z", "2006-01-02T15:04:05.999Z"}}
	strmCfg.RecordFormat = attrs
	config.Streams = []StreamConfig{strmCfg}

	err := config.validate()
	if err == nil {
		t.Fail()
		fmt.Println(err)
	}
}

/*
 * Invalid configuration - invalid destination format
 */
func TestValidateInvalidDestination(t *testing.T) {
	var attrs = []Attribute{{"key", "timestamp", 16, "2006-01-02T15:04:05.999Z", "2006-01-02T15::05.999Z"}}
	strmCfg.RecordFormat = attrs
	config.Streams = []StreamConfig{strmCfg}

	err := config.validate()

	if err == nil {
		t.Fail()
		fmt.Println(err)
	}
}

/*
 * Long source to short destination
 * Result - warns of a possible invalid format
 */
func TestValidateShortDestination(t *testing.T) {
	var attrs = []Attribute{{"key", "timestamp", 16, "2006-01-02T15:04:05.999Z", "2006-01-02"}}
	strmCfg.RecordFormat = attrs
	config.Streams = []StreamConfig{strmCfg}

	err := config.validate()
	if err != nil {
		t.Fail()
		fmt.Println(err)
	}
}

/*
 * Short Date to long date
 * Result - warns of a possible invalid format
 */
func TestValidateShortSource(t *testing.T) {
	var attrs = []Attribute{{"key", "timestamp", 16, "2006-01-02", "2006-01-02T15:04:05.999Z"}}
	strmCfg.RecordFormat = attrs
	config.Streams = []StreamConfig{strmCfg}

	err := config.validate()
	if err != nil {
		t.Fail()
		fmt.Println(err)
	}
}
