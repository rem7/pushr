package main

import (
	"fmt"
	"testing"
	"time"
)

// layout string is a representation of the time stamp,
// Jan 2 15:04:05 2006 MST
// 01/02/2006 15:04PM
// 1 2 3 4 5 6 -7

const debug = false

func checkValidReturn(t *testing.T, returnVal, expectedReturn string, err error) {
	if debug {
		fmt.Printf("Test %s\n", t.Name())
		fmt.Printf("Return Val %s\n", returnVal)
		fmt.Printf("Expected Val %s\n\n", expectedReturn)
	}

	if returnVal != expectedReturn || err != nil {
		t.Fail()
		fmt.Printf("Error %s\n", err)
	}
}

func checkInvalidReturn(t *testing.T, returnVal string, err error) {
	if debug {
		fmt.Printf("Test %s\n", t.Name())
		fmt.Printf("Return Val %s\n", returnVal)
	}

	fmt.Printf("Error %s\n", err)

	if returnVal != "" || err == nil {
		t.Fail()
	}
}

func TestSlashDate(t *testing.T) {
	formattedTs, err := toDestinationTimestamp("2006/01/02 15:04:05", ISO_8601, "2018/10/29 11:02:30")
	expectedReturn := "2018-10-29T11:02:30Z"

	checkValidReturn(t, formattedTs, expectedReturn, err)
}

func TestFormatUnderscoreDest(t *testing.T) {
	formattedTs, err := toDestinationTimestamp("2006/01/02 15:04:05", "Mon Jan _2 15:04:05 2006", "2018/10/29 11:02:30")
	expectedReturn := "Mon Oct 29 11:02:30 2018"

	checkValidReturn(t, formattedTs, expectedReturn, err)
}

func TestEmptySrcDest(t *testing.T) {
	formattedTs, err := toDestinationTimestamp("", "", "2018/10/29 11:02:30")
	expectedReturn := "2018/10/29 11:02:30"
	checkValidReturn(t, formattedTs, expectedReturn, err)
}

func TestFormatUnderscoreSrc(t *testing.T) {
	formattedTs, err := toDestinationTimestamp("2006_01_02_15:04:05", ISO_8601, "2018/10/29 11:02:30")

	checkInvalidReturn(t, formattedTs, err)
}

func TestNoInput(t *testing.T) {
	formattedTs, err := toDestinationTimestamp("2006/01/02 15:04:05", ISO_8601, "")

	checkInvalidReturn(t, formattedTs, err)
}

// Full to Shorten date
func TestTruncDestination(t *testing.T) {
	formattedTs, err := toDestinationTimestamp("2006/01/02 15:04:05", "2006/01/02", "2018/03/15 12:13:16")
	expectedReturn := "2018/03/15"

	checkValidReturn(t, formattedTs, expectedReturn, err)
}

// Short to Full date
func TestFullDestination(t *testing.T) {
	formattedTs, err := toDestinationTimestamp("2006/01/02", "2006/01/02 15:04:05", "2018/03/15")
	expectedReturn := "2018/03/15 00:00:00"

	checkValidReturn(t, formattedTs, expectedReturn, err)
}

func TestMismatchSourceFormat(t *testing.T) {
	formattedTs, err := toDestinationTimestamp(ISO_8601, time.RubyDate, "Mon Jan 02 15:04:05 -0700 2006")

	checkInvalidReturn(t, formattedTs, err)
}

func TestRubyDateFormat(t *testing.T) {
	formattedTs, err := toDestinationTimestamp(time.RubyDate, ISO_8601, "Tue Apr 17 8:07:06 -0500 2018")
	expectedReturn := "2018-04-17T08:07:06Z"

	checkValidReturn(t, formattedTs, expectedReturn, err)
}
