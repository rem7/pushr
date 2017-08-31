/*
 * Copyright (c) 2016 Yanko Bolanos
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */
package tail

import (
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"testing"
)

var fullText = `Lorem ipsum dolor sit amet, consectetur adipiscing elit.
Nam nunc odio, vestibulum quis efficitur vel, vulputate eu tortor.
Integer sed maximus nunc. Donec venenatis in purus ac feugiat. Phasellus leo leo,
scelerisque non odio quis, cursus placerat urna. Quisque et tempor neque.
Fusce feugiat quis lorem ullamcorper sagittis. Aliquam erat volutpat.
Vestibulum quis nisl finibus, vehicula neque vitae, consectetur mauris.
Aliquam congue non elit non eleifend. Aliquam erat volutpat. Aliquam ac fermentum velit.
`

var startText = `Lorem ipsum dolor sit amet, consectetur adipiscing elit.
Nam nunc odio, vestibulum quis efficitur vel, vulputate eu tortor.
Integer sed maximus nunc. Donec venenatis in purus ac feugiat. Phasellus leo leo,
`

// var extraText = `scelerisque non odio quis, cursus placerat urna. Quisque et tempor neque.
// Fusce feugiat quis lorem ullamcorper sagittis. Aliquam erat volutpat.
// Vestibulum quis nisl finibus, vehicula neque vitae, consectetur mauris.
// Aliquam congue non elit non eleifend. Aliquam erat volutpat. Aliquam ac fermentum velit.
// `

var extraLines = []string{
	"scelerisque non odio quis, cursus placerat urna. Quisque et tempor neque.",
	"Fusce feugiat quis lorem ullamcorper sagittis. Aliquam erat volutpat.",
	"Vestibulum quis nisl finibus, vehicula neque vitae, consectetur mauris.",
	"Aliquam congue non elit non eleifend. Aliquam erat volutpat. Aliquam ac fermentum velit.",
}

var all_lines = strings.Split(fullText, "\n")

func TestTail(t *testing.T) {

	file, err := ioutil.TempFile(os.TempDir(), "tail_test")
	if err != nil {
		t.Fatal(err.Error())
	}
	defer func() {
		file.Close()
		os.Remove(file.Name())
	}()

	file.WriteString(startText)

	tail := NewTail(file.Name())

	_ = new(sync.Mutex)

	go func() {
		for _, line := range extraLines {
			t.Logf("%v", line)
			// time.Sleep(time.Second * 1)
			// m.Lock()
			// file.WriteString(line)
			// m.Unlock()
		}
	}()

	t.Log("here")

	for i := 0; i < len(all_lines); i++ {
		line := <-tail.LineChan
		t.Log(line)
		if all_lines[i] != line {
			t.Fatal("lines don't match")
		}
	}

	// fmt.Println()

}
