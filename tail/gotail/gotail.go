/*
 * Copyright (c) 2016 Yanko Bolanos
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */
package main

// Sample program using tail library

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"sync"
	"unicode/utf8"

	"github.com/rem7/pushr/tail"
)

func main() {

	if len(os.Args) < 2 {
		fmt.Printf("need one arg")
		os.Exit(-1)
	}

	fls := regexp.MustCompile(`\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}-\d{2}:\d{2}\s`)

	var wg sync.WaitGroup
	files := os.Args[1:]
	for _, file := range files {
		wg.Add(1)
		go func(path string) {

			t := tail.NewTailWithCtx(context.Background(), path, false, true, fls, true, false)

			n := 1
			for {
				line := <-t.LineChan
				if ok := utf8.ValidString(line); !ok {
					fmt.Print("line %d not UTF-8: ", n)
				}
				fmt.Println(line)
				n += 1
			}
			wg.Done()
		}(file)
	}
	wg.Wait()
}
