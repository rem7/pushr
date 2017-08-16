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
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"regexp"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"gopkg.in/fsnotify.v1"
)

const (
	SLEEP_TIMEOUT = time.Second * 1
	FD_TIMEOUT    = time.Minute * 10
)

var (
	delim = regexp.MustCompile(`\r?\n`)
)

type Tail struct {
	Filename      string
	LineChan      chan string
	Cancel        context.CancelFunc
	follow        bool
	context       context.Context
	retryFileOpen bool // keep trying to re-open the file, helpful when the file doesn't exist yet
}

func NewTail(path string) *Tail {

	ctx, cancel := context.WithCancel(context.Background())

	t := &Tail{
		Filename:      path,
		LineChan:      make(chan string),
		Cancel:        cancel,
		follow:        true,
		context:       ctx,
		retryFileOpen: true,
	}

	t.Start()

	return t
}

func (t *Tail) Start() {
	go t.watchFile(t.context, t.Filename)
}

func NewTailWithCtx(ctx context.Context, path string, follow, retryFileOpen bool) *Tail {

	ctx, cancel := context.WithCancel(ctx)

	t := &Tail{
		Filename:      path,
		LineChan:      make(chan string),
		Cancel:        cancel,
		follow:        follow,
		context:       ctx,
		retryFileOpen: retryFileOpen,
	}

	t.Start()
	return t
}

func (t *Tail) Close() {
	close(t.LineChan)
	t.Cancel()
}

func (t *Tail) openFile(path string) (*os.File, error) {

	var f *os.File
	var err error
	for {

		if !t.retryFileOpen {
			select {
			case <-t.context.Done():
				return nil, errors.New("Tail context cancelled.")
			default:
				break
			}
		}

		f, err = os.Open(path)
		if err != nil {
			log.Infof("Unable to open. %s. Waiting 5 seconds and retrying", err.Error())
			time.Sleep(time.Second * 5)
		} else {
			break
		}
	}

	return f, nil
}

func (t *Tail) watchFile(ctx context.Context, path string) {

	fileIn, err := t.openFile(path)
	if err != nil {
		log.Infof("1. Unable to openFile. %s. Waiting 5 seconds and retrying", err.Error())
		return
	}
	defer fileIn.Close()

	r := bufio.NewReader(fileIn)

	accum := new(bytes.Buffer)
	done := make(chan bool)

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}
	defer watcher.Close()

	err = watcher.Add(path)
	if err != nil {
		log.Fatal(err)
	}

loop:
	for {

		buffer := read(ctx, r, accum)
		reader := bufio.NewReader(buffer)
		for {

			select {
			case <-ctx.Done():
				t.Close()
				return
			default:
				break
			}

			line, err := reader.ReadString('\n')
			if err != nil && err != io.EOF {
				log.WithField("file", path).Error(err.Error())
				break

			}

			if len(line) > 0 {
				t.LineChan <- string([]rune(strings.TrimRight(line, "\n")))
			}

			if err == io.EOF {
				if !t.follow {
					t.Close()
					return
				}
				break
			}

		}

		select {
		case <-time.After(SLEEP_TIMEOUT):
		case event := <-watcher.Events:
			if event.Op&fsnotify.Rename == fsnotify.Rename {
				log.WithField("file", path).Info("File renamed. Monitoring old fd for 10 minutes")
				go t.watchFile(ctx, path)
				time.AfterFunc(FD_TIMEOUT, func() {
					log.WithField("file", path).Info("Closing old fd")
					done <- true
				})
			}
		case err := <-watcher.Errors:
			log.WithField("file", path).Errorf("inotify error: %v", err)
		case <-done:
			break loop
		}
	}

}

func read(ctx context.Context, f io.Reader, accum *bytes.Buffer) io.Reader {

	r, w := io.Pipe()
	buffer_size := 1048576 // 1MB

	go func() {
		defer w.Close()

		buffer := make([]byte, buffer_size)
		for {

			select {
			case <-ctx.Done():
				return
			default:
				break
			}

			n, err := f.Read(buffer)
			if err != nil {
				w.Close()
				break
			}

			slice := buffer[:n]
			locs := delim.FindAllSubmatchIndex(slice, -1)

			if len(locs) == 0 {
				accum.Write(slice)
			} else {
				start_idx := 0
				end_idx := 0
				for _, loc := range locs {
					end_idx = loc[1]
					data := append(accum.Bytes(), slice[start_idx:end_idx]...)
					w.Write(data)
					accum.Reset()
					start_idx = end_idx
				}

				if end_idx < n {
					accum.Write(slice[end_idx:])
				}
			}
		}

	}()

	return r
}
