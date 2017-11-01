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
	FD_TIMEOUT    = time.Minute * 5
)

var (
	gDelim = regexp.MustCompile(`\r?\n`)
)

type Tail struct {
	Filename       string
	LineChan       chan string
	Cancel         context.CancelFunc
	Follow         bool
	Context        context.Context
	RetryFileOpen  bool // keep trying to re-open the file, helpful when the file doesn't exist yet
	lineStartSplit bool // logic for handling begining of line split (splunk like, by timestamp)
	delim          *regexp.Regexp
	SeekToEnd      bool
}

func NewTail(path string) *Tail {

	ctx, cancel := context.WithCancel(context.Background())

	t := &Tail{
		Filename:       path,
		LineChan:       make(chan string),
		Cancel:         cancel,
		Follow:         true,
		Context:        ctx,
		RetryFileOpen:  true,
		lineStartSplit: false,
		delim:          gDelim,
		SeekToEnd:      false,
	}

	return t
}

func (t *Tail) Start() {
	go t.watchFile(t.Context, t.Filename)
}

func NewTailWithCtx(ctx context.Context, path string, follow, retryFileOpen bool, delim *regexp.Regexp, lineStartSplit bool, skipToEnd bool) *Tail {

	ctx, cancel := context.WithCancel(ctx)

	d := gDelim
	if delim != nil {
		d = delim
	}

	t := &Tail{
		Filename:       path,
		LineChan:       make(chan string),
		Cancel:         cancel,
		Follow:         follow,
		Context:        ctx,
		RetryFileOpen:  retryFileOpen,
		lineStartSplit: lineStartSplit,
		delim:          d,
		SeekToEnd:      skipToEnd,
	}

	t.Start()
	return t
}

func (t *Tail) Close() {
	defer func() {
		// this is a hack. Need to figure out the order of closing the channel.
		// sometimes its closing more than once
		if r := recover(); r != nil {
			log.WithField("file", t.Filename).
				Warnf("Recovered in Tail.Close(): %s", r)
		}
	}()
	close(t.LineChan)
}

func (t *Tail) openFile(path string) (*os.File, error) {

	var f *os.File
	var err error
	for {

		if !t.RetryFileOpen {
			select {
			case <-t.Context.Done():
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
			if finfo, err := os.Stat(path); err == nil && t.SeekToEnd {
				f.Seek(finfo.Size(), 0)
			}
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

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}
	defer watcher.Close()

	err = watcher.Add(path)
	if err != nil {
		log.Fatal(err)
	}
	done := make(chan bool)

	for {

		if t.lineStartSplit {
			readFrontSplit(ctx, t.delim, t.LineChan, r, accum)
		} else {
			buffer := read(ctx, t.delim, r, accum)
			reader := bufio.NewReader(buffer)
			for {
				select {
				case <-ctx.Done():
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
					if !t.Follow {
						t.Close()
						return
					}
					break
				}

			}
		}

		select {
		case <-time.After(SLEEP_TIMEOUT):
			if t.lineStartSplit && accum.Len() > 0 {
				t.LineChan <- accum.String()
				accum.Reset()
			}
			break
		case event := <-watcher.Events:
			if event.Op&fsnotify.Rename == fsnotify.Rename {
				log.WithField("file", path).Info("File renamed. Monitoring old fd for 5 minutes")
				go t.watchFile(ctx, path)
				time.AfterFunc(FD_TIMEOUT, func() {
					done <- true
				})
			}
			break
		case err := <-watcher.Errors:
			log.WithField("file", path).Errorf("inotify error: %v", err)
			break
		case <-done:
			log.WithField("file", path).Info("Closing old fd")
			return
		case <-ctx.Done():
			if accum.Len() > 0 {
				t.LineChan <- accum.String()
				accum.Reset()
			}
			t.Close()
			return
		}
	}
}

func read(ctx context.Context, delim *regexp.Regexp, f io.Reader, accum *bytes.Buffer) io.Reader {

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

func readFrontSplit(ctx context.Context, delim *regexp.Regexp, lineChan chan string, f io.Reader, accum *bytes.Buffer) {

	buffer_size := 1048576 // 1MB

	// go func() {
	// defer w.Close()

	buffer := make([]byte, buffer_size)
	first := true
	for {
		select {
		case <-ctx.Done():
			return
		default:
			break
		}

		n, err := f.Read(buffer)
		if err != nil {
			break
		}

		slice := buffer[:n]
		locs := delim.FindAllSubmatchIndex(slice, -1)

		if len(locs) == 0 {
			accum.Write(slice)
		} else {

			line_begin_idx := 0
			line_end_idx := 0
			next_start := 0
			for _, loc := range locs {
				if first {
					first = false
					next_start = loc[0]
					if loc[0] == 0 && accum.Len() > 0 {
						lineChan <- accum.String()
						accum.Reset()
					}
					continue
				} else {
					line_begin_idx = next_start
				}

				line_end_idx = loc[0]
				next_start = loc[1] - (loc[1] - loc[0])
				data := bytes.Replace(slice[line_begin_idx:line_end_idx], []byte("\x0A"), nil, -1)
				data = append(accum.Bytes(), data...)
				lineChan <- string(data)
				accum.Reset()
				// data_trim := bytes.TrimRight(data, "\n")
				// log.Printf("%v -> %v", line_begin_idx, line_end_idx)
				// fmt.Printf("\n-----line start %v -----\n%s\n-----line end %v -----\n", line_begin_idx, data_trim, line_end_idx)

			}

			data := slice[next_start:n]
			accum.Write(data)
			// w.Write(data)
			// lineChan <- string(data)
			// data_trim := bytes.TrimRight(data, "\n")
			// fmt.Printf("\n-----line start %v -----\n%s\n-----line end %v -----\n", next_start, data, n)
		}
	}

	// lineChan <- string(accum.Bytes())
	// accum.Reset()
	// }()

	// <-ctx.Done()

}
