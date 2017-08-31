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
	log "github.com/Sirupsen/logrus"
	"sync"
)

var gSubscribers map[string]Subscriber
var gSubMutex *sync.Mutex

func Publish(record *Record) {
	select {
	case gRecords <- record:
	default:
		log.Println("message was not sent")
	}
}

type Subscriber struct {
	SubscriberName string
	Filename       string
	Ch             chan *Record
}

func StartSubServer() chan *Record {
	records := make(chan *Record, 1024)
	gSubMutex = new(sync.Mutex)
	gSubscribers = make(map[string]Subscriber)
	go func() {

		for {
			r := <-records
			for _, subs := range gSubscribers {
				select {
				case subs.Ch <- r:
				default:
					log.Println("message was not sent")
				}
			}
		}

	}()
	return records
}

func AddSubscriber(s Subscriber) {
	log.Printf("subscriber added: %v", s.SubscriberName)
	gSubMutex.Lock()
	defer gSubMutex.Unlock()
	gSubscribers[s.SubscriberName] = s
}

func RemoveSubscriber(name string) {
	log.Printf("subscriber removed: %v", name)
	gSubMutex.Lock()
	defer gSubMutex.Unlock()
	for k, _ := range gSubscribers {
		if k == name {
			delete(gSubscribers, name)
			return
		}
	}

}
