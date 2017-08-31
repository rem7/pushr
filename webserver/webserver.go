/*
 * Copyright (c) 2016 Yanko Bolanos
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */
package webserver

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/websocket"
	"io/ioutil"
	"net/http"
	"strings"
	"tail"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/autoscaling"
	"github.com/aws/aws-sdk-go/service/ec2"
)

var gAddresses []string

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

func TailServer(config ConfigFile) {

	port := ":8888"
	gAddresses = getIPs()
	log.Printf("starting web server: %s", port)

	fs := http.FileServer(http.Dir("static"))
	http.Handle("/", fs)

	tailHandler := NewTailHandler(config)

	http.Handle("/tail", tailHandler)
	http.Handle("/list_files", &ListFilesHandler{config})

	http.HandleFunc("/subscribe", subscribeRaw)
	http.HandleFunc("/subscribe_parsed", subscribeParsed)

	log.Fatal(http.ListenAndServe(port, nil))
	log.Printf("server stopped")
}

type Msg struct {
	// Timestamp string `json:"timestamp"`
	Hostname   string   `json:"hostname"`
	LineId     uint64   `json:"line_id"`
	Line       string   `json:"line"`
	LineParsed []string `json:"line_parsed"`
}

type Event struct {
	Line     string
	ServerIP string
	Record   []string
}

type TailHandler struct {
	ec2        *ec2.EC2
	asg        *autoscaling.AutoScaling
	instanceId string
}

func NewTailHandler(config ConfigFile) *TailHandler {

	// sess := &session.Session{}
	sess := session.New(nil)

	instanceId := ""
	if config.EC2Host {
		var err error
		instanceId, err = getInstanceId()
		if err != nil {
			log.Printf("TailHandler could not get instanceId")
		}
		log.Printf("instanceId: %s", instanceId)
	}

	return &TailHandler{
		ec2:        ec2.New(sess),
		asg:        autoscaling.New(sess),
		instanceId: instanceId,
	}
}

func (t *TailHandler) ServeHTTP(rw http.ResponseWriter, req *http.Request) {

	conn, err := upgrader.Upgrade(rw, req, nil)
	if err != nil {
		http.Error(rw, "Websockets unsupported!", http.StatusInternalServerError)
		return
	}
	defer conn.Close()

	go func() {
		<-req.Context().Done()
		// resp.Body.Read on the parent routine is blocking
		// this will break it out
		return
	}()

	var id uint64 = 0
	lines := make(chan Event)
	parsed := false
	filename := req.URL.Query().Get("filename")
	parsedValue := req.URL.Query().Get("parsed")
	if parsedValue != "" {
		parsed = true
	}

	if t.instanceId != "" {
		log.Printf("getting instance IPs from ASG")
		ips := t.getAutoScaleGroupIPs()
		log.Printf("asg ips:\n%+v", ips)
	}

	servers := []string{"127.0.0.1"}
	// servers := []string{"10.179.0.11", "10.179.1.206"}
	for _, serverIP := range servers {
		go connect(req.Context(), serverIP, filename, parsed, lines)
	}

	for {
		id += 1
		line, ok := <-lines
		if !ok {
			return
		}

		var msg Msg

		if parsed {

			r := csv.NewReader(strings.NewReader(line.Line))
			record, _ := r.Read()
			msg = Msg{

				Hostname:   line.ServerIP,
				LineParsed: record,
				LineId:     id,
			}

		} else {
			msg = Msg{
				Hostname: line.ServerIP,
				Line:     line.Line,
				LineId:   id,
			}
		}

		w := new(bytes.Buffer)
		err := json.NewEncoder(w).Encode(msg)
		if err != nil {
			fmt.Printf("error encoding")
			break
		}

		if err := conn.WriteMessage(websocket.TextMessage, w.Bytes()); err != nil {
			break
		}
	}
}

func (t *TailHandler) getAutoScaleGroupIPs() []*string {

	instanceIps := []*string{}
	group, err := t.getAutoScaleGroup(t.instanceId)
	if err != nil {
		log.Printf(err.Error())
		return instanceIps
	}

	instanceIds := []*string{}
	for _, instance := range group.Instances {
		instanceIds = append(instanceIds, instance.InstanceId)
	}
	////////////

	ec2params := &ec2.DescribeInstancesInput{
		InstanceIds: instanceIds,
	}

	ec2resp, err := t.ec2.DescribeInstances(ec2params)
	if err != nil {
		log.Printf(err.Error())
		return nil
	}

	for _, reservation := range ec2resp.Reservations {
		for _, instance := range reservation.Instances {
			if *instance.State.Name == "running" {
				instanceIps = append(instanceIps, instance.PrivateIpAddress)
			}
		}
	}

	return instanceIps

}

func (t *TailHandler) getAutoScaleGroup(instanceId string) (*Group, error) {

	params := &autoscaling.DescribeAutoScalingInstancesInput{
		InstanceIds: []*string{
			aws.String(instanceId),
		},
	}

	resp, err := t.asg.DescribeAutoScalingInstances(params)
	if err != nil {
		return nil, err
	}

	if len(resp.AutoScalingInstances) == 0 {
		return nil, errors.New("autoscale group for this instance not found")
	}

	name := resp.AutoScalingInstances[0].AutoScalingGroupName
	paramsAsg := &autoscaling.DescribeAutoScalingGroupsInput{
		AutoScalingGroupNames: []*string{name},
	}

	respASG, err := t.asg.DescribeAutoScalingGroups(paramsAsg)
	if err != nil {
		return nil, err
	}

	if len(respASG.AutoScalingGroups) < 1 {
		return nil, errors.New("asg not found")
	}

	asg := respASG.AutoScalingGroups[0]

	return &Group{
		Name:      *name,
		Instances: asg.Instances,
	}, nil
}

func subscribeParsed(rw http.ResponseWriter, req *http.Request) {

	conn, err := upgrader.Upgrade(rw, req, nil)
	if err != nil {
		log.Printf(err.Error())
		return
	}
	defer conn.Close()

	filename := req.URL.Query().Get("filename")
	s := Subscriber{
		SubscriberName: req.RemoteAddr,
		Filename:       filename,
		Ch:             make(chan *Record),
	}

	go func() {
		<-req.Context().Done()
	}()

	AddSubscriber(s)
	defer RemoveSubscriber(s.SubscriberName)
	for {
		record, ok := <-s.Ch
		if !ok {
			return
		}
		if err := conn.WriteMessage(websocket.TextMessage, record.RecordToCSV()); err != nil {
			break
		}

	}
	log.Printf("subscribeParsedHandler out")

}

func subscribeRaw(rw http.ResponseWriter, req *http.Request) {

	conn, err := upgrader.Upgrade(rw, req, nil)
	if err != nil {
		log.Printf(err.Error())
		return
	}

	filename := req.URL.Query().Get("filename")
	t := tail.NewTail(filename)
	t.SeekToEnd = true
	t.RetryFileOpen = true
	t.Context = req.Context()
	t.Follow = true
	t.Start()
	for {

		line, ok := <-t.LineChan
		if !ok {
			return
		}

		p := []byte(line)
		if err := conn.WriteMessage(websocket.TextMessage, p); err != nil {
			break
		}
	}
	log.Printf("subscribeRaw out")

}

func connect(ctx context.Context, serverIP, filename string, parsed bool, lines chan Event) {

	parsedStr := ""
	if parsed {
		parsedStr = "_parsed"
	}

	log.Printf("subscribing to %s", serverIP)
	url := fmt.Sprintf("ws://%s:8888/subscribe%s?filename=%s", serverIP, parsedStr, filename)

	c, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		log.Printf("dial:", err)
		return
	}

	go func() {
		<-ctx.Done()
		// parent routine is blocking closing the websocket
		// will break it out
		c.Close()
	}()

	for {
		_, message, err := c.ReadMessage()
		if err != nil {
			break
		}
		lines <- Event{ServerIP: serverIP, Line: string(message)}
	}

	log.Printf("unsubscribing")

}

func liveHandler(rw http.ResponseWriter, req *http.Request) {

	html, err := ioutil.ReadFile("./live.html")
	if err != nil {
		http.Error(rw, "live.html not found on disc", http.StatusInternalServerError)
		return
	}

	log.Printf("user connected to log")
	rw.Header().Set("Cache-Control", "no-cache")
	rw.Header().Set("Access-Control-Allow-Origin", "*")

	fmt.Fprintf(rw, "%s", html)

}

func scriptHandler(rw http.ResponseWriter, req *http.Request) {

	log.Printf("script requested")

	html, err := ioutil.ReadFile("./script.js")
	if err != nil {
		http.Error(rw, "script.js not found on disc", http.StatusInternalServerError)
		return
	}

	rw.Header().Set("Cache-Control", "no-cache")
	rw.Header().Set("Access-Control-Allow-Origin", "*")

	fmt.Fprintf(rw, "%s", html)

}

type ListFilesHandler struct {
	config ConfigFile
}

func (l *ListFilesHandler) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	rw.Header().Add("Cache-Control", "no-cache, no-store, must-revalidate")
	rw.Header().Add("Access-Control-Allow-Origin", "*")
	rw.Header().Add("Access-Control-Allow-Methods", "*")
	rw.Header().Add("Access-Control-Allow-Headers", "Content-Type")
	rw.Header().Add("Access-Control-Max-Age", "3600")

	resp := struct {
		FileList []Logfile `json:"file_list"`
	}{
		FileList: l.config.Logfiles,
	}

	w := new(bytes.Buffer)
	err := json.NewEncoder(w).Encode(resp)
	if err != nil {
		http.Error(rw, "json encoding failed", http.StatusInternalServerError)
		return
	}
	fmt.Fprint(rw, w.String())
}

type Group struct {
	Name      string `json:"name"`
	Instances []*autoscaling.Instance
}

func getInstanceId() (string, error) {
	// TODO Update to use metadata
	const instanceIdUrl = "http://169.254.169.254/latest/meta-data/instance-id"
	resp, err := http.Get(instanceIdUrl)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	data, err := ioutil.ReadAll(resp.Body)

	return string(data), err
}
