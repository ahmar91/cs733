package main

import (
	//"fmt"
	//"github.com/cs733-iitb/cluster"
	//"github.com/cs733-iitb/cluster/mock"
	"strconv"
	"testing"
	"time"
	"fmt"
	"errors"
)

var my_clust = []NetConfig{
	{Id: 101, Host: "127.0.0.1", Port: 9001},
	{Id: 102, Host: "127.0.0.1", Port: 9002},
	{Id: 103, Host: "127.0.0.1", Port: 9003},
	{Id: 104, Host: "127.0.0.1", Port: 9004},
	{Id: 105, Host: "127.0.0.1", Port: 9005}}


func makeRaft(clust []NetConfig, index int) Node {
	file_dir := "node"
	conf := Config{Cluster: clust, Id: clust[index].Id, LogFileDir: file_dir + strconv.Itoa(clust[index].Id) + "/log", ElectionTimeout: 800, HeartbeatTimeout: 400}
	raft_node := New(conf)
	return raft_node
}


func makeRafts(clust []NetConfig) []Node {
	var nodes []Node

	for i, _ := range clust {
		raft_node := makeRaft(clust, i)
		nodes = append(nodes, raft_node)
	}

	return nodes
}


func getLeader(nodes []Node) (Node, error) {
	var id, leader_id int
	var err error
	for _, rn := range nodes {
		id, err = rn.Id()
		if err != nil {
			continue
		}
		leader_id, err = rn.LeaderId()
		if err != nil {
			continue
		}
		if id == leader_id {
			return rn, nil
		}
	}
	return nil, errors.New("ERR_NO_LEADER_FOUND")
}


///*
func TestBasic(t *testing.T) {
	rafts := makeRafts(my_clust) // array of []raft.Node
	//fmt.Println(rafts)
	//time.Sleep(time.Second * 2)
	
	ldr, err := getLeader(rafts)
	for err != nil {
		fmt.Println("No leader elected yet")
//		ldr, err = getLeader(rafts)
	}
	//fmt.Println(ldr)
	data := "foo"
	ok := errors.New("err")
	for ok != nil {
		ldr, ok = getLeader(rafts)
	}
	res := ldr.Append([]byte(data))
	if res != nil {
		t.Error("Trying append on shutdown leader")
	}

	time.Sleep(2 * time.Second)

	for _, rn := range rafts {
		ch, _ := rn.CommitChannel()
		//node := rn.(*RaftNode)
		select {
		case c := <- ch:
			//fmt.Println("data=====",node.sm.id,string(c.Data))
			if c.Err.Errortype != "" {
				t.Error(c.Err)
			} else if string(c.Data) != data {
				t.Error("basic: Different data")
			}
		}
	}	
}
//*/
