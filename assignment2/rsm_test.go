package main

import (
	"testing"
	"fmt"
	//"math/rand"
)

func TestAppendEntriesRPC(t *testing.T){
	var sm StateMachine
	sm = StateMachine{currTerm: 2,serverIds: []int{5,13,21,55},selfid:34,currState:"follower"}
	sm.clusterSize = len(sm.serverIds)+1
	var loc_entries []logEntry
	loc_entries = []logEntry{
			{term:1,command:[]byte{1,2,3}},
			{term:2,command:[]byte{4,5,6}},
			}
	z := sm.ProcessEvent(
	AppendEntriesReq{
	senderTerm : 1, 
	senderId: 5, 
	prevLogIndex: 100, 
	prevLogTerm: 3, 
	entries: loc_entries,
	senderCommitIndex: 1})
	f,ok := z[0].(Send)
	if ok{
	fmt.Printf("%v\n", f)
	}		
/*	sm.currState = 3
	a:=sm.ProcessEvent(VoteReqEv{candidateId:10,term:20,lastLogIndex:30,lastLogTerm:5})
	
	sm.state=2
	z :=[]byte{1,2,3,4}
	a:= sm.ProcessEvent(Append{data:z})
	
	sm.state=2
	a := sm.ProcessEvent(Timeout{})
		
	sm.state = 3
	a:=sm.ProcessEvent(AppendEntriesRespEv{senderId: 1, senderTerm: 3, response:true})

	sm.state = 3
	a:=sm.ProcessEvent(VoteRespEv{senderTerm: 3, response:true})
	
*/
	/*sm.state=3
	z :=[]byte{1,2,3,4}
	a:= sm.ProcessEvent(Append{data:z})
	f,ok := a[0].(LogStore)
	//fmt.Printf("%v\n", x)
	//fmt.Println("Error is:", a[0])
	if ok {
	fmt.Printf("%v\n", f)	
	}*/

	//z :=[]byte{1,2,3,4}
	/*sm.state = 1
	sm.timer = 11
	fmt.Println(rand.Intn(2*sm.timer-sm.timer)+sm.timer)
	/*a:=sm.ProcessEvent(Timeout{})
	f,ok := a[0].(Alarm)
	if ok {
	fmt.Printf("%v\n", f)	
	}*/
	



}
/*
func TestAppendFollower(t *testing.T){
	var sm StateMachine

}*/
