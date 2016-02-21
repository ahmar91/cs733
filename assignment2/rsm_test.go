package main

import (
	"testing"
	"fmt"
	"strconv"
	//"math/rand"
)

func TestAppend(t *testing.T){
	var sm StateMachine
	sm = StateMachine{currTerm: 2,serverIds: []int{5,13,21,55},selfid:34,leaderId:34,currState:"leader"}
	sm.clusterSize = len(sm.serverIds)+1
	sm.log = make([]logEntry,5)
	for i := 0;i<len(sm.log);i++{
		sm.log[i].term = i+1
		sm.log[i].command = []byte{'j','m','p'}
	}
	sm.nextIndex = make(map[int]int)
	for i := 0;i<len(sm.serverIds);i++{
		sm.nextIndex[sm.serverIds[i]] = 1
	}
//	fmt.Println("hello")
	z := sm.ProcessEvent(Append{data:[]byte{'m','o','v'}})
//	fmt.Println("world")
	var numSend int
	var numLogStore int
	var incorrectAction int
	for i := 0; i<len(z);i++ {
		switch z[i].(type){
		case Send:
			numSend++
		case LogStore:
			numLogStore++
		default:
			incorrectAction++
		}
	}
	// fmt.Println("num of sends: " + strconv.Itoa(numSend))
	// fmt.Println("num of log stores " + strconv.Itoa(numLogStore))
	// fmt.Println("num of incorrect actions " + strconv.Itoa(incorrectAction))
//	x := strconv.Itoa(numSend)
	expect(t,strconv.Itoa(numSend),strconv.Itoa(len(sm.serverIds)))
	expect(t,strconv.Itoa(numLogStore),"1")
	expect(t,strconv.Itoa(incorrectAction),"0")
}

func TestAppendEntriesReq(t *testing.T){
	var sm StateMachine
	sm = StateMachine{currTerm: 2,serverIds: []int{5,13,21,55},selfid:34,timer:1000,currState:"follower"}
	sm.clusterSize = len(sm.serverIds)+1
	sm.log = make([]logEntry,10)
	for i := 0;i<len(sm.log);i++{
		sm.log[i].term = i+1
		sm.log[i].command = []byte{'j','m','p'}
	}
	sm.nextIndex = make(map[int]int)
	for i := 0;i<len(sm.serverIds);i++{
		sm.nextIndex[sm.serverIds[i]] = 1
	}	
	var loc_entries []logEntry
	loc_entries = []logEntry{
			{term:1,command:[]byte{'c','m','p'}},
			{term:2,command:[]byte{'a','d','d'}},
			}
	z := sm.ProcessEvent(
	AppendEntriesReq{
	senderTerm : 3, 
	senderId: 5, 
	prevLogIndex: 3, 
	prevLogTerm: 4, 
	entries: loc_entries,
	senderCommitIndex: 1})
	var numSend int
	var numLogStore int
	var numAlarm int
	var numStateStore int
	var numCommit int
	var incorrectAction int
	for i := 0; i<len(z);i++ {
		switch z[i].(type){
		case Send:
			numSend++
		case LogStore:
			numLogStore++
		case Alarm:
			numAlarm++
		case StateStore:
			numStateStore++
		case Commit:
			numCommit++
		default:
			incorrectAction++
		}
	}
	// fmt.Println("num of sends: " + strconv.Itoa(numSend))
	// fmt.Println("num of log stores " + strconv.Itoa(numLogStore))
	// fmt.Println("num of alarms " + strconv.Itoa(numAlarm))
	// fmt.Println("num of state stores " + strconv.Itoa(numStateStore))
	// fmt.Println("num of commits " + strconv.Itoa(numCommit))
	// fmt.Println("num of incorrect actions " + strconv.Itoa(incorrectAction))
	expect(t,strconv.Itoa(numSend),"1")
	expect(t,strconv.Itoa(numLogStore),"2")
	expect(t,strconv.Itoa(numAlarm),"1")
	expect(t,strconv.Itoa(numStateStore),"1")
	expect(t,strconv.Itoa(numCommit),"1")
	expect(t,strconv.Itoa(incorrectAction),"0")		
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
	}
*/
	
/*
func TestAppendFollower(t *testing.T){
	var sm StateMachine

}*/
// Useful testing function
func expect(t *testing.T, a string, b string) {
        if a != b {
                t.Error(fmt.Sprintf("Expected %v, found %v", b, a)) // t.Error is visible when running `go test -verbose`
        }
}