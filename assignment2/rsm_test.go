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

func TestTimeout(t *testing.T){
	var sm StateMachine
	sm = StateMachine{currTerm: 4,serverIds: []int{5,13,21,55},selfid:34,timer:1000,currState:"leader"}
	sm.clusterSize = len(sm.serverIds)+1
	sm.log = make([]logEntry,20)
	for i := 0;i<len(sm.log);i++{
		sm.log[i].term = i+1
		sm.log[i].command = []byte{'j','m','p'}
	}
	sm.lastLogIndex = 19
	sm.nextIndex = make(map[int]int)
	sm.matchIndex = make(map[int]int)
	for i := 0;i<len(sm.serverIds);i++{
		sm.nextIndex[sm.serverIds[i]] = 1
		sm.matchIndex[sm.serverIds[i]] = 0
	}	
	z := sm.ProcessEvent(Timeout{})
	var numSend int
	var numLogStore int
	var numAlarm int
	var numStateStore int
	var numCommit int
	var incorrectAction int
	for i := 0; i<len(z);i++ {
		switch z[i].(type){
		case Send:
			a,_ := z[i].(Send)
			_,ok := a.event.(AppendEntriesReq)
			if ok{
				numSend++
			}else{
				incorrectAction++
			}
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
	expect(t,strconv.Itoa(numSend),"4")
	expect(t,strconv.Itoa(numLogStore),"0")
	expect(t,strconv.Itoa(numAlarm),"1")
	expect(t,strconv.Itoa(numStateStore),"0")
	expect(t,strconv.Itoa(numCommit),"0")
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
			a,_ := z[i].(Send)
			_,ok := a.event.(AppendEntriesResp)
			if ok{
				numSend++
			}else{
				incorrectAction++
			}
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

func TestAppendEntriesResp(t *testing.T){
	var sm StateMachine
	sm = StateMachine{currTerm: 4,serverIds: []int{5,13,21,55},selfid:34,timer:1000,currState:"leader"}
	sm.clusterSize = len(sm.serverIds)+1
	sm.log = make([]logEntry,20)
	for i := 0;i<len(sm.log);i++{
		sm.log[i].term = i+1
		sm.log[i].command = []byte{'j','m','p'}
	}
	sm.lastLogIndex = 19
	sm.nextIndex = make(map[int]int)
	sm.matchIndex = make(map[int]int)
	for i := 0;i<len(sm.serverIds);i++{
		sm.nextIndex[sm.serverIds[i]] = 1
		sm.matchIndex[sm.serverIds[i]] = 0
	}	
	z := sm.ProcessEvent(
	AppendEntriesResp{
	senderTerm : 3, 
	senderId: 5, 
	lastMatchInd: 2,
	response: false})
	var numSend int
	var numLogStore int
	var numAlarm int
	var numStateStore int
	var numCommit int
	var incorrectAction int
	for i := 0; i<len(z);i++ {
		switch z[i].(type){
		case Send:
			a,_ := z[i].(Send)
			_,ok := a.event.(AppendEntriesReq)
			if ok{
				numSend++
			}else{
				incorrectAction++
			}
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
	expect(t,strconv.Itoa(numLogStore),"0")
	expect(t,strconv.Itoa(numAlarm),"0")
	expect(t,strconv.Itoa(numStateStore),"0")
	expect(t,strconv.Itoa(numCommit),"0")
	expect(t,strconv.Itoa(incorrectAction),"0")		
}

// Useful testing function
func expect(t *testing.T, a string, b string) {
        if a != b {
                t.Error(fmt.Sprintf("Expected %v, found %v", b, a)) // t.Error is visible when running `go test -verbose`
        }
}