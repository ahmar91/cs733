package main

import (
	"testing"
	"fmt"
	"strconv"
)

func TestAppend(t *testing.T){
	var sm StateMachine
	sm = StateMachine{currTerm: 2,serverIds: []int{5,13,21,55},selfId:34,leaderId:34,currState:"leader"}
	sm.clusterSize = len(sm.serverIds)+1
	sm.log = make([]LogEntry,5)
	for i := 0;i<len(sm.log);i++{
		sm.log[i].Term = i+1
		sm.log[i].Command = []byte{'j','m','p'}
	}
	sm.nextIndex = make(map[int]int64)
	for i := 0;i<len(sm.serverIds);i++{
		sm.nextIndex[sm.serverIds[i]] = 1
	}
	z := sm.ProcessEvent(Append{Data:[]byte{'m','o','v'}})
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
	expect(t,strconv.Itoa(numSend),strconv.Itoa(len(sm.serverIds)))
	expect(t,strconv.Itoa(numLogStore),"1")
	expect(t,strconv.Itoa(incorrectAction),"0")
}

func TestTimeout(t *testing.T){
	var sm StateMachine
	sm = StateMachine{currTerm: 4,serverIds: []int{5,13,21,55},selfId:34,timer:1000,currState:"leader"}
	sm.clusterSize = len(sm.serverIds)+1
	sm.log = make([]LogEntry,20)
	for i := 0;i<len(sm.log);i++{
		sm.log[i].Term = i+1
		sm.log[i].Command = []byte{'j','m','p'}
	}
	sm.lastLogIndex = 19
	sm.nextIndex = make(map[int]int64)
	sm.matchIndex = make(map[int]int64)
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
			_,ok := a.Event.(AppendEntriesReq)
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
	expect(t,strconv.Itoa(numSend),"4")
	expect(t,strconv.Itoa(numLogStore),"0")
	expect(t,strconv.Itoa(numAlarm),"1")
	expect(t,strconv.Itoa(numStateStore),"0")
	expect(t,strconv.Itoa(numCommit),"0")
	expect(t,strconv.Itoa(incorrectAction),"0")
}

func TestAppendEntriesReq(t *testing.T){
	var sm StateMachine
	sm = StateMachine{currTerm: 2,serverIds: []int{5,13,21,55},selfId:34,timer:1000,currState:"follower",electionAlarmPeriod:5,heartbeatAlarmPeriod:4}
	sm.clusterSize = len(sm.serverIds)+1
	sm.log = make([]LogEntry,10)
	for i := 0;i<len(sm.log);i++{
		sm.log[i].Term = i+1
		sm.log[i].Command = []byte{'j','m','p'}
	}
	sm.nextIndex = make(map[int]int64)
	for i := 0;i<len(sm.serverIds);i++{
		sm.nextIndex[sm.serverIds[i]] = 1
	}	
	var loc_entries []LogEntry
	loc_entries = []LogEntry{
			{Term:1,Command:[]byte{'c','m','p'}},
			{Term:2,Command:[]byte{'a','d','d'}},
			}
	z := sm.ProcessEvent(
	AppendEntriesReq{
	SenderTerm : 3, 
	SenderId: 5, 
	PrevLogIndex: 3, 
	PrevLogTerm: 4, 
	Entries: loc_entries,
	SenderCommitIndex: 1})
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
			_,ok := a.Event.(AppendEntriesResp)
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
	expect(t,strconv.Itoa(numSend),"1")
	expect(t,strconv.Itoa(numLogStore),"0")
	expect(t,strconv.Itoa(numAlarm),"1")
	expect(t,strconv.Itoa(numStateStore),"1")
	expect(t,strconv.Itoa(numCommit),"0")
	expect(t,strconv.Itoa(incorrectAction),"0")		
}

func TestAppendEntriesResp(t *testing.T){
	var sm StateMachine
	sm = StateMachine{currTerm: 4,serverIds: []int{5,13,21,55},selfId:34,timer:1000,currState:"leader",electionAlarmPeriod:5,heartbeatAlarmPeriod:4}
	sm.clusterSize = len(sm.serverIds)+1
	sm.log = make([]LogEntry,20)
	for i := 0;i<len(sm.log);i++{
		sm.log[i].Term = i+1
		sm.log[i].Command = []byte{'j','m','p'}
	}
	sm.lastLogIndex = 19
	sm.nextIndex = make(map[int]int64)
	sm.matchIndex = make(map[int]int64)
	for i := 0;i<len(sm.serverIds);i++{
		sm.nextIndex[sm.serverIds[i]] = 1
		sm.matchIndex[sm.serverIds[i]] = 0
	}	
	z := sm.ProcessEvent(
	AppendEntriesResp{
	SenderTerm : 3, 
	SenderId: 5, 
	SenderLastRepIndex: 2,
	Response: false})
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
			_,ok := a.Event.(AppendEntriesReq)
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
	expect(t,strconv.Itoa(numSend),"1")
	expect(t,strconv.Itoa(numLogStore),"0")
	expect(t,strconv.Itoa(numAlarm),"0")
	expect(t,strconv.Itoa(numStateStore),"0")
	expect(t,strconv.Itoa(numCommit),"0")
	expect(t,strconv.Itoa(incorrectAction),"0")		
}

func TestVoteReq(t *testing.T){
	var sm StateMachine
	sm = StateMachine{currTerm: 2,serverIds: []int{5,13,21,55},selfId:34,timer:1000,currState:"follower",electionAlarmPeriod:5,heartbeatAlarmPeriod:4}
	sm.clusterSize = len(sm.serverIds)+1
	sm.log = make([]LogEntry,20)
	for i := 0;i<len(sm.log);i++{
		sm.log[i].Term = i+1
		sm.log[i].Command = []byte{'j','m','p'}
	}
	sm.lastLogIndex = 19
	sm.lastLogTerm = 4
	sm.votedFor = -1
	sm.nextIndex = make(map[int]int64)
	sm.matchIndex = make(map[int]int64)
	for i := 0;i<len(sm.serverIds);i++{
		sm.nextIndex[sm.serverIds[i]] = 1
		sm.matchIndex[sm.serverIds[i]] = 0
	}	
	z := sm.ProcessEvent(VoteReq{SenderId:21,SenderTerm:3,SenderLastLogIndex:13,SenderLastLogTerm:5})
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
			_,ok := a.Event.(VoteResp)
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
	expect(t,strconv.Itoa(numSend),"1")
	expect(t,strconv.Itoa(numLogStore),"0")
	expect(t,strconv.Itoa(numAlarm),"1")
	expect(t,strconv.Itoa(numStateStore),"2")
	expect(t,strconv.Itoa(numCommit),"0")
	expect(t,strconv.Itoa(incorrectAction),"0")	
}

func TestVoteResp(t *testing.T){
	var sm StateMachine
	sm = StateMachine{currTerm: 3,serverIds: []int{5,13},selfId:34,timer:1000,currState:"follower",electionAlarmPeriod:5,heartbeatAlarmPeriod:4}
	sm.clusterSize = len(sm.serverIds)+1
	sm.log = make([]LogEntry,20)
	for i := 0;i<len(sm.log);i++{
		sm.log[i].Term = i+1
		sm.log[i].Command = []byte{'j','m','p'}
	}
	sm.lastLogIndex = 19
	sm.nextIndex = make(map[int]int64)
	sm.matchIndex = make(map[int]int64)
	for i := 0;i<len(sm.serverIds);i++{
		sm.nextIndex[sm.serverIds[i]] = 1
		sm.matchIndex[sm.serverIds[i]] = 0
	}
	x := sm.ProcessEvent(Timeout{})	
	y := sm.ProcessEvent(VoteResp{SenderId:5,SenderTerm:4,Response:true})
	z := append(x,y...)
	var numSendAppendEntriesReq int
	var numSendVoteReq int
	var numLogStore int
	var numAlarm int
	var numStateStore int
	var numCommit int
	var incorrectAction int
	for i := 0; i<len(z);i++ {
		switch z[i].(type){
		case Send:
			a,_ := z[i].(Send)
			_,ok1 := a.Event.(AppendEntriesReq)
			if ok1{
				numSendAppendEntriesReq++
			}else{
				_,ok2 := a.Event.(VoteReq)
				if ok2{
					numSendVoteReq++
				}else{	
				incorrectAction++
				}
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
	expect(t,strconv.Itoa(numSendAppendEntriesReq),"2")
	expect(t,strconv.Itoa(numSendVoteReq),"2")
	expect(t,strconv.Itoa(numLogStore),"0")
	expect(t,strconv.Itoa(numAlarm),"2")
	expect(t,strconv.Itoa(numStateStore),"1")
	expect(t,strconv.Itoa(numCommit),"0")
	expect(t,strconv.Itoa(incorrectAction),"0")
}
// Useful testing function
func expect(t *testing.T, a string, b string) {
        if a != b {
                t.Error(fmt.Sprintf("Expected %v, found %v", b, a)) // t.Error is visible when running `go test -verbose`
        }
}
