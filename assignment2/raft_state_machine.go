package main
import (
"math"
"math/rand"
"time"
)

type Action interface {
}

type Event interface {
}

type StateMachine struct {
	//persistent state variables
	currTerm int
	votedFor int
	log []logEntry
	serverIds []int // other server ids
	selfid int // server id
	clusterSize int
	timer int
	currState string
	lastLogIndex int
	lastLogTerm int
	// non-persistent state variables
	commitIndex int
	leaderId int
	hasVoted map[int]int // applicable to candidates only, size = clusterSize, 1 for positive vote, 0 for no vote, -1 for negative vote 
	nextIndex map[int]int // applicable to leader only
	matchIndex map[int]int // applicable to leader only
}

type Append struct {
	data []byte
}

type Timeout struct {
}

type AppendEntriesReq struct {
	senderTerm int
	senderId int
	prevLogIndex int
	prevLogTerm int
	entries []logEntry
	senderCommitIndex int
}

type AppendEntriesResp struct {
	senderId int
	senderTerm int
	lastMatchInd int
	response bool
}

type VoteReq struct {
	senderId int
	senderTerm int
	senderLastLogIndex int
	senderLastLogTerm int
}

type VoteResp struct {
	senderId int
	senderTerm int
	response bool
}

type Error struct {
	errortype string // Can be of the following types:
					 // 1. ERR_GOTO_LEADER
					 // 2. ERR_WAIT_FOR_ELECTION
					 // 3. ""(if no error)
}

type logEntry struct {
	term int
	command []byte
}

type Send struct {
	peerId int
	event Event
}

type Commit struct {
	index int
	data []byte
	id int // set to leaderId to redirect the request to leader
	err Error
}

type Alarm struct {
	t int
}

type LogStore struct { 
	index int
	entry logEntry
}

type StateStore struct {
	currTerm int
	votedFor int
}

func random(min, max int) int {
    rand.Seed(time.Now().Unix())
    return rand.Intn(max - min) + min
}

func handleAppend(sm *StateMachine, cmd *Append) []Action{
	var action []Action
	if sm.currState == "follower"{
		var cmt Commit
		cmt = Commit{index:-1,data:cmd.data,id:sm.leaderId,err:Error{errortype:"ERR_GOTO_LEADER"}}	
		action = append(action,cmt)
	}else if sm.currState == "candidate"{
		var cmt Commit
		cmt = Commit{index:-1,data:cmd.data,id:-1,err:Error{errortype:"ERR_WAIT_FOR_ELECTION"}}
		action = append(action,cmt)
	}else{
		sm.lastLogIndex++
		sm.lastLogTerm = sm.currTerm
		var lgstr LogStore
		lgstr = LogStore{index:sm.lastLogIndex,entry:logEntry{term:sm.currTerm,command:cmd.data}}
		action = append(action,lgstr)
		for i := 0; i<len(sm.serverIds); i++ {
			loc_prevLogIndex := sm.nextIndex[sm.serverIds[i]]-1
			loc_prevLogTerm := sm.log[loc_prevLogIndex].term
			loc_entries := sm.log[sm.nextIndex[sm.serverIds[i]]:]
			var appenreq AppendEntriesReq	
			appenreq = AppendEntriesReq{senderTerm:sm.currTerm,senderId:sm.selfid,prevLogIndex:loc_prevLogIndex,prevLogTerm:loc_prevLogTerm,entries:loc_entries,senderCommitIndex:sm.commitIndex}
			var snd Send
			snd = Send{peerId:sm.serverIds[i],event:appenreq}
			action = append(action,snd)
		}
	}
	return action
}

func handleTimeout(sm *StateMachine) []Action{
	var action []Action
	if sm.currState == "leader" {
		var alm Alarm
		alm = Alarm{t:random(sm.timer,2*sm.timer)}
		action = append(action,alm)
		for i := 0; i < len(sm.serverIds); i++ {
			loc_prevLogIndex := sm.nextIndex[sm.serverIds[i]] - 1
			loc_prevLogTerm := sm.log[loc_prevLogIndex].term
			var appenreq AppendEntriesReq
			appenreq = AppendEntriesReq{senderTerm:sm.currTerm,senderId:sm.selfid,prevLogIndex:loc_prevLogIndex,prevLogTerm:loc_prevLogTerm,entries:[]logEntry{},senderCommitIndex:sm.commitIndex}
			var snd Send
			snd = Send{peerId:sm.serverIds[i],event:appenreq}
			action = append(action,snd)
		}
	}else{
		sm.currState = "candidate"
		sm.currTerm++
		sm.hasVoted = make(map[int]int)
		sm.hasVoted[sm.selfid] = 1
		sm.votedFor = sm.selfid
		var ststr StateStore
		ststr = StateStore{currTerm:sm.currTerm,votedFor:sm.votedFor}
		action = append(action,ststr)
		var alm Alarm
		alm = Alarm{t:random(sm.timer,2*sm.timer)}
		action = append(action,alm)
		for i := 0; i < len(sm.serverIds); i++ {
			var snd Send
			snd = Send{peerId:sm.serverIds[i],event:VoteReq{senderId:sm.selfid,senderTerm:sm.currTerm,senderLastLogIndex:sm.lastLogIndex,senderLastLogTerm:sm.lastLogTerm}}
			action = append(action,snd)
		}
	}
	return action
}

func handleAppendEntriesReq(sm *StateMachine, cmd *AppendEntriesReq) []Action{
	var action []Action
	if sm.currTerm > cmd.senderTerm {
		var snd Send
		snd = Send{peerId : cmd.senderId, event: AppendEntriesResp{senderId:sm.selfid,senderTerm:sm.currTerm,response:false,lastMatchInd:sm.commitIndex}}
		action = append(action,snd)
		return action
	}
	var alm Alarm
	alm = Alarm{t:random(sm.timer,2*sm.timer)}
	action = append(action,alm)
	sm.currState = "follower" //handles all cases
	sm.currTerm = cmd.senderTerm
	sm.votedFor = -1
	sm.hasVoted = make(map[int]int)
	var ststr StateStore
	ststr = StateStore{currTerm:sm.currTerm,votedFor:sm.votedFor}
	action = append(action,ststr)
	sm.leaderId = cmd.senderId
	if sm.log[cmd.prevLogIndex].term != cmd.prevLogTerm {
		var snd Send
		snd = Send{peerId:cmd.senderId,event:AppendEntriesResp{senderId:sm.selfid,senderTerm:sm.currTerm,response:false,lastMatchInd:sm.commitIndex}}
		action = append(action,snd)
		return action
	}
	for i := 0; i<len(cmd.entries);i++ {
		var lgstr LogStore
		lgstr = LogStore{index:cmd.prevLogIndex+1+i,entry:cmd.entries[i]}
		action = append(action,lgstr)
	}
	sm.lastLogIndex = cmd.prevLogIndex + len(cmd.entries)
	sm.lastLogTerm = sm.log[sm.lastLogIndex].term
	sm.commitIndex = int(math.Min(float64(cmd.senderCommitIndex),float64(sm.lastLogIndex)))
	var cmt Commit
	cmt = Commit{index:sm.commitIndex,data:(sm.log[sm.commitIndex]).command,id:sm.selfid,err:Error{errortype:""}}
	action = append(action,cmt)	
	var snd Send
	snd = Send{peerId : cmd.senderId, event: AppendEntriesResp{senderId:sm.selfid,senderTerm:sm.currTerm,response:true,lastMatchInd:sm.commitIndex}}
	action = append(action,snd)
	return action
}

func handleAppendEntriesResp(sm *StateMachine, cmd *AppendEntriesResp) []Action{
	var action []Action
	if sm.currState == "leader" {
		if sm.currTerm < cmd.senderTerm {
			sm.currState = "follower"
			sm.currTerm = cmd.senderTerm
			sm.votedFor = -1
			sm.hasVoted = make(map[int]int)
			var ststr StateStore
			ststr = StateStore{currTerm:sm.currTerm,votedFor:sm.votedFor}
			action = append(action,ststr)			
			return action
		}
		if cmd.response == false {
			sm.nextIndex[cmd.senderId] = cmd.lastMatchInd + 1
			loc_prevLogIndex := sm.nextIndex[cmd.senderId]-1
			loc_prevLogTerm := sm.log[loc_prevLogIndex].term
			loc_entries := sm.log[sm.nextIndex[cmd.senderId]:sm.lastLogIndex]
			var appenreq AppendEntriesReq
			appenreq = AppendEntriesReq{senderTerm:sm.currTerm,senderId:sm.selfid,prevLogIndex:loc_prevLogIndex,prevLogTerm:loc_prevLogTerm,entries:loc_entries,senderCommitIndex:sm.commitIndex}
			var snd Send
			snd = Send{peerId:cmd.senderId,event:appenreq}
			action = append(action,snd)
			return action			
		}else{
			sm.nextIndex[cmd.senderId] = cmd.lastMatchInd+1
			sm.matchIndex[cmd.senderId] = cmd.lastMatchInd
			var count int
			for i := sm.matchIndex[cmd.senderId];i>sm.commitIndex;i--{
				count = 1
				for _,value := range sm.matchIndex{
					if value>=i{
						count++
					}
				}
				if count > sm.clusterSize/2 && sm.log[i].term == cmd.senderTerm{
					sm.commitIndex = i
					var cmt Commit
					cmt = Commit{index:sm.commitIndex,data:sm.log[sm.commitIndex].command,id:sm.selfid,err:Error{errortype:""}}
					action = append(action,cmt)
					break
				}
			}
			if sm.lastLogIndex > sm.matchIndex[cmd.senderId]{
				loc_prevLogIndex := sm.nextIndex[cmd.senderId]-1
				loc_prevLogTerm := sm.log[loc_prevLogIndex].term
				loc_entries := sm.log[sm.nextIndex[cmd.senderId]:sm.lastLogIndex]
				var appenreq AppendEntriesReq
				appenreq = AppendEntriesReq{senderTerm:sm.currTerm,senderId:sm.selfid,prevLogIndex:loc_prevLogIndex,prevLogTerm:loc_prevLogTerm,entries:loc_entries,senderCommitIndex:sm.commitIndex}
				var snd Send
				snd = Send{peerId:cmd.senderId,event:appenreq}
				action = append(action,snd)
			}
			return action
		}
	}
	return action
}

func handleVoteReq(sm *StateMachine, cmd *VoteReq) []Action{
	var action []Action
	if sm.currState == "follower"{
		if sm.currTerm > cmd.senderTerm{
			var snd Send
			snd = Send{peerId:cmd.senderId,event:VoteResp{senderId:sm.selfid,senderTerm:sm.currTerm,response:false}}
			action = append(action,snd)
			return action
		}
		if sm.lastLogTerm > cmd.senderLastLogTerm || (sm.lastLogTerm == cmd.senderLastLogTerm && sm.lastLogIndex>cmd.senderLastLogIndex){
			var snd Send
			snd = Send{peerId:cmd.senderId,event:VoteResp{senderId:sm.selfid,senderTerm:sm.currTerm,response:false}}
			action = append(action,snd)
			return action
		}
		if sm.votedFor != -1 && sm.votedFor != cmd.senderId{
			var snd Send
			snd = Send{peerId:cmd.senderId,event:VoteResp{senderId:sm.selfid,senderTerm:sm.currTerm,response:false}}
			action = append(action,snd)
			return action		
		}
		sm.votedFor = cmd.senderId
		sm.hasVoted = make(map[int]int)
		sm.hasVoted[cmd.senderId] = 1		
		var ststr StateStore
		ststr = StateStore{currTerm:sm.currTerm,votedFor:sm.votedFor}
		action = append(action,ststr)
		var snd Send
		snd = Send{peerId:cmd.senderId,event:VoteResp{senderId:sm.selfid,senderTerm:sm.currTerm,response:true}}
		action = append(action,snd)
	}else{
		if sm.currTerm < cmd.senderTerm{
			sm.currState = "follower"
			sm.currTerm = cmd.senderTerm
			sm.votedFor = -1
			sm.hasVoted = make(map[int]int)
			var ststr StateStore
			ststr = StateStore{currTerm:sm.currTerm,votedFor:sm.votedFor}
			action = append(action,ststr)
			if sm.lastLogTerm > cmd.senderLastLogTerm || (sm.lastLogTerm == cmd.senderLastLogTerm && sm.lastLogIndex>cmd.senderLastLogIndex){
				var snd Send
				snd = Send{peerId:cmd.senderId,event:VoteResp{senderId:sm.selfid,senderTerm:sm.currTerm,response:false}}
				action = append(action,snd)
				return action
			}
			sm.votedFor = cmd.senderId
			sm.hasVoted = make(map[int]int)
			sm.hasVoted[cmd.senderId] = 1	
			ststr = StateStore{currTerm:sm.currTerm,votedFor:sm.votedFor}
			action = append(action,ststr)	
			var snd Send
			snd = Send{peerId:cmd.senderId,event:VoteResp{senderId:sm.selfid,senderTerm:sm.currTerm,response:true}}
			action = append(action,snd)
			return action			
		}
		var snd Send
		snd = Send{peerId:cmd.senderId,event:VoteResp{senderId:sm.selfid,senderTerm:sm.currTerm,response:false}}
		action = append(action,snd)
	}
	return action	
}

func handleVoteResp(sm *StateMachine, cmd *VoteResp) []Action{
	var action []Action
	if sm.currTerm < cmd.senderTerm{
		sm.currState = "follower"
		sm.currTerm = cmd.senderTerm
		var ststr StateStore
		ststr = StateStore{currTerm:sm.currTerm,votedFor:sm.votedFor}
		action = append(action,ststr)		
	}	
	if sm.currState == "candidate"{
		if cmd.response == true{
			sm.hasVoted[cmd.senderId] = 1
			var numVotes int
			numVotes = 0
			for key,_ := range sm.hasVoted{
				if sm.hasVoted[key] == 1{
					numVotes++
				}
			}
			if numVotes > sm.clusterSize/2{
				sm.currState = "leader"
				sm.leaderId = sm.selfid
				for i := 0;i<len(sm.serverIds); i++{
					loc_prevLogIndex := sm.nextIndex[sm.serverIds[i]]-1
					loc_prevLogTerm := sm.log[loc_prevLogIndex].term
					var appenreq AppendEntriesReq
					appenreq = AppendEntriesReq{senderTerm:sm.currTerm,senderId:sm.selfid,prevLogIndex:loc_prevLogIndex,prevLogTerm:loc_prevLogTerm,entries:[]logEntry{},senderCommitIndex:sm.commitIndex}
					var snd Send
					snd = Send{peerId:sm.serverIds[i],event:appenreq}
					action = append(action,snd)				
				}
			}
		}else{
			sm.hasVoted[cmd.senderId] = -1
			var numVotes int
			numVotes = 0
			for key,_ := range sm.hasVoted{
				if sm.hasVoted[key] == -1{
					numVotes++
				}
			}
			if numVotes > sm.clusterSize/2{
				sm.currState = "follower"
			}			
		}
	}
	return action
}

func (sm *StateMachine) ProcessEvent (ev Event) []Action{
	var action []Action
	switch ev.(type) {
		case Append:
			cmd := ev.(Append)
			action = handleAppend(sm,&cmd)
		case Timeout:
			_ = ev.(Timeout)	
			action = handleTimeout(sm)	
		case AppendEntriesReq:
			cmd := ev.(AppendEntriesReq)	
			action = handleAppendEntriesReq(sm,&cmd)
		case AppendEntriesResp:
			cmd := ev.(AppendEntriesResp)	
			action = handleAppendEntriesResp(sm,&cmd)
		case VoteReq:
			cmd := ev.(VoteReq)	
			action = handleVoteReq(sm,&cmd)
		case VoteResp:
			cmd := ev.(VoteResp)	
			action = handleVoteResp(sm,&cmd)
		default: println ("Unrecognized")
	}
	return action
}
