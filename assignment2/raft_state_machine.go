package main
import (
"math/rand"
"time"
)

type Action interface {
}

type Event interface {
}

const (
	lb = 2
	ub = 3
)

type StateMachine struct {
	//persistent state variables
	currTerm int
	votedFor int
	log []LogEntry
	serverIds []int // other server Ids
	selfId int // server Id
	clusterSize int
	currState string
	lastLogIndex int64
	lastLogTerm int
	electionAlarmPeriod int64 //newly added
	heartbeatAlarmPeriod int64 //newly added
	lastRepIndex int64 //newly added
	// non-persistent state variables
	commitIndex int64
	leaderId int
	hasVoted map[int]int // applicable to candIdates only, size = clusterSize, 1 for positive vote, 0 for no vote, -1 for negative vote 
	nextIndex map[int]int64 // applicable to leader only
	matchIndex map[int]int64 // applicable to leader only
}

type Append struct {
	Data []byte
}

type Timeout struct {
}

type AppendEntriesReq struct {
	SenderTerm int
	SenderId int
	PrevLogIndex int64
	PrevLogTerm int
	Entries []LogEntry
	SenderCommitIndex int64
}

type AppendEntriesResp struct {
	SenderId int
	SenderTerm int
	SenderLastRepIndex int64
	Response bool
}

type VoteReq struct {
	SenderId int
	SenderTerm int
	SenderLastLogIndex int64
	SenderLastLogTerm int
}

type VoteResp struct {
	SenderId int
	SenderTerm int
	Response bool
}

type Error struct {
	Errortype string // Can be of the following types:
					 // 1. Err_GOTO_LEADER
					 // 2. Err_WAIT_FOR_ELECTION
					 // 3. ""(if no Error)
}

type LogEntry struct {
	Term int
	Command []byte
}

type Send struct {
	PeerId int
	Event Event
}

type Commit struct {
	Index int64
	Data []byte
	Id int // set to leaderId to redirect the request to leader
	Err Error
}

type Alarm struct {
	T int64
}

type LogStore struct { 
	Index int64
	Entry LogEntry
}

type StateStore struct {
	CurrTerm int
	VotedFor int
	LastRepIndex int64
}

func randRange(min, max int64) int64 {	
	return int64(rand.Intn(int(max - min))) + min
}

func (sm *StateMachine) init(currTerm int, votedFor int, log []LogEntry, selfId int, PeerIds []int, electionAlarmPeriod int64, heartbeatAlarmPeriod int64, lastRepIndex int64, currState string, commitIndex int64, leaderId int, lastLogIndex int64, lastLogTerm int, hasVoted map[int]int, nextIndex map[int]int64, matchIndex map[int]int64) {
	sm.currTerm = currTerm
	sm.votedFor = votedFor
	sm.log = log
	sm.selfId = selfId
	sm.serverIds = PeerIds	
	sm.electionAlarmPeriod = electionAlarmPeriod
	sm.heartbeatAlarmPeriod = heartbeatAlarmPeriod
	sm.lastRepIndex = lastRepIndex
	sm.currState = currState
	sm.commitIndex = commitIndex
	sm.leaderId = leaderId
	sm.lastLogIndex = lastLogIndex
	sm.lastLogTerm = lastLogTerm
	sm.clusterSize = len(PeerIds) + 1
	sm.hasVoted = hasVoted
	sm.nextIndex = nextIndex
	sm.matchIndex = matchIndex

	// For Generating Random Time between timer and 2 * timer
	rand.Seed(time.Now().Unix())
}

func random(min, max int) int {
    rand.Seed(time.Now().Unix())
    return rand.Intn(max - min) + min
}

func min(a, b int64) int64 {
	if a < b {	
		return a
	} else {
	return b
	}
}

func max(a, b int64) int64 {
	if a >= b {	
		return a
	} 
	return b
}

func handleAppend(sm *StateMachine, cmd *Append) []Action{
	var action []Action
	if sm.currState == "follower"{
		var cmt Commit
		cmt = Commit{Index:-1,Data:cmd.Data,Id:sm.leaderId,Err:Error{Errortype:"Err_GOTO_LEADER"}}	
		action = append(action,cmt)
	}else if sm.currState == "candidate"{
		var cmt Commit
		cmt = Commit{Index:-1,Data:cmd.Data,Id:-1,Err:Error{Errortype:"Err_WAIT_FOR_ELECTION"}}
		action = append(action,cmt)
	}else{
		sm.lastLogIndex++
		sm.lastLogTerm = sm.currTerm
		var lgstr LogStore
		lgstr = LogStore{Index:sm.lastLogIndex,Entry:LogEntry{Term:sm.currTerm,Command:cmd.Data}}
		action = append(action,lgstr)
		for i := 0; i<len(sm.serverIds); i++ {
			loc_prevLogIndex := sm.nextIndex[sm.serverIds[i]]-1
			var loc_prevLogTerm int
			if loc_prevLogIndex < 0 {
				loc_prevLogTerm = 0
			} else {	
				loc_prevLogTerm = sm.log[loc_prevLogIndex].Term
			}			
			loc_Entries := sm.log[sm.nextIndex[sm.serverIds[i]]:]
			var appenreq AppendEntriesReq	
			appenreq = AppendEntriesReq{SenderTerm:sm.currTerm,SenderId:sm.selfId,PrevLogIndex:loc_prevLogIndex,PrevLogTerm:loc_prevLogTerm,Entries:loc_Entries,SenderCommitIndex:sm.commitIndex}
			var snd Send
			snd = Send{PeerId:sm.serverIds[i],Event:appenreq}
			action = append(action,snd)
		}
	}
	return action
}

func handleTimeout(sm *StateMachine) []Action{
	var action []Action
	if sm.currState == "leader" {
		var alm Alarm
		alm = Alarm{T:sm.heartbeatAlarmPeriod}
		action = append(action,alm)		
		for i := 0; i < len(sm.serverIds); i++ {
			loc_prevLogIndex := sm.nextIndex[sm.serverIds[i]] - 1
			var loc_prevLogTerm int
			if loc_prevLogIndex < 0 {
				loc_prevLogTerm = 0
			} else {	
				loc_prevLogTerm = sm.log[loc_prevLogIndex].Term
			}
			var appenreq AppendEntriesReq
			appenreq = AppendEntriesReq{SenderTerm:sm.currTerm,SenderId:sm.selfId,PrevLogIndex:loc_prevLogIndex,PrevLogTerm:loc_prevLogTerm,Entries:[]LogEntry{},SenderCommitIndex:sm.commitIndex}
			var snd Send
			snd = Send{PeerId:sm.serverIds[i],Event:appenreq}
			action = append(action,snd)
		}
	}else{
		var alm Alarm
		alm = Alarm{T:randRange(lb * sm.electionAlarmPeriod, ub * sm.electionAlarmPeriod)}
		action = append(action,alm)		
		sm.currState = "candidate"
		sm.currTerm++
		sm.lastRepIndex = -1
		sm.hasVoted = make(map[int]int)
		sm.hasVoted[sm.selfId] = 1
		for i := 0; i < len(sm.serverIds); i++ {
			sm.hasVoted[sm.serverIds[i]] = 0
		}		
		sm.votedFor = sm.selfId
		var ststr StateStore
		ststr = StateStore{CurrTerm:sm.currTerm,VotedFor:sm.votedFor,LastRepIndex:sm.lastRepIndex}
		action = append(action,ststr)
		for i := 0; i < len(sm.serverIds); i++ {
			var snd Send
			snd = Send{PeerId:sm.serverIds[i],Event:VoteReq{SenderId:sm.selfId,SenderTerm:sm.currTerm,SenderLastLogIndex:sm.lastLogIndex,SenderLastLogTerm:sm.lastLogTerm}}
			action = append(action,snd)
		}
	}
	return action
}

func handleAppendEntriesReq(sm *StateMachine, cmd *AppendEntriesReq) []Action{
	var action []Action
	if sm.currTerm > cmd.SenderTerm {
		var snd Send
		snd = Send{PeerId : cmd.SenderId, Event: AppendEntriesResp{SenderId:sm.selfId,SenderTerm:sm.currTerm,Response:false,SenderLastRepIndex:sm.lastRepIndex}}
		action = append(action,snd)
	} else {
		var alm Alarm
		alm = Alarm{randRange(lb * sm.electionAlarmPeriod, ub * sm.electionAlarmPeriod)}
		action = append(action,alm)
		sm.currState = "follower" //handles all cases
		if sm.currTerm < cmd.SenderTerm {
			sm.currTerm = cmd.SenderTerm
			sm.votedFor = -1
			sm.lastRepIndex = -1
			sm.hasVoted = make(map[int]int)
			var ststr StateStore
			ststr = StateStore{CurrTerm:sm.currTerm,VotedFor:sm.votedFor,LastRepIndex:sm.lastRepIndex}
			action = append(action,ststr)
		}
		sm.leaderId = cmd.SenderId
		if (cmd.PrevLogIndex > sm.lastLogIndex) || (cmd.PrevLogIndex > -1 && (sm.log[cmd.PrevLogIndex].Term != cmd.PrevLogTerm)){
			var snd Send
			snd = Send{PeerId : cmd.SenderId, Event: AppendEntriesResp{SenderId:sm.selfId,SenderTerm:sm.currTerm,Response:false,SenderLastRepIndex:sm.lastRepIndex}}
			action = append(action,snd)			
		} else if sm.lastRepIndex >= (cmd.PrevLogIndex + int64(len(cmd.Entries))) {
			if sm.commitIndex < sm.lastLogIndex && sm.commitIndex < cmd.SenderCommitIndex{
				sm.commitIndex = min(cmd.SenderCommitIndex, sm.lastLogIndex)
				var cmt Commit
				cmt = Commit{Index:sm.commitIndex,Data:sm.log[sm.commitIndex].Command,Id:sm.leaderId,Err:Error{"nil"}}	
				action = append(action,cmt)
			}
			var snd Send
			snd = Send{PeerId : cmd.SenderId, Event: AppendEntriesResp{SenderId:sm.selfId,SenderTerm:sm.currTerm,Response:true,SenderLastRepIndex:sm.lastRepIndex}}
			action = append(action,snd)
		} else {
			if len(cmd.Entries) > 0 {
				sm.lastLogIndex = cmd.PrevLogIndex
				sm.log = sm.log[:sm.lastLogIndex + 1]
			}
			sm.lastRepIndex = cmd.PrevLogIndex
			for i := 0; i<len(cmd.Entries);i++ {
				sm.lastLogIndex++
				sm.log = append(sm.log, cmd.Entries[i])
				var lgstr LogStore
				lgstr = LogStore{Index:sm.lastLogIndex,Entry:cmd.Entries[i]}
				action = append(action,lgstr)
				sm.lastLogTerm = sm.log[sm.lastLogIndex].Term
				sm.lastRepIndex = sm.lastLogIndex
			}
			var ststr StateStore
			ststr = StateStore{CurrTerm:sm.currTerm,VotedFor:sm.votedFor,LastRepIndex:sm.lastRepIndex}
			action = append(action,ststr)			
			if sm.commitIndex < sm.lastLogIndex && sm.commitIndex < cmd.SenderCommitIndex{
				sm.commitIndex = min(cmd.SenderCommitIndex, sm.lastLogIndex)
				var cmt Commit
				cmt = Commit{Index:sm.commitIndex,Data:sm.log[sm.commitIndex].Command,Id:sm.leaderId,Err:Error{"nil"}}
				action = append(action,cmt)
			}
			var snd Send
			snd = Send{PeerId : cmd.SenderId, Event: AppendEntriesResp{SenderId:sm.selfId,SenderTerm:sm.currTerm,Response:true,SenderLastRepIndex:sm.lastRepIndex}}
			action = append(action,snd)
		}
	}
	return action
}

func handleAppendEntriesResp(sm *StateMachine, cmd *AppendEntriesResp) []Action{
	var action []Action
	if sm.currState == "leader" {
		if sm.currTerm < cmd.SenderTerm {
			sm.currState = "follower"
			sm.currTerm = cmd.SenderTerm
			sm.votedFor = -1
			sm.lastRepIndex = -1
			var alm Alarm
			alm = Alarm{randRange(lb * sm.electionAlarmPeriod, ub * sm.electionAlarmPeriod)}
			action = append(action,alm)		
			sm.hasVoted = make(map[int]int)
			var ststr StateStore
			ststr = StateStore{CurrTerm:sm.currTerm,VotedFor:sm.votedFor,LastRepIndex:sm.lastRepIndex}
			action = append(action,ststr)			
			return action
		}
		if cmd.Response == false {
			sm.nextIndex[cmd.SenderId]--
			loc_prevLogIndex := sm.nextIndex[cmd.SenderId] - 1
			var loc_prevLogTerm int
			if loc_prevLogIndex < 0 {
				loc_prevLogTerm = 0
			} else {	
				loc_prevLogTerm = sm.log[loc_prevLogIndex].Term
			}
			loc_Entries := sm.log[sm.nextIndex[cmd.SenderId]:sm.lastLogIndex]
			var appenreq AppendEntriesReq
			appenreq = AppendEntriesReq{SenderTerm:sm.currTerm,SenderId:sm.selfId,PrevLogIndex:loc_prevLogIndex,PrevLogTerm:loc_prevLogTerm,Entries:loc_Entries,SenderCommitIndex:sm.commitIndex}
			var snd Send
			snd = Send{PeerId:cmd.SenderId,Event:appenreq}
			action = append(action,snd)
			return action			
		}else{
			sm.matchIndex[cmd.SenderId] = max(sm.matchIndex[cmd.SenderId], cmd.SenderLastRepIndex)			
			sm.nextIndex[cmd.SenderId] = sm.matchIndex[cmd.SenderId]+1
			var count int
			for i := sm.matchIndex[cmd.SenderId];i>sm.commitIndex;i--{
				count = 1
				for _,value := range sm.matchIndex{
					if value>=i{
						count++
					}
				}
				if count > sm.clusterSize/2 && sm.log[i].Term == cmd.SenderTerm{
					sm.commitIndex = i
					var cmt Commit
					cmt = Commit{Index:sm.commitIndex,Data:sm.log[sm.commitIndex].Command,Id:sm.selfId,Err:Error{Errortype:""}}
					action = append(action,cmt)
					break
				}
			}
			if sm.lastLogIndex > sm.matchIndex[cmd.SenderId]{
				loc_prevLogIndex := sm.nextIndex[cmd.SenderId]-1
				var loc_prevLogTerm int
				if loc_prevLogIndex < 0 {
					loc_prevLogTerm = 0
				} else {	
					loc_prevLogTerm = sm.log[loc_prevLogIndex].Term
				}
				loc_Entries := sm.log[sm.nextIndex[cmd.SenderId]:sm.lastLogIndex]
				var appenreq AppendEntriesReq
				appenreq = AppendEntriesReq{SenderTerm:sm.currTerm,SenderId:sm.selfId,PrevLogIndex:loc_prevLogIndex,PrevLogTerm:loc_prevLogTerm,Entries:loc_Entries,SenderCommitIndex:sm.commitIndex}
				var snd Send
				snd = Send{PeerId:cmd.SenderId,Event:appenreq}
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
		if sm.currTerm < cmd.SenderTerm {
			sm.currTerm = cmd.SenderTerm
			sm.votedFor = -1
			sm.lastRepIndex = -1
			var ststr StateStore
			ststr = StateStore{CurrTerm:sm.currTerm,VotedFor:sm.votedFor,LastRepIndex:sm.lastRepIndex}
			action = append(action,ststr)

		}
		if sm.currTerm > cmd.SenderTerm{
			var snd Send
			snd = Send{PeerId:cmd.SenderId,Event:VoteResp{SenderId:sm.selfId,SenderTerm:sm.currTerm,Response:false}}
			action = append(action,snd)
			return action
		}
		if sm.lastLogTerm > cmd.SenderLastLogTerm || (sm.lastLogTerm == cmd.SenderLastLogTerm && sm.lastLogIndex>cmd.SenderLastLogIndex){
			var snd Send
			snd = Send{PeerId:cmd.SenderId,Event:VoteResp{SenderId:sm.selfId,SenderTerm:sm.currTerm,Response:false}}
			action = append(action,snd)
			return action
		}
		if sm.votedFor != -1 && sm.votedFor != cmd.SenderId{
			var snd Send
			snd = Send{PeerId:cmd.SenderId,Event:VoteResp{SenderId:sm.selfId,SenderTerm:sm.currTerm,Response:false}}
			action = append(action,snd)
			return action		
		}
		var alm Alarm
		alm = Alarm{randRange(lb * sm.electionAlarmPeriod, ub * sm.electionAlarmPeriod)}
		action = append(action,alm)				
		sm.votedFor = cmd.SenderId
		sm.hasVoted = make(map[int]int)
		sm.hasVoted[cmd.SenderId] = 1		
		var ststr StateStore
		ststr = StateStore{CurrTerm:sm.currTerm,VotedFor:sm.votedFor,LastRepIndex:sm.lastRepIndex}
		action = append(action,ststr)
		var snd Send
		snd = Send{PeerId:cmd.SenderId,Event:VoteResp{SenderId:sm.selfId,SenderTerm:sm.currTerm,Response:true}}
		action = append(action,snd)
	}else{
		if sm.currTerm < cmd.SenderTerm{
			sm.currState = "follower"
			sm.currTerm = cmd.SenderTerm
			sm.lastRepIndex = -1
			sm.votedFor = -1
			var alm Alarm
			alm = Alarm{randRange(lb * sm.electionAlarmPeriod, ub * sm.electionAlarmPeriod)}
			action = append(action,alm)		
			sm.hasVoted = make(map[int]int)
			var ststr StateStore
			ststr = StateStore{CurrTerm:sm.currTerm,VotedFor:sm.votedFor,LastRepIndex:sm.lastRepIndex}
			action = append(action,ststr)
			if sm.lastLogTerm > cmd.SenderLastLogTerm || (sm.lastLogTerm == cmd.SenderLastLogTerm && sm.lastLogIndex>cmd.SenderLastLogIndex){
				var snd Send
				snd = Send{PeerId:cmd.SenderId,Event:VoteResp{SenderId:sm.selfId,SenderTerm:sm.currTerm,Response:false}}
				action = append(action,snd)
				return action
			}
			sm.votedFor = cmd.SenderId
			sm.hasVoted = make(map[int]int)
			sm.hasVoted[cmd.SenderId] = 1	
			ststr = StateStore{CurrTerm:sm.currTerm,VotedFor:sm.votedFor,LastRepIndex:sm.lastRepIndex}
			action = append(action,ststr)	
			var snd Send
			snd = Send{PeerId:cmd.SenderId,Event:VoteResp{SenderId:sm.selfId,SenderTerm:sm.currTerm,Response:true}}
			action = append(action,snd)
			return action			
		}
		var snd Send
		snd = Send{PeerId:cmd.SenderId,Event:VoteResp{SenderId:sm.selfId,SenderTerm:sm.currTerm,Response:false}}
		action = append(action,snd)
	}
	return action	
}

func handleVoteResp(sm *StateMachine, cmd *VoteResp) []Action{
	var action []Action
	if sm.currState == "leader" || sm.currState == "follower"{
		if sm.currTerm < cmd.SenderTerm{
			if sm.currState == "leader" {
				sm.currState = "follower"
				var alm Alarm
				alm = Alarm{randRange(lb * sm.electionAlarmPeriod, ub * sm.electionAlarmPeriod)}
				action = append(action,alm)	
			}
			sm.currTerm = cmd.SenderTerm
			var ststr StateStore
			ststr = StateStore{CurrTerm:sm.currTerm,VotedFor:sm.votedFor,LastRepIndex:sm.lastRepIndex}
			action = append(action,ststr)		
		}	
	} else if sm.currState == "candidate" {
		if sm.currTerm < cmd.SenderTerm {
			sm.currState = "follower"
			sm.currTerm = cmd.SenderTerm
			sm.votedFor = -1
			sm.lastRepIndex = -1
			var alm Alarm
			alm = Alarm{randRange(lb * sm.electionAlarmPeriod, ub * sm.electionAlarmPeriod)}
			action = append(action, alm)
			var ststr StateStore			
			ststr = StateStore{sm.currTerm, sm.votedFor, sm.lastRepIndex}
			action = append(action, ststr)		
		} else if sm.currTerm == cmd.SenderTerm {
			if cmd.Response == true{
				sm.hasVoted[cmd.SenderId] = 1
				var numVotes int
				numVotes = 0
				for key,_ := range sm.hasVoted{
					if sm.hasVoted[key] == 1{
						numVotes++
					}
				}
				if numVotes > sm.clusterSize/2{
					sm.currState = "leader"
					sm.leaderId = sm.selfId
					for i := 0; i < len(sm.serverIds); i++ {
							sm.nextIndex[sm.serverIds[i]] = sm.lastLogIndex + 1
							sm.matchIndex[sm.serverIds[i]] = -1
					}					
					for i := 0;i<len(sm.serverIds); i++{
						loc_prevLogIndex := sm.nextIndex[sm.serverIds[i]]-1
						var loc_prevLogTerm int
						if loc_prevLogIndex < 0 {
							loc_prevLogTerm = 0
						} else {	
							loc_prevLogTerm = sm.log[loc_prevLogIndex].Term
						}				
						var appenreq AppendEntriesReq
						appenreq = AppendEntriesReq{SenderTerm:sm.currTerm,SenderId:sm.selfId,PrevLogIndex:loc_prevLogIndex,PrevLogTerm:loc_prevLogTerm,Entries:[]LogEntry{},SenderCommitIndex:sm.commitIndex}
						var snd Send
						snd = Send{PeerId:sm.serverIds[i],Event:appenreq}
						action = append(action,snd)				
					}
					var alm Alarm	
					alm = Alarm{sm.heartbeatAlarmPeriod}
					action = append(action, alm)
				}
			}else{
				sm.hasVoted[cmd.SenderId] = -1
				var numVotes int
				numVotes = 0
				for key,_ := range sm.hasVoted{
					if sm.hasVoted[key] == -1{
						numVotes++
					}
				}
				if numVotes > sm.clusterSize/2{
					sm.currState = "follower"
					var alm Alarm
					alm = Alarm{randRange(lb * sm.electionAlarmPeriod, ub * sm.electionAlarmPeriod)}
					action = append(action,alm)		
				}			
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
