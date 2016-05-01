package main

import (
	//"fmt"
	"encoding/gob"
	"errors"
	"github.com/cs733-iitb/cluster"
	//"github.com/cs733-iitb/cluster/mock"
	"github.com/cs733-iitb/log"
	"strconv"
	"time"
	"sync"
	//"math/rand"
	//"bufio"
	//"io"
	//"math"
	//"net"
	//"strings"
)

// -------------------- configuration structures --------------------

type NetConfig struct {
	Id   int
	Host string
	Port int
}

type Config struct {
	Cluster          []NetConfig
	Id               int
	LogFileDir       string
	ElectionTimeout  int64
	HeartbeatTimeout int64
}

// -------------------- commit info structures --------------------

type CommitInfo Commit


// -------------------- raft node structures --------------------

type Node interface {
	Append([]byte) error
	CommitChannel() (<-chan CommitInfo, error)
	CommittedIndex() (int64, error)
	Get(index int) ([]byte, error)
	Id() (int, error)
	LeaderId() (int, error)
	Shutdown()
}

type RaftNode struct {
	SM             StateMachine
	Mux            sync.Mutex
	IsWorking      bool
	TimeoutTimer   *time.Timer
	AppendEventCh  chan Event
	CommitCh       chan CommitInfo
	NetServer      cluster.Server
	LogFile        *log.Log
	StateFile      *log.Log
}

// -------------------- raft node functions --------------------

func (RN *RaftNode) Append(data []byte) error {
	RN.Mux.Lock()
	defer RN.Mux.Unlock()
	if RN.IsWorking {
		append_ev := Append{Data: data}
		RN.AppendEventCh <- append_ev
		return nil
	}
	return errors.New("ERR_RN_NOT_WORKING")
}

func (RN *RaftNode) CommitChannel() (<-chan CommitInfo, error) {
	RN.Mux.Lock()
	defer RN.Mux.Unlock()
	if RN.IsWorking {
		return RN.CommitCh, nil
	}
	return nil, errors.New("ERR_RN_NOT_WORKING")
}

func (RN *RaftNode) CommittedIndex() (int64, error) {
	RN.Mux.Lock()
	defer RN.Mux.Unlock()
	if RN.IsWorking {
		return RN.SM.commitIndex, nil
	}
	return -1, errors.New("ERR_RN_NOT_WORKING")
}

func (RN *RaftNode) Get(index int) ([]byte, error) {
	RN.Mux.Lock()
	defer RN.Mux.Unlock()
	if RN.IsWorking {
		if index <= len(RN.SM.log)-1 {
			return RN.SM.log[index].Command, nil
		} else {
			return nil, errors.New("ERR_NO_LOG_ENTRY_FOR_INDEX")
		}
	}
	return nil, errors.New("ERR_RN_NOT_WORKING")
}

func (RN *RaftNode) Id() (int, error) {
	RN.Mux.Lock()
	defer RN.Mux.Unlock()
	if RN.IsWorking {
		return RN.SM.selfId, nil
	}
	return -1, errors.New("ERR_RN_NOT_WORKING")
}

func (RN *RaftNode) LeaderId() (int, error) {
	RN.Mux.Lock()
	defer RN.Mux.Unlock()
	if RN.IsWorking {
		return RN.SM.leaderId, nil
	}
	return -1, errors.New("ERR_RN_NOT_WORKING")
}

func (RN *RaftNode) Shutdown() {
	RN.Mux.Lock()
	defer RN.Mux.Unlock()
 
 	if RN.IsWorking {
		RN.IsWorking = false
		RN.TimeoutTimer.Stop()
		//Close(RN.TimeoutEventCh)
		//Close(RN.AppendEventCh)
		//Close(RN.CommitCh)
		RN.NetServer.Close()
		RN.LogFile.Close()
		//RN.StateFile.Close()
	}
}

func GetClusterConfig(conf Config) cluster.Config {
	var peer_config []cluster.PeerConfig

	for _, peer := range conf.Cluster {
		peer_config = append(peer_config, cluster.PeerConfig{Id: peer.Id, Address: peer.Host + ":" + strconv.Itoa(peer.Port)})
	}

	return cluster.Config{Peers: peer_config, InboxSize: 1000, OutboxSize: 1000}
}

func getPeerIds(config Config) []int {
	var (
		i int
		id int
		peerIds []int
	)

	for i = 0; i < len(config.Cluster); i++ {
		id = config.Cluster[i].Id
		if id != config.Id {
			peerIds = append(peerIds, id)
		}
	}
	return peerIds
}

func getVotesRcvd(peerIds []int) map[int]int {
	var (
		i int
		id int
		hasVoted map[int]int
	)

	hasVoted = make(map[int]int)
	for i = 0; i < len(peerIds); i++ {
		id = peerIds[i]
		hasVoted[id] = 0
	}
	return hasVoted
}

func getNextIndex(peerIds []int) map[int]int64 {
	var (
		i int
		id int
		nextIndex map[int]int64
	)

	nextIndex = make(map[int]int64)
	for i = 0; i < len(peerIds); i++ {
		id = peerIds[i]
		nextIndex[id] = -1
	}
	return nextIndex
}

func getMatchIndex(peerIds []int) map[int]int64 {
	var (
		i int
		id int
		matchIndex map[int]int64
	)

	matchIndex = make(map[int]int64)
	for i = 0; i < len(peerIds); i++ {
		id = peerIds[i]
		matchIndex[id] = -1
	}
	return matchIndex
}

func registerStructs() {
	gob.Register(LogEntry{})
	gob.Register(AppendEntriesReq{})
	gob.Register(AppendEntriesResp{})
	gob.Register(VoteReq{})
	gob.Register(VoteResp{})
	gob.Register(Send{})
}

func (rn *RaftNode) getRsmLog() ([]LogEntry, error) {
	var (
		rsmLog []LogEntry
		entry LogEntry
		itf interface{}
		err error
		lastInd int64
		i int64
	)

	lastInd = rn.LogFile.GetLastIndex()
	for i = 0; i < lastInd; i++ {
		itf, err = rn.LogFile.Get(i)
		if err != nil {
			return []LogEntry{}, err
		}
		entry = itf.(LogEntry)
		rsmLog = append(rsmLog, entry)
	}
	return rsmLog, nil
}

func New(conf Config) Node {
	var(
		rn RaftNode
		rsmLog []LogEntry
		peerIds []int
		hasVoted map[int]int
		nextIndex map[int]int64
		matchIndex map[int]int64
		ClustConfig cluster.Config
	)
	// initlisation of other raft node variables
	rn.IsWorking = true
	rn.TimeoutTimer = time.NewTimer(time.Duration(randRange(conf.ElectionTimeout, 2 * conf.ElectionTimeout)) * time.Millisecond)	
	<-rn.TimeoutTimer.C
	//fmt.Println(<-rn.TimeoutTimer.C)
	rn.AppendEventCh = make(chan Event, 100)
	//rn.TimeoutEventCh = make(chan Event, 100)
	rn.CommitCh = make(chan CommitInfo, 100)
	ClustConfig = GetClusterConfig(conf)               
	rn.NetServer, _ = cluster.New(conf.Id, ClustConfig) 
	rn.LogFile, _ = log.Open(conf.LogFileDir)	
	// initilisation of state machine
	peerIds = getPeerIds(conf)
	hasVoted = getVotesRcvd(peerIds)
	nextIndex = getNextIndex(peerIds)
	matchIndex = getMatchIndex(peerIds)
	registerStructs()
	rsmLog, _ = rn.getRsmLog();
	//rsmState, err = rn.getRsmState();
	rn.SM.init( /* currTerm */ 0, 
						 	/* votedFor */ -1, 
						 	/* Log */ rsmLog,
						 	/* selfId */ conf.Id, 
						 	/* peerIds */ peerIds, 
						 	/* electionAlarm */ conf.ElectionTimeout, 
						 	/* heartbeatAlarm */ conf.HeartbeatTimeout, 
						 	/* lastMatchIndex */ -1, 
						 	/* currState --Follower*/ "follower", 
						 	/* commitIndex */ -1, 
						 	/* leaderId */ -1, 
						 	/* lastLogIndex */ -1, 
						 	/* lastLogTerm */ 0, 
						 	/* votedAs */ hasVoted, 
						 	/* nextIndex */ nextIndex, 
							/* matchIndex */ matchIndex)
	go rn.ProcessNodeEvents()
	return &rn
}

func (RN *RaftNode) ProcessActions(actions []Action) {
	for _, act := range actions {
		switch act.(type) {

		case Send:
			send_act := act.(Send)
			// send_act.Event to outbox of send_act.peerid
			RN.NetServer.Outbox() <- &cluster.Envelope{Pid: send_act.PeerId, Msg: send_act.Event}

		case Commit:
			commit_act := act.(Commit)
			RN.CommitCh <- CommitInfo{Data: commit_act.Data, Index: commit_act.Index, Err: commit_act.Err}

		case Alarm:
			alarm_act := act.(Alarm)
			ret := RN.TimeoutTimer.Reset(time.Millisecond * time.Duration(alarm_act.T))
			if !ret {
				RN.TimeoutTimer = time.NewTimer(time.Millisecond * time.Duration(alarm_act.T))
			}

		case LogStore:
			log_store_act := act.(LogStore)
			RN.LogFile.TruncateToEnd(int64(log_store_act.Index))
			RN.LogFile.Append(LogEntry{Term: log_store_act.Entry.Term, Command: log_store_act.Entry.Command})

		case StateStore:
//			state_store_act := act.(StateStore)
//			RN.StateFile.TruncateToEnd(0)
//			RN.StateFile.Append(StateEntry{Term: state_store_act.Term, VotedFor: state_store_act.VotedFor, CurrTermLastLogIndex: state_store_act.CurrTermLastLogIndex})
		}
	}
}

func (RN *RaftNode) ProcessNodeEvents() {
	for {
		RN.Mux.Lock()
		if RN.IsWorking {
			var ev Event

			select {
			case ev = <-RN.AppendEventCh:
				// Append Event
				actions := RN.SM.ProcessEvent(ev)
				RN.ProcessActions(actions)

			case <-RN.TimeoutTimer.C:
				// Timeout Event
				ev = Timeout{}
				actions := RN.SM.ProcessEvent(ev)
				RN.ProcessActions(actions)

			case inboxEvent := <-RN.NetServer.Inbox():
				// Network Event
				ev = inboxEvent.Msg
				actions := RN.SM.ProcessEvent(ev)
				RN.ProcessActions(actions)
			default:
			}
			//actions := RN.SM.ProcessEvent(ev)
			//RN.ProcessActions(actions)
		}
		RN.Mux.Unlock()
	}
}