package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "labrpc"
import "fmt"
import "time"
import "math/rand"

import "bytes"
import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu                sync.Mutex          // Lock to protect shared access to this peer's state
	peers             []*labrpc.ClientEnd // RPC end points of all peers
	persister         *Persister          // Object to hold this peer's persisted state
	me                int                 // this peer's index into peers[]

	// START CODE
	commitIndex       int
	lastApplied       int
	nextIndex         []int
	matchIndex        []int

	currentTerm       int
	votedFor          int
	log               []int

	state             string
	votesForMe        int
	receivedHeartbeat bool
	// FINISH CODE

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	// START CODE
	term = rf.currentTerm
	isleader = (rf.state == "leader")
	// FINISH CODE

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	// START CODE
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	enc.Encode(rf.currentTerm)
	enc.Encode(rf.votedFor)
	enc.Encode(rf.log)
	rf.persister.SaveRaftState(buf.Bytes())
	// FINISH CODE
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
	rf.persister.SaveRaftState(data)
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A).

	// START CODE
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
	Used bool
	// FINISH CODE
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).

	// START CODE
	Term int
	VoteGranted bool
	// Used bool
	// FINISH CODE
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	fmt.Println(rf.me, "received request (", args.Used,")")
	// fmt.Println(reply)

	// START CODE
	if args.Used {
		myReply := &RequestVoteReply{}
		myArgs := &RequestVoteArgs{}

		myArgs.Used = false
		myReply.Term = rf.currentTerm
		myReply.VoteGranted = (rf.votedFor == -1) && (args.Term >= rf.currentTerm)

		if myReply.VoteGranted {
			rf.votedFor = args.CandidateId
			rf.persist()
		}

		fmt.Println(rf.me, "sending reply with vote granted", myReply.VoteGranted)
		rf.sendRequestVote(args.CandidateId, myArgs, myReply)
	}

	if !args.Used {
		fmt.Println(rf.me, "received response with vote granted", reply.VoteGranted)
		if reply.VoteGranted {
			rf.votesForMe++
		}

		if rf.votesForMe > (len(rf.peers) / 2) {
			rf.state = "leader"
			rf.sendAppendEntries()
		}
	}
	// FINISH CODE
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	fmt.Println("Created",me)

	// Your initialization code here (2A, 2B, 2C).

	// START CODE
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = "follower"
	rf.votesForMe = 0
	rf.votedFor = -1
	rf.receivedHeartbeat = false
	// FINISH CODE

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// START CODE
	go rf.waitHeartbeat()
	// FINISH CODE

	return rf
}

// START CODE
func (rf *Raft) waitHeartbeat() {
	for true {
		receivedHeartbeat := make(chan bool)

		go func() {
			for true {
				if rf.receivedHeartbeat {
					rf.receivedHeartbeat = false
					break
				}
			}

			receivedHeartbeat <- true
		}()

		select {
			case <- receivedHeartbeat: 
				fmt.Println("Received heartbeat!")
			case <- time.After(time.Duration((rand.Intn(600 - 400) + 400)) * time.Millisecond):
				rf.startElection()
		}
	}
}
// FINISH CODE

// START CODE
func (rf *Raft) startElection() {
	rf.state = "candidate"
	rf.currentTerm++

	rf.persist()

	args := &RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = len(rf.log)
	args.LastLogTerm = 0 //TA ERRADO
	args.Used = true

	reply := &RequestVoteReply{}

	for i := 0; i < len(rf.peers); i++ {
		fmt.Println(rf.me, "send request to", i)
		go rf.sendRequestVote(i, args, reply)
	}
}
// FINISH CODE

// START CODE
type HeartbeatArgs struct {
	Term int
	Used bool
}

type HeartbeatReply struct {
	Term int
	Used bool
}

func (rf *Raft) AppendEntries(args *HeartbeatArgs, reply *HeartbeatReply) {
	if args.Used {
		rf.receivedHeartbeat = true
		rf.state = "follower"
		rf.currentTerm = args.Term

		rf.persist()
	}
}

func (rf *Raft) sendAppendEntries() {
	for rf.state == "leader" {
		args := &HeartbeatArgs{}
		reply := &HeartbeatReply{}
		args.Term = rf.currentTerm
		args.Used = true

		for server := 0; server < len(rf.peers); server++ {
			if server != rf.me {
				rf.peers[server].Call("Raft.AppendEntries", args, reply)
			}
		}

		time.Sleep(time.Duration(100) * time.Millisecond)
	}
}
// FINISH CODE