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

import (
	//	"bytes"
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	// "fmt"
	"math"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.

/*
当每个Raft对等体意识到连续的日志条目已提交时，该对等体应通过传递给Make（）的applyCh向同一服务器上的服务（或测试人员）发送ApplyMsg。
将CommandValid设置为true，表示ApplyMsg包含新提交的日志条目。

*/

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// Log 的结构
type Log struct {
	Term    int
	Index   int
	Command interface{}
}

const (
	Leader    = 1
	Candidate = 2
	Follower  = 3
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	mu_cond sync.Mutex
	cond    *sync.Cond

	stat int

	currentTerm  int
	votedFor     int
	logs         []Log
	commitIndex  int
	lastApplied  int
	LastLogIndex int
	LastLogTerm  int

	// 快照
	LastSnapshotIndex int
	LastSnapshotTerm  int
	snapshot          []byte

	channel chan ApplyMsg

	// for leader
	nextIndex  []int
	matchIndex []int

	// 用于计时器
	IsElection bool

	winvote int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// // fmt.Println("Term", rf.currentTerm, "取得", rf.me, "的状态为", rf.stat)
	term = rf.currentTerm
	if rf.stat == Leader {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	raftstate := w.Bytes()

	if rf.snapshot == nil {
		rf.persister.Save(raftstate, nil)
	} else {
		w_ := new(bytes.Buffer)
		e_ := labgob.NewEncoder(w_)
		e_.Encode(rf.LastSnapshotIndex)
		e_.Encode(rf.LastLogTerm)
		e_.Encode(rf.snapshot)
		snapshot := w_.Bytes()
		rf.persister.Save(raftstate, snapshot)
	}
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []Log

	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil {
		Debug(dWarn, "S%v 读取持久化失败", rf.me)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		rf.LastLogTerm = rf.logs[len(rf.logs)-1].Term
		rf.LastLogIndex = rf.logs[len(rf.logs)-1].Index
	}
}

func (rf *Raft) ReadSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var LastSnapshotIndex int
	var LastSnapshotTerm int
	var snapshot []byte

	if d.Decode(&LastSnapshotIndex) != nil || d.Decode(&LastSnapshotTerm) != nil || d.Decode(&snapshot) != nil {
		Debug(dWarn, "S%v 读取快照持久化失败", rf.me)
	} else {
		rf.LastSnapshotIndex = LastSnapshotIndex
		rf.LastSnapshotTerm = LastSnapshotTerm
		rf.snapshot = snapshot
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	rf.snapshot = snapshot

	// fmt.Println(rf.me, "当前快照位置为", rf.LastSnapshotIndex, "index: ", index)

	var i = rf.logs[index-rf.LastSnapshotIndex].Index
	var t = rf.logs[index-rf.LastSnapshotIndex].Term

	rf.logs = rf.logs[index - rf.LastSnapshotIndex : ]

	rf.LastSnapshotIndex = i
	rf.LastSnapshotTerm = t

	// fmt.Println(rf.me, "保存快照位置为", rf.LastSnapshotIndex, "index: ", index)

	rf.persist()
	rf.mu.Unlock()
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CabdudateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term         int
	VotedGranted bool
	// 测试使用
	// O_term int
	// O_stat int
	// O_votedFor int
	// O_winvote int

	// A_term int
	// A_stat int
	// A_votedFor int
	// A_winvote int
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term      int
	Success   bool
	LogIndex1 int // 这个term中的第一个index
	LogIndex2 int // 这个term中的第二个index
	LogTerm   int

	// 用于测试
	// Logs []Log
	CommitIndex int
}

func (rf *Raft) ProcessElection(args RequestVoteArgs, target_server int) {

	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(target_server, &args, &reply)
	if !ok {
		rf.mu.Lock()
		Debug(dVote, "S%v -> S%v 在Term %v通信失败", rf.me, target_server, args.Term)
		rf.mu.Unlock()
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term != rf.currentTerm {
		return
	}

	if reply.Term > rf.currentTerm {

		rf.stat = Follower
		rf.votedFor = -1
		rf.winvote = 0
		rf.currentTerm = reply.Term

		rf.persist()

		return
	}

	if rf.stat != Candidate {
		return
	}

	if reply.VotedGranted {
		rf.winvote++
		Debug(dVote, "S%v 在Term %v获得S%v的投票, 总票数为%v", rf.me, rf.currentTerm, target_server, rf.winvote)
	}

	if rf.winvote >= len(rf.peers)/2+1 {
		rf.stat = Leader

		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = rf.LastLogIndex + 1
			rf.matchIndex[i] = 0
		}

		go rf.Heartticker()

		Debug(dVote, "S %v 在Term %v成为领导人, 总票数为%v", rf.me, rf.currentTerm, rf.winvote)
		return
	}

}

func (rf *Raft) DoElection() {
	rf.mu.Lock()
	rf.currentTerm++

	Debug(dVote, "S%v 在Term%v成为候选人其log为: %v", rf.me, rf.currentTerm, rf.logs)

	rf.stat = Candidate
	rf.votedFor = rf.me
	rf.winvote = 1
	rf.persist()
	rf.mu.Unlock()

	for target_server := range rf.peers {
		rf.mu.Lock()
		if rf.stat != Candidate {
			rf.mu.Unlock()
			break
		}

		if target_server == rf.me {
			rf.mu.Unlock()
			continue
		}

		args := RequestVoteArgs{
			Term:         rf.currentTerm,
			CabdudateId:  rf.me,
			LastLogIndex: rf.LastLogIndex,
			LastLogTerm:  rf.LastLogTerm,
		}
		rf.mu.Unlock()

		// 发送信息
		go rf.ProcessElection(args, target_server)
	}
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {

		rf.stat = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.winvote = 0
		rf.persist()
	}

	// 除非term小于对方，否则己方是leader（说明已经得到多数同意）直接拒绝对方，防止leader投票给别人
	if rf.stat == Leader {
		reply.Term = rf.currentTerm
		reply.VotedGranted = false
		return
	}

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VotedGranted = false
		return
	}

	reply.Term = rf.currentTerm

	if rf.votedFor == -1 || rf.votedFor == rf.me || rf.votedFor == args.CabdudateId {

		// if args.LastLogIndex < rf.commitIndex {
		// 	reply.VotedGranted = false

		// 	return
		// }

		if args.LastLogTerm < rf.LastLogTerm {
			reply.VotedGranted = false

			return
		} else if args.LastLogTerm > rf.LastLogTerm {
			reply.VotedGranted = true
			rf.stat = Follower
			rf.winvote = 0
			rf.votedFor = args.CabdudateId
			Debug(dVote, "S%v voted for S%v, Term%v, stat: %v", rf.me, args.CabdudateId, rf.currentTerm, rf.stat)
			rf.persist()

			return
		} else {
			if args.LastLogIndex < rf.LastLogIndex {
				reply.VotedGranted = false

				return
			} else {
				reply.VotedGranted = true
				rf.stat = Follower
				rf.votedFor = args.CabdudateId
				rf.winvote = 0
				Debug(dVote, "S%v voted for S%v, Term%v, stat: %v", rf.me, args.CabdudateId, rf.currentTerm, rf.stat)
				rf.persist()

				return
			}
		}
	} else {
		reply.VotedGranted = false
		return
	}
}

func (rf *Raft) commit() {
	for !rf.killed() {
		rf.mu_cond.Lock()
		for rf.lastApplied == rf.commitIndex {
			rf.cond.Wait()
		}
		rf.mu_cond.Unlock()

		rf.mu.Lock()
		command := rf.logs[rf.lastApplied + 1 - rf.LastSnapshotIndex].Command
		index := rf.logs[rf.lastApplied + 1 - rf.LastSnapshotIndex].Index

		// fmt.Println("提交信息", rf.lastApplied, rf.commitIndex, index, rf.LastSnapshotIndex)

		Debug(dCommit, "S%v commit %v, Index %v, Term %v", rf.me, command, index, rf.logs[rf.lastApplied+1-rf.LastSnapshotIndex].Term)
		rf.mu.Unlock()

		applymsg := ApplyMsg{
			CommandValid: true,
			Command:      command,
			CommandIndex: index,
		}

		rf.channel <- applymsg

		rf.mu_cond.Lock()
		rf.lastApplied += 1
		rf.mu_cond.Unlock()
	}
}

func (rf *Raft) ProcessHeart(args AppendEntriesArgs, target_server int) {

	reply := AppendEntriesReply{}
	ok := rf.sendHeart(target_server, &args, &reply)
	if !ok {
		// rf.mu.Lock()
		// Debug(dWarn, "S%v -> S%v 在Term%v发送心跳失败, stat: %v", rf.me, target_server, args.Term, rf.stat)
		// rf.mu.Unlock()
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term != rf.currentTerm {
		return
	}

	// Debug(dHeart, "S%v <- S%v reply: [Term: %v, Success: %v, LogIndex1: %v, LogIndex2: %v, LogTerm: %v]", rf.me, target_server, reply.Term, reply.Success, reply.LogIndex1, reply.LogIndex2, reply.LogTerm)

	if reply.Term > rf.currentTerm {
		rf.stat = Follower
		rf.currentTerm = reply.Term
		rf.persist()
		return
	}

	if rf.stat != Leader {
		return
	}

	if reply.Success {

		if args.Entries != nil {
			rf.nextIndex[target_server] = args.Entries[len(args.Entries)-1].Index + 1
			rf.matchIndex[target_server] = args.Entries[len(args.Entries)-1].Index
		}

		if rf.logs[rf.matchIndex[target_server]-rf.LastSnapshotIndex].Term == rf.currentTerm && rf.matchIndex[target_server] > rf.commitIndex {
			num := 1
			for i := range rf.peers {
				if i != rf.me && rf.matchIndex[i] >= rf.matchIndex[target_server] {
					num++
				}
			}

			// // fmt.Println("符合的数目：", num)

			if num >= len(rf.peers)/2+1 {
				// todo 提交给上层
				rf.mu_cond.Lock()
				if rf.matchIndex[target_server] > rf.lastApplied {
					rf.commitIndex = rf.matchIndex[target_server]
					rf.cond.Signal()
				}
				rf.mu_cond.Unlock()
			}
		}

	} else {

		Debug(rHeart, "S%v 接收到来着S%v的返回命令, [%v, %v], Term %v, 当前log为%v", rf.me, target_server, reply.LogIndex1, reply.LogIndex2, reply.LogTerm, rf.logs)

		if reply.LogIndex1 == 0 {
			rf.nextIndex[target_server] = 1
		}

		i := reply.LogIndex2

		for ; i >= reply.LogIndex1; i-- {
			if rf.logs[i-rf.LastSnapshotIndex].Term == reply.LogTerm {
				break
			}
		}

		if i >= reply.LogIndex1 {
			rf.nextIndex[target_server] = i + 1
		} else {
			rf.nextIndex[target_server] = reply.LogIndex1
		}
	}

}

func (rf *Raft) DoHeart() {
	rf.mu.Lock()
	rf.IsElection = false // leader不需要选举
	rf.mu.Unlock()

	num := 0
	for target_server := range rf.peers {

		rf.mu.Lock()
		if rf.stat != Leader {
			rf.mu.Unlock()
			break
		}

		if rf.me == target_server {
			rf.mu.Unlock()
			continue
		}

		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			LeaderCommit: rf.commitIndex,
		}

		args.PrevLogIndex = rf.nextIndex[target_server] - 1
		args.PrevLogTerm = rf.logs[args.PrevLogIndex-rf.LastSnapshotIndex].Term

		if rf.nextIndex[target_server] == rf.LastLogIndex+1 {
			args.Entries = nil
		} else {
			args.Entries = make([]Log, rf.LastLogIndex+1-rf.nextIndex[target_server])
			copy(args.Entries, rf.logs[rf.nextIndex[target_server]-rf.LastSnapshotIndex:rf.LastLogIndex+1-rf.LastSnapshotIndex])
		}

		// Debug(dHeart, "S%v -> S%v arg: [Term: %v, LeaderId: %v, PrevLogTerm: %v, PrevLogIndex: %v, Entries: %v, LeaderCommit: %v]", rf.me, target_server, args.Term, args.LeaderId, args.PrevLogTerm, args.PrevLogIndex, args.Entries, args.LeaderCommit)

		// Debug(dHeart, "S%v -> S%v : Term %v, PLI %v, PLT %v", rf.me, target_server, args.Term, args.PrevLogIndex, args.PrevLogTerm)
		// Debug(dEntry, "S%v 发送日志为：%v", rf.me, args.Entries)
		// Debug(dLog, "S%v 的日志为：%v", rf.me, rf.logs)
		rf.mu.Unlock()

		go rf.ProcessHeart(args, target_server)

		num++
	}
}

func (rf *Raft) AppednEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.persist()
	}

	if args.Entries != nil {
		Debug(rHeart, "S%v 收到来自S%v的心跳", rf.me, args.LeaderId)
		Debug(rHeart, "S%v Entries: %v, PLI %v, PLT %v, Term %v", rf.me, args.Entries, args.PrevLogIndex, args.PrevLogTerm, args.Term)
		Debug(dLog, "S%v 的原日志为：%v", rf.me, rf.logs)
	}

	rf.IsElection = false

	rf.stat = Follower
	reply.Term = rf.currentTerm

	if rf.LastLogIndex >= args.PrevLogIndex {
		rf.LastLogIndex = args.PrevLogIndex

		rf.logs = rf.logs[:rf.LastLogIndex+1-rf.LastSnapshotIndex] //保证长度一致

		if rf.logs[rf.LastLogIndex-rf.LastSnapshotIndex].Term == args.PrevLogTerm { // 等于直接修改log
			rf.logs = append(rf.logs, args.Entries...)
			rf.LastLogIndex += len(args.Entries)
			rf.LastLogTerm = rf.logs[rf.LastLogIndex-rf.LastSnapshotIndex].Term
			reply.Success = true
			Debug(dLog, "S%v 最终日志为：%v", rf.me, rf.logs)

			rf.mu_cond.Lock()
			rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(rf.LastLogIndex)))
			if rf.commitIndex > rf.lastApplied {
				rf.cond.Signal()
			}
			rf.mu_cond.Unlock()

			rf.persist()
			return
		}

		if rf.logs[rf.LastLogIndex-rf.LastSnapshotIndex].Term < args.PrevLogTerm {
			reply.Success = false
			reply.LogIndex2 = rf.LastLogIndex

			i := rf.LastLogIndex
			for ; i-rf.LastSnapshotIndex > 0 && rf.logs[i-rf.LastSnapshotIndex].Term == rf.logs[rf.LastLogIndex-rf.LastSnapshotIndex].Term; i-- {
			} // 寻找当前term的index范围

			reply.LogIndex1 = i + 1

			reply.LogTerm = rf.logs[rf.LastLogIndex-rf.LastSnapshotIndex].Term

			// reply.Logs = rf.logs

			return
		}

		if rf.logs[rf.LastLogIndex-rf.LastSnapshotIndex].Term > args.PrevLogTerm {
			reply.Success = false
			reply.LogIndex2 = rf.LastLogIndex

			i := rf.LastLogIndex
			for ; i-rf.LastSnapshotIndex > 0 && rf.logs[i-rf.LastSnapshotIndex].Term > args.PrevLogTerm; i-- {
			} // 找到小于等于PrevLogTerm的位置

			// todo
			if i == 0 {
				reply.LogIndex1 = 0
				reply.LogIndex2 = 0
				reply.LogTerm = 0

				// reply.Logs = rf.logs
				return
			}

			reply.LogIndex2 = i
			for ; i-rf.LastSnapshotIndex > 0 && rf.logs[i-rf.LastSnapshotIndex].Term == rf.logs[rf.LastLogIndex-rf.LastSnapshotIndex].Term; i-- {
			} // 寻找当前term的index范围
			reply.LogIndex1 = i + 1
			reply.LogTerm = rf.logs[reply.LogIndex2-rf.LastSnapshotIndex].Term

			// reply.Logs = rf.logs
			return
		}
	}

	reply.Success = false
	i := rf.LastLogIndex
	for ; i-rf.LastSnapshotIndex > 0 && rf.logs[i-rf.LastSnapshotIndex].Term > args.PrevLogTerm; i-- {
	} // 找到小于等于PrevLogTerm的位置

	// todo
	if i == 0 {
		reply.LogIndex1 = 0
		reply.LogIndex2 = 0
		reply.LogTerm = 0

		// reply.Logs = rf.logs
		return
	}
	reply.LogIndex2 = i
	for ; i-rf.LastSnapshotIndex > 0 && rf.logs[i-rf.LastSnapshotIndex].Term == rf.logs[rf.LastLogIndex].Term; i-- {
	} // 寻找当前term的index范围
	reply.LogIndex1 = i + 1
	reply.LogTerm = rf.logs[reply.LogIndex2-rf.LastSnapshotIndex].Term

	// reply.Logs = rf.logs
	// return

}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendHeart(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppednEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.stat == Leader {
		isLeader = true
	} else {
		isLeader = false
		return index, term, isLeader
	}
	index = rf.LastLogIndex + 1
	term = rf.currentTerm

	rf.logs = append(rf.logs, Log{rf.currentTerm, rf.LastLogIndex + 1, command})
	rf.LastLogIndex += 1
	rf.LastLogTerm = rf.currentTerm

	rf.persist()

	Debug(dCommand, "S%v <- {%v, %v, %v}", rf.me, term, index, command)

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// rf.mu.Lock()
		// if rf.IsElection && rf.stat != Leader{
		// 	go rf.DoElection()
		// }
		// rf.IsElection = true // 睡觉的时候有没有人修改
		// rf.mu.Unlock()

		// ms := 50 + (rand.Int63() % 350)
		ms := 1000 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		rf.mu.Lock()
		if rf.IsElection && rf.stat != Leader {
			go rf.DoElection()
		}
		rf.IsElection = true // 睡觉的时候有没有人修改
		rf.mu.Unlock()
	}
}

func (rf *Raft) Heartticker() {
	for !rf.killed() {

		rf.mu.Lock()
		if rf.stat != Leader {
			rf.mu.Unlock()
			return
		}
		go rf.DoHeart()
		rf.mu.Unlock()

		// ms := 40
		ms := 50
		time.Sleep(time.Duration(ms) * time.Millisecond)

	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
/*
服务端或测试器希望创建一个 Raft 服务器。所有 Raft 服务器（包括当前服务器）的端口都在 peers[] 数组中。
当前服务器的端口是 peers[me]。所有服务器的 peers[] 数组的顺序都相同。
persister 是用于让该服务器保存其持久状态的位置，它也最初包含了最近保存的状态（如果有的话）。
applyCh 是一个通道，测试器或服务期望 Raft 在其上发送 ApplyMsg 消息。
Make() 必须迅速返回，因此它应该为任何长时间运行的工作启动 Goroutines。
*/

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	// Your initialization code here (3A, 3B, 3C).
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.cond = sync.NewCond(&rf.mu_cond)

	rf.stat = Follower

	rf.currentTerm = 0
	rf.votedFor = rf.me

	rf.IsElection = true

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.logs = append(rf.logs, Log{0, 0, nil})

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.LastSnapshotIndex = 0
	rf.LastSnapshotTerm = 0
	rf.snapshot = nil

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.ReadSnapshot(persister.ReadSnapshot())

	// rf.mu.Lock()
	// // fmt.Println("初始化状态")
	// // fmt.Println(rf.me, rf.LastLogTerm, rf.LastLogTerm)
	// // // fmt.Println(rf.PrevLogTerm, rf.PrevLogIndex)
	// // fmt.Println(" ")
	// rf.mu.Unlock()

	// 提交到kv服务器
	rf.channel = applyCh

	// // fmt.Println(len(rf.peers))

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.commit()

	return rf
}
