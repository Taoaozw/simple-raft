package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/rpc"
	"strings"
	"time"
)

type State int

const (
	Leader = iota + 1
	Candidate
	Follower
)

type node struct {
	id      int
	connect bool
	address string
}

type LogEntry struct {
	LogTerm  int
	LogIndex int
	LogCmd   any
}

type Raft struct {
	id          int
	nodes       map[int]*node
	state       State
	currentTerm int
	voteFor     int
	voteCount   int
	log         []LogEntry
	// 被提交的最大索引
	commitIndex int
	// 被应用到状态机的最大索引
	lastApplied int
	// 保存需要发送给每个节点的下一个条目索引
	nextIndex []int
	// 保存已经复制给每个节点日志的最高索引
	matchIndex []int

	heartbeat chan bool
	toLeaderC chan bool
}

type VoteArgs struct {
	Term, Index, Candidate int
}

type VoteReply struct {
	Term      int
	VoteGrant bool
}

type HeartbeatArgs struct {
	Term   int
	Leader int
	//新日志之前的索引值
	PrevLogIndex int
	//新日志之前的任期
	PrevLogTerm int
	Entries     []LogEntry

	//leader 已经commit的索引值
	LeaderCommit int
}

type HeartbeatReply struct {
	Success bool
	Term    int
	//如果 Follower Index小于Leader Index ，会告诉Leader下一次发送的索引位置
	NextIndex int
}

func newNode(address string, id int) *node {
	n := &node{
		id:      id,
		address: address,
	}
	return n
}

// RequestVote This is the RPC method that is called by other nodes when they want to vote for this node.
func (r *Raft) RequestVote(args VoteArgs, reply *VoteReply) error {
	log.Printf("接受到请求选票调用,节点为:%v,Term为: %v", args.Candidate, args.Term)
	if r.currentTerm > args.Term {
		reply.Term = r.currentTerm
		reply.VoteGrant = false
		return nil
	}
	if r.voteFor == -1 {
		r.currentTerm = args.Term
		r.voteFor = args.Candidate
		reply.Term = r.currentTerm
		reply.VoteGrant = true
	}
	return nil
}

// HeartBeat The above code is handling the heartbeat message from the leader.
func (r *Raft) HeartBeat(args HeartbeatArgs, reply *HeartbeatReply) error {
	log.Printf("接受到心跳调用,节点为:%v,Term为: %v\n", args.Leader, args.Term)
	if r.currentTerm > args.Term {
		reply.Success = false
		reply.Term = r.currentTerm
		return nil
	}
	r.heartbeat <- true
	//处理日志
	if len(args.Entries) == 0 {
		reply.Success = false
		reply.Term = r.currentTerm
		return nil
	}

	//如果当前的日志参数小于心跳中之前的日志索引的话
	//说明断连过 通知leader需要进行日志的回溯
	if r.lastIndex() < args.PrevLogIndex {
		reply.Success = false
		reply.Term = r.currentTerm
		reply.NextIndex = r.lastIndex() + 1
		return nil
	}

	r.log = append(r.log, args.Entries...)
	r.commitIndex = r.lastIndex()

	reply.Success = true
	reply.Term = r.currentTerm
	reply.NextIndex = r.commitIndex + 1

	return nil
}

func (r *Raft) rpc(port string) {
	err := rpc.Register(r)
	if err != nil {
		log.Fatal("rpc enable failed: ", err)
	}
	rpc.HandleHTTP()
	go func() {
		err := http.ListenAndServe(port, nil)
		if err != nil {
			log.Fatal("listen failed: ", err)
		}
	}()
}

func (r *Raft) start() {
	r.state = Follower
	r.currentTerm = 0
	r.voteFor = -1
	r.heartbeat = make(chan bool)
	r.toLeaderC = make(chan bool)

	go func() {
		rand.Seed(time.Now().UnixNano())
		for {
			switch r.state {
			case Follower:
				select {
				case <-r.heartbeat:
					log.Printf("follower-%d recived heartbeat\n", r.id)
				case <-time.After(time.Duration(rand.Intn(500-300)+3000) * time.Millisecond):
					log.Printf("followe-%d  not receive leader beat because timeout\n", r.id)
					r.state = Candidate
				}

			case Candidate:
				log.Printf("Node %d current change to Candidate", r.id)
				r.currentTerm++
				r.voteFor = r.id
				r.voteCount = 1
				go r.broadcastVote()
				select {
				case <-time.After(time.Duration(rand.Intn(500-300)+3000) * time.Millisecond):
					r.state = Follower
				case <-r.toLeaderC:
					log.Printf("Node: %v become Leader \n", r.id)
					r.state = Leader
					r.nextIndex = make([]int, len(r.nodes))
					r.matchIndex = make([]int, len(r.nodes))
					for i := range r.nodes {
						r.nextIndex[i] = 1
						r.matchIndex[i] = 0
					}
					go func() {
						i := 0
						for {
							i++
							r.log = append(r.log, LogEntry{r.currentTerm, i, fmt.Sprintf("user send : %d", i)})
							time.Sleep(3 * time.Second)
						}
					}()

				}
			case Leader:
				//broadcast heartbeat
				r.broadcastHeartbeat()
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()
}

func (r *Raft) broadcastVote() {
	var voteArgs VoteArgs
	if size := len(r.log); size == 0 {
		voteArgs = VoteArgs{
			Term:      r.currentTerm,
			Index:     0,
			Candidate: r.id,
		}
	} else {
		voteArgs = VoteArgs{
			Term:      r.currentTerm,
			Index:     r.log[len(r.log)-1].LogIndex,
			Candidate: r.id,
		}
	}
	for _, n := range r.nodes {
		go func() {
			r.sendRequestVote(voteArgs, n.address, &VoteReply{})
		}()
	}
}

func (r *Raft) sendRequestVote(args VoteArgs, address string, reply *VoteReply) {
	client, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		log.Print("dialing: ", err)
		return
	}
	defer func(client *rpc.Client) {
		err := client.Close()
		if err != nil {
			log.Print("close client failed:", err)
		}
	}(client)
	err = client.Call("Raft.RequestVote", args, reply)
	if err != nil {
		log.Printf("node: %v send requrest vote to node: %v failed ", r.id, address)
		return
	}
	if reply.Term > r.currentTerm {
		log.Printf("接收到的reply大于当前的任期 %v", reply)
		r.currentTerm = reply.Term
		r.state = Follower
		r.voteFor = -1
	}
	if reply.VoteGrant {
		r.voteCount++
	}

	if r.voteCount >= len(r.nodes)/2+1 {
		//超过半数成为leader
		r.toLeaderC <- true
	}
}

func (r *Raft) broadcastHeartbeat() {
	log.Printf("当前的nodes列表:%v", r.nodes)
	for i, n := range r.nodes {
		log.Printf("需要广播心跳 index:%v  node: %v", i, n.address)
		if r.id == n.id {
			continue
		}
		//log.Printf("broadcast to %v", n)
		args := HeartbeatArgs{
			Term:         r.currentTerm,
			Leader:       r.id,
			LeaderCommit: r.commitIndex,
		}
		//也就是当前所在的日志索引
		preLogIndex := r.nextIndex[i] - 1
		if r.lastIndex() > preLogIndex {
			args.PrevLogIndex = preLogIndex
			args.PrevLogTerm = r.log[preLogIndex].LogTerm
			args.Entries = r.log[preLogIndex:]
		}

		go func() {
			r.sendHeartBeat(n, args, &HeartbeatReply{})
		}()
	}
}

func (r *Raft) sendHeartBeat(n *node, args HeartbeatArgs, reply *HeartbeatReply) {
	log.Printf("send heartbeat to :%v", n.address)
	client, err := rpc.DialHTTP("tcp", n.address)
	if err != nil {
		log.Printf("Connect to Node %v failed: %v ", n.address, err)
		return
	}
	err = client.Call("Raft.HeartBeat", args, reply)
	if err != nil {
		log.Printf("Remote call [Node %v] raft.Heartbeat failed: %v", n.address, err)
	}
	if reply.Success {
		if reply.NextIndex > 0 {
			r.nextIndex[n.id] = reply.NextIndex
			r.matchIndex[n.id] = reply.NextIndex - 1
		}
	} else {
		// 如果leader的term小于follower 需要重新选举
		if reply.Term > r.currentTerm {
			r.currentTerm = reply.Term
			r.state = Follower
			r.voteFor = -1
			return
		}
	}
}

func (r *Raft) lastIndex() int {
	index := len(r.log)
	if index == 0 {
		return 0
	}
	return r.log[index-1].LogIndex
}

func main() {
	port := flag.String("port", ":9091", "rpc listen port")
	cluster := flag.String("cluster", "127.0.0.1:9091", "comma sep")
	id := flag.Int("id", 0, "node ID")

	flag.Parse()
	clusters := strings.Split(*cluster, ",")
	ns := make(map[int]*node)
	for k, v := range clusters {
		ns[k] = newNode(v, k)
		log.Printf("Node created: %v", ns[k])
	}

	raft := &Raft{
		id:    *id,
		nodes: ns,
	}
	raft.rpc(*port)
	raft.start()
	select {}
}
