package core

import (
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"time"

	"nis3607/mylogger"
	"nis3607/myrpc"
)

type State uint64

const (
	follower State = 0
	candidate State = 1
	leader State = 2

	HeartbeatInterval = 50 * time.Millisecond
	ElectionTimeout = 300 * time.Millisecond
)

// 保存共识协议的一些状态
type Consensus struct {
	id   uint8
	n    uint8
	port uint64
	last  uint64 // last -  1 表示链上最后一个区块的编号
	//BlockChain
	blockChain *BlockChain
	//logger
	logger *mylogger.MyLogger
	//rpc network
	peers []*myrpc.ClientEnd

	//message channel exapmle
	eventChan chan *myrpc.Event

	electionTimeout		time.Duration
	heartbeatInterval	time.Duration
	quorumSize			uint64

	state State
	votedFor int
	currentTerm uint64
	blockTerms []uint64 
	nextIndex   []uint64 // 用来保存peer节点的下一个区块号
	matchIndex  []uint64 // 
	reservedLog []*Block // 
	commitIndex uint64
}

func InitConsensus(config *Configuration) *Consensus {
	rand.Seed(time.Now().UnixNano())
	c := &Consensus{
		id:         config.Id,
		n:          config.N,
		port:       config.Port,
		last:        0,
		blockChain: InitBlockChain(config.Id, config.BlockSize),
		logger:     mylogger.InitLogger("node", config.Id),
		peers:      make([]*myrpc.ClientEnd, 0),

		electionTimeout:	ElectionTimeout,
		heartbeatInterval:	HeartbeatInterval,
		quorumSize:	uint64(config.N)/2 + 1,

		eventChan: make(chan *myrpc.Event, 1024),

		state:		follower,
		currentTerm: 0,
		votedFor:	-1,
		nextIndex:   make([]uint64, config.N),
		matchIndex:  make([]uint64, config.N),
		blockTerms:  make([]uint64, 0, 1024),
		reservedLog: make([]*Block, 0, 1024),
		commitIndex: 0,
	}

	c.appendBlocks(&Block{Seq: c.last})

	for _, peer := range config.Committee {
		clientEnd := &myrpc.ClientEnd{Port: uint64(peer)}
		c.peers = append(c.peers, clientEnd)
	}
	go c.serve()

	return c
}

func (c *Consensus) appendBlocks(block * Block) {
	c.reservedLog = append(c.reservedLog, block)
	c.blockTerms = append(c.blockTerms, c.currentTerm)
	c.last++
}

func (c *Consensus) propose() {
	block :=c.blockChain.getBlock(c.last)
	c.appendBlocks(block)
}

func (c *Consensus) serve() {
	rpc.Register(c)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":"+strconv.Itoa(int(c.port)))
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}


func (c *Consensus) hasVoted() bool{
	return c.votedFor != -1
}

func (c *Consensus) updateCurrentTerm(Term uint64){
	c.state = follower
	c.currentTerm = Term
	c.votedFor = -1
}

func (c *Consensus) handleHeartbeatMsg(msg *HeartbeatMsg, reply *HeartbeatReply) bool{
	reply.Term = c.currentTerm
	// Success == True 表示与leader匹配成功
	reply.Success = false
	reply.LastLogIndex = Min(msg.PrevLogIndex, c.last - 1)
	reply.From = c.id

	// 如果msg.Term小，可能是之前leader crash，拒绝心跳
	if msg.Term < c.currentTerm{
		return false
	// 如果msg.Term大，则当前节点Term过时，需要更新
	} else if msg.Term > c.currentTerm{
		c.updateCurrentTerm(msg.Term)
	// 如果该节点和心跳Term相等，则该任期内已有leader，将当前节点设为follower
	} else if c.state == candidate{
		c.state = follower
	}

	// 寻找相同块，不匹配，则返回收到心跳，但未成功匹配
	if c.last - 1 < msg.PrevLogIndex || c.blockTerms[msg.PrevLogIndex] != msg.PrevLogTerm{
		return true
	}

	// 配成功，更新当前节点区块链，具体来说是将匹配块前的链和leader发送过来的链拼接在一起
	c.reservedLog = append(c.reservedLog[:msg.PrevLogIndex + 1], msg.Entries...)
	c.last = uint64(len(c.reservedLog))
	c.blockTerms = append(c.blockTerms[:msg.PrevLogIndex + 1], msg.EntryTerms...)

	// 更新commit状态，与leader保持一致
	if msg.CommitIndex > c.commitIndex {
		for i := c.commitIndex + 1; i <= msg.CommitIndex; i++ {
			block := c.reservedLog[i]
			c.blockChain.commitBlock(block)
		}

		c.commitIndex = msg.CommitIndex
	}

	// 设置回复状态
	reply.Term = c.currentTerm
	reply.Success = true
	reply.LastLogIndex = c.last - 1

	return true
}

func (c *Consensus) handleHeartbeatReply(msg *HeartbeatReply) {
	if msg.Term > c.currentTerm{
		c.updateCurrentTerm(msg.Term)
		return
	}

	if !msg.Success && c.nextIndex[msg.From] > 0 {
		c.nextIndex[msg.From] = Min(c.nextIndex[msg.From] - 1, msg.LastLogIndex + 1)
		return
	}
	if msg.Success {
		c.nextIndex[msg.From] = Min(msg.LastLogIndex + 1, c.last)
		c.matchIndex[msg.From] = msg.LastLogIndex
		committedIndex := c.commitIndex
		for i := committedIndex + 1; i <= msg.LastLogIndex; i++ {
			matchCount := uint64(1)
			for j := range c.peers {
				if c.matchIndex[j] >= i {
					matchCount++
				}
			}

			if matchCount >= c.quorumSize {
				block := c.reservedLog[i]
				c.blockChain.commitBlock(block)
				c.commitIndex = i
			}
		}
	}

}

func (c *Consensus) handleRequestVoteMsg(msg *myrpc.RequestVoteMsg, reply *myrpc.RequestVoteReply) bool {
	reply.Term = c.currentTerm
	reply.VoteGranted = false

	// candidate Term过时，不能成为leader
	if msg.Term < c.currentTerm {
		return false
	// candidate Term新，更新当前节点Term
	} else if msg.Term > c.currentTerm {
		c.updateCurrentTerm(msg.Term)
	} else if c.hasVoted() && c.votedFor != int(msg.CandidateId) {
		return false
	}

	lastLogIndex := c.last - 1
	if lastLogIndex > msg.LastLogIndex || c.blockTerms[lastLogIndex] > msg.LastLogTerm {
		return false
	}

	reply.Term = c.currentTerm
	c.votedFor = int(msg.CandidateId)
	reply.VoteGranted = true
	return true
}

func (c *Consensus) OnReceiveHeartbeat(msg *HeartbeatMsg, reply *HeartbeatReply) error {
	// rpc通信：调用该函数
	// 创建Event类型变量，Err用于阻塞
	event := &myrpc.Event {
		Message: msg,
		Reply:	reply,
		Err:	make(chan error, 1),
	}
	// 将Msg信息传入主线程中，并阻塞
	c.eventChan <- event
	//等待回调
	err := <-event.Err
	return err
}

func (c *Consensus) OnReceiveRequestVoteMsg(msg *myrpc.RequestVoteMsg, reply *myrpc.RequestVoteReply) error {
	event := &myrpc.Event{
		Message: msg,
		Reply:	reply,
		Err:	make(chan error, 1),
	}
	c.eventChan <- event

	err := <-event.Err
	return err
}


func (c *Consensus) followerLoop(){
	c.logger.DPrintf("Become follower at terms: %v", c.currentTerm)
	// c.logger.DPrintf("111")
	timeout := after(c.electionTimeout, 2 * c.electionTimeout)

	// follower时一直监听心跳或投票请求
	for c.state == follower {
		update := false
		select {
		case event := <-c.eventChan:
			switch msg := event.Message.(type) {
			case *HeartbeatMsg:
				update = c.handleHeartbeatMsg(msg, event.Reply.(*HeartbeatReply))
			case *myrpc.RequestVoteMsg:
				update = c.handleRequestVoteMsg(msg, event.Reply.(*myrpc.RequestVoteReply))
			}
			event.Err <- nil

		// 超时未接受到心跳或投票请求，认为leader crash，进入选举期，成为candidate
		case <- timeout:
			c.state = candidate
		}

		// update == true,说明leader或者candidate存在，刷新超时时间
		if update {
			timeout = after(c.electionTimeout, 2 * c.electionTimeout)
		}
	}
}	

func (c *Consensus) candidateLoop() {
	c.logger.DPrintf("Become candidate at terms: %v", c.currentTerm)

	broadcastRequestVote := true

	var votesCount uint64
	var timeout <-chan time.Time
	var votesReply chan *myrpc.RequestVoteReply

	for c.state == candidate{
		// 广播投票请求
		if broadcastRequestVote {
			c.currentTerm++
			c.votedFor = int(c.id)
			votesCount = 1
			timeout = after(c.electionTimeout, 2 * c.electionTimeout)

			votesReply = make(chan *myrpc.RequestVoteReply, c.n)
			msg := &myrpc.RequestVoteMsg{
				Term:			c.currentTerm,
				LastLogIndex:	c.last - 1,
				LastLogTerm:	c.blockTerms[c.last - 1],
				CandidateId:	c.id,
			}

			for id := range c.peers {
				if id == int(c.id) {
					continue
				}

				go func(p *myrpc.ClientEnd) {
					reply := &myrpc.RequestVoteReply{}
					p.Call("Consensus.OnReceiveRequestVoteMsg", msg, reply)
					votesReply <- reply
				}(c.peers[id])
			}
			// 一次广播结束后先暂停
			broadcastRequestVote = false
		}

		select {
		case votes := <-votesReply:
			if votes.VoteGranted && votes.Term == c.currentTerm{
				votesCount++
				if votesCount == c.quorumSize {
					c.state = leader
					return
				}
			}

			if votes.Term > c.currentTerm {
				c.state = follower
				c.currentTerm = votes.Term
				c.votedFor = -1
				return
			}
		case event := <-c.eventChan:
			switch msg := event.Message.(type) {
			case *HeartbeatMsg:
				_ = c.handleHeartbeatMsg(msg, event.Reply.(*HeartbeatReply))
			case *myrpc.RequestVoteMsg:
				_ = c.handleRequestVoteMsg(msg, event.Reply.(*myrpc.RequestVoteReply))
			}

			// 回调
			event.Err <- nil
		
		// 超时后（仍在选举期内），则重新广播投票请求
		case <-timeout:
			broadcastRequestVote = true
		}

	}
}

func (c *Consensus) leaderLoop() {
	c.logger.DPrintf("Become leader at terms: %v", c.currentTerm)

	// time.NewTicker是周期性定时器
	ticker := time.NewTicker(c.heartbeatInterval)
	defer ticker.Stop()
	heartbeatReply := make(chan *HeartbeatReply, 1024)

	for id := range c.peers {
		c.nextIndex[id] = c.last
		c.matchIndex[id] = 0
	}
	for c.state == leader {
		select {
		case <- ticker.C:
			for id := range c.peers {
				if id == int(c.id) {
					continue
				}

				prevLogIndex := c.nextIndex[id] - 1
				msg := &HeartbeatMsg {
					Term:			c.currentTerm,
					PrevLogIndex:	prevLogIndex,
					PrevLogTerm:	c.blockTerms[prevLogIndex],
					EntryTerms:		c.blockTerms[c.nextIndex[id]:],
					Entries:		c.reservedLog[c.nextIndex[id]:],
					CommitIndex: 	c.commitIndex,
					Leaderid:		c.id,
				}
				go func(p *myrpc.ClientEnd) {
					reply := &HeartbeatReply{}
					p.Call("Consensus.OnReceiveHeartbeat", msg, reply)
					heartbeatReply <- reply
				}(c.peers[id])
			}

		case msg := <-heartbeatReply:
			c.handleHeartbeatReply(msg)
		case event := <-c.eventChan:
			switch msg := event.Message.(type) {
				case *myrpc.RequestVoteMsg:
					_ = c.handleRequestVoteMsg(msg, event.Reply.(*myrpc.RequestVoteReply))
				case *HeartbeatMsg:
					_ = c.handleHeartbeatMsg(msg, event.Reply.(*HeartbeatReply))
				}
				// 回调
				event.Err <- nil
		}

		c.propose()
	}
}

func (c *Consensus) Run() {
	// wait for other node to start
	time.Sleep(time.Duration(1) * time.Second)
	//init rpc client
	for id := range c.peers {
		c.peers[id].Connect()
	}

	state := c.state
	for{
		switch state{
		case follower: c.followerLoop()
		case candidate: c.candidateLoop()
		case leader: c.leaderLoop()
		}
		//update c state
		state = c.state
	}
}
