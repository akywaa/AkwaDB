package cluster

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/akywaa/akwadb/server"
)

type NodeConfig struct {
	NodeID    string
	BindAddr  string
	DataDir   string
	Peers     []string
	IsLeader  bool
	ElectionTick int
	HeartbeatTick int
}

func (c NodeConfig) electionTimeout() time.Duration {
	ticks := c.ElectionTick
	if ticks <= 0 {
		ticks = 10
	}
	return time.Duration(ticks) * 100 * time.Millisecond
}

func (c NodeConfig) heartbeatTimeout() time.Duration {
	ticks := c.HeartbeatTick
	if ticks <= 0 {
		ticks = 3
	}
	return time.Duration(ticks) * 100 * time.Millisecond
}

type LogEntry struct {
	Term    uint64
	Index   uint64
	Command raftCommand
}

type Node struct {
	config  NodeConfig
	fsm     *EngineFSM
	db      server.DB

	mu          sync.RWMutex
	term        uint64
	votedFor    string
	state       nodeState
	leaderID    string
	log         []LogEntry
	commitIndex uint64
	lastApplied uint64
	nextIndex   map[string]uint64
	matchIndex  map[string]uint64

	peers     map[string]*peerConn
	listener  net.Listener
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	applyCh   chan LogEntry
	voteCh    chan voteRequest
	stepDown  chan struct{}
	lastLogIndex uint64
}

type nodeState int32

const (
	stateFollower  nodeState = iota
	stateCandidate
	stateLeader
)

type peerConn struct {
	addr   string
	conn   net.Conn
	mu     sync.Mutex
	rpcMu  sync.Mutex // serializes write+read pairs on the shared TCP socket
	closed bool
}

type voteRequest struct {
	candidateID string
	term        uint64
	index       uint64
	resp        chan voteResponse
}

type voteResponse struct {
	term        uint64
	voteGranted bool
}

type appendReq struct {
	Term         uint64
	LeaderID     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

type appendResp struct {
	Term    uint64
	Success bool
	Index   uint64
}

type voteReq struct {
	Term        uint64
	CandidateID string
	LastLogIndex uint64
	LastLogTerm  uint64
}

type voteResp struct {
	Term        uint64
	VoteGranted bool
}

func NewNode(config NodeConfig, db server.DB) *Node {
	ctx, cancel := context.WithCancel(context.Background())
	n := &Node{
		config: config,
		fsm:    NewEngineFSM(db),
		db:     db,
		peers:  make(map[string]*peerConn),
		ctx:    ctx,
		cancel: cancel,
		applyCh: make(chan LogEntry, 4096),
		voteCh:  make(chan voteRequest, 128),
		stepDown: make(chan struct{}, 1),
		nextIndex:  make(map[string]uint64),
		matchIndex: make(map[string]uint64),
	}
	n.state = stateFollower
	return n
}

func (n *Node) hardStatePath() string {
	return filepath.Join(n.config.DataDir, "hard_state")
}

// persistHardState synchronously flushes currentTerm and votedFor to disk.
func (n *Node) persistHardState() error {
	if n.config.DataDir == "" {
		return nil
	}
	_ = os.MkdirAll(n.config.DataDir, 0755)

	votedFor := []byte(n.votedFor)
	buf := make([]byte, 8+4+len(votedFor))
	binary.BigEndian.PutUint64(buf[0:8], n.term)
	binary.BigEndian.PutUint32(buf[8:12], uint32(len(votedFor)))
	copy(buf[12:], votedFor)

	tmp := n.hardStatePath() + ".tmp"
	if err := os.WriteFile(tmp, buf, 0644); err != nil {
		return err
	}
	if err := os.Rename(tmp, n.hardStatePath()); err != nil {
		return err
	}
	f, err := os.Open(n.hardStatePath())
	if err != nil {
		return err
	}
	err = f.Sync()
	f.Close()
	return err
}

// loadHardState reads persisted term/votedFor. Returns false if no state file exists.
func (n *Node) logPath() string {
	return filepath.Join(n.config.DataDir, "raft_log")
}

// persistLogEntry appends a single log entry to the Raft log file.
func (n *Node) persistLogEntry(entry LogEntry) error {
	if n.config.DataDir == "" {
		return nil
	}
	_ = os.MkdirAll(n.config.DataDir, 0755)

	data := encodeRaftCommand(entry.Command)

	// Format: [term(8)][index(8)][cmdLen(4)][cmdData]
	var hdr [20]byte
	binary.BigEndian.PutUint64(hdr[0:8], entry.Term)
	binary.BigEndian.PutUint64(hdr[8:16], entry.Index)
	binary.BigEndian.PutUint32(hdr[16:20], uint32(len(data)))

	f, err := os.OpenFile(n.logPath(), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := f.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		return err
	}
	return f.Sync()
}

// loadLogEntries reads persisted Raft log entries from disk.
func (n *Node) loadLogEntries() []LogEntry {
	data, err := os.ReadFile(n.logPath())
	if err != nil || len(data) == 0 {
		return nil
	}

	var entries []LogEntry
	off := 0
	for off+20 <= len(data) {
		term := binary.BigEndian.Uint64(data[off : off+8])
		index := binary.BigEndian.Uint64(data[off+8 : off+16])
		cmdLen := int(binary.BigEndian.Uint32(data[off+16 : off+20]))
		off += 20
		if off+cmdLen > len(data) {
			break
		}
		cmd, err := decodeRaftCommand(data[off : off+cmdLen])
		if err != nil {
			off += cmdLen
			continue
		}
		off += cmdLen
		entries = append(entries, LogEntry{
			Term:    term,
			Index:   index,
			Command: cmd,
		})
	}
	return entries
}

// truncateLog removes persisted log entries up to (and including) the given index.
func (n *Node) truncateLog(upToIndex uint64) {
	if n.config.DataDir == "" {
		return
	}
	logPath := n.logPath()
	data, err := os.ReadFile(logPath)
	if err != nil || len(data) == 0 {
		return
	}

	// Find the offset of the first entry with index > upToIndex
	off := 0
	for off+20 <= len(data) {
		index := binary.BigEndian.Uint64(data[off+8 : off+16])
		cmdLen := int(binary.BigEndian.Uint32(data[off+16 : off+20]))
		off += 20 + cmdLen
		if index > upToIndex {
			off -= 20 + cmdLen // back up to this entry
			break
		}
	}

	if off < len(data) {
		_ = os.WriteFile(logPath, data[:off], 0644)
	}
}

func (n *Node) loadHardState() bool {
	data, err := os.ReadFile(n.hardStatePath())
	if err != nil || len(data) < 12 {
		return false
	}
	n.term = binary.BigEndian.Uint64(data[0:8])
	vLen := binary.BigEndian.Uint32(data[8:12])
	if int(vLen) <= len(data)-12 {
		n.votedFor = string(data[12 : 12+vLen])
	}
	return true
}

func (n *Node) Start() error {
	ln, err := net.Listen("tcp", n.config.BindAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", n.config.BindAddr, err)
	}
	n.listener = ln

	n.loadHardState()

	// Recover persisted Raft log entries
	if persisted := n.loadLogEntries(); len(persisted) > 0 {
		n.log = append(n.log, persisted...)
		n.lastApplied = persisted[len(persisted)-1].Index
	}

	if n.config.IsLeader {
		n.becomeLeader()
	}

	n.wg.Add(1)
	go n.serveListener()

	n.wg.Add(1)
	go n.electionTimer()

	n.wg.Add(1)
	go n.applyLoop()

	for _, peer := range n.config.Peers {
		n.connectPeer(peer)
	}

	slog.Info("cluster node started", "id", n.config.NodeID, "addr", n.config.BindAddr, "state", n.state)
	return nil
}

func (n *Node) serveListener() {
	defer n.wg.Done()
	for {
		conn, err := n.listener.Accept()
		if err != nil {
			select {
			case <-n.ctx.Done():
				return
			default:
				slog.Error("accept error", "err", err)
				continue
			}
		}
		n.wg.Add(1)
		go n.handleConn(conn)
	}
}

func (n *Node) handleConn(conn net.Conn) {
	defer n.wg.Done()
	defer conn.Close()

	for {
		msg, err := readMessage(conn)
		if err != nil {
			return
		}

		switch msg.Type {
		case msgAppendEntries:
			var req appendReq
			if err := json.Unmarshal(msg.Data, &req); err != nil {
				return
			}
			resp := n.handleAppendEntries(req)
			respData, _ := json.Marshal(resp)
			writeMessage(conn, msgAppendResp, respData)

		case msgVoteRequest:
			var req voteReq
			if err := json.Unmarshal(msg.Data, &req); err != nil {
				return
			}
			resp := n.handleVoteRequest(req)
			respData, _ := json.Marshal(resp)
			writeMessage(conn, msgVoteResp, respData)

		case msgAppendResp, msgVoteResp:
			// handled synchronously in sendAppend/sendVote
		}
	}
}

func (n *Node) handleAppendEntries(req appendReq) appendResp {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term < n.term {
		return appendResp{Term: n.term, Success: false}
	}

	if req.Term > n.term {
		n.term = req.Term
		n.votedFor = ""
		n.stepToFollower(req.LeaderID)
		_ = n.persistHardState()
	}

	n.leaderID = req.LeaderID

	if req.PrevLogIndex > 0 {
		if req.PrevLogIndex > uint64(len(n.log)) {
			return appendResp{Term: n.term, Success: false}
		}
		if req.PrevLogIndex > 0 && n.log[req.PrevLogIndex-1].Term != req.PrevLogTerm {
			n.log = n.log[:req.PrevLogIndex-1]
			return appendResp{Term: n.term, Success: false}
		}
	}

	for _, entry := range req.Entries {
		idx := entry.Index - 1
		if idx < uint64(len(n.log)) {
			if n.log[idx].Term != entry.Term {
				n.log = n.log[:idx]
				n.log = append(n.log, entry)
			}
		} else {
			n.log = append(n.log, entry)
		}
	}

	if req.LeaderCommit > n.commitIndex {
		newCommit := req.LeaderCommit
		if newCommit > uint64(len(n.log)) {
			newCommit = uint64(len(n.log))
		}
		n.commitIndex = newCommit
		n.sendCommittedEntries()
	}

	return appendResp{Term: n.term, Success: true, Index: uint64(len(n.log))}
}

func (n *Node) handleVoteRequest(req voteReq) voteResp {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term < n.term {
		return voteResp{Term: n.term, VoteGranted: false}
	}

	if req.Term > n.term {
		n.term = req.Term
		n.votedFor = ""
		n.stepToFollower("")
		_ = n.persistHardState()
	}

	lastLogIndex := uint64(len(n.log))
	var lastLogTerm uint64
	if lastLogIndex > 0 {
		lastLogTerm = n.log[lastLogIndex-1].Term
	}

	logOk := req.LastLogTerm > lastLogTerm ||
		(req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIndex)

	if (n.votedFor == "" || n.votedFor == req.CandidateID) && logOk {
		n.votedFor = req.CandidateID
		_ = n.persistHardState()
		return voteResp{Term: n.term, VoteGranted: true}
	}

	return voteResp{Term: n.term, VoteGranted: false}
}

func (n *Node) stepToFollower(leaderID string) {
	if n.state == stateLeader {
		n.stepDown <- struct{}{}
	}
	n.state = stateFollower
	n.leaderID = leaderID
}

// sendCommittedEntries enqueues all entries between lastApplied+1 and commitIndex
// into applyCh so the applyLoop can apply them to the FSM.
// Must be called while holding n.mu.
func (n *Node) sendCommittedEntries() {
	for n.lastApplied < n.commitIndex && n.lastApplied < uint64(len(n.log)) {
		n.lastApplied++
		entry := n.log[n.lastApplied-1]
		select {
		case n.applyCh <- entry:
		case <-n.ctx.Done():
			return
		}
	}
}

func (n *Node) electionTimer() {
	defer n.wg.Done()
	timer := time.NewTimer(n.config.electionTimeout())
	defer timer.Stop()

	for {
		select {
		case <-n.ctx.Done():
			return
		case <-n.stepDown:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(n.config.electionTimeout())
		case <-timer.C:
			n.mu.RLock()
			state := n.state
			n.mu.RUnlock()

			if state == stateFollower {
				n.startElection()
			}
			timer.Reset(n.config.electionTimeout())
		}
	}
}

func (n *Node) startElection() {
	n.mu.Lock()
	n.term++
	n.state = stateCandidate
	n.votedFor = n.config.NodeID
	n.leaderID = ""
	_ = n.persistHardState()
	term := n.term
	lastLogIndex := uint64(len(n.log))
	var lastLogTerm uint64
	if lastLogIndex > 0 {
		lastLogTerm = n.log[lastLogIndex-1].Term
	}
	n.mu.Unlock()

	slog.Info("starting election", "term", term)

	votes := int32(1)
	// full cluster size N = peers + self; majority = N/2 + 1
	needed := int32((len(n.config.Peers) + 1) / 2 + 1)
	var mu sync.Mutex

	for _, peerAddr := range n.config.Peers {
		go func(addr string) {
			req := voteReq{
				Term:         term,
				CandidateID:  n.config.NodeID,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			var resp voteResp
			if err := n.rpcVote(addr, req, &resp); err != nil {
				return
			}

			if resp.Term > term {
				n.mu.Lock()
				if resp.Term > n.term {
					n.term = resp.Term
					n.votedFor = ""
					n.stepToFollower("")
					_ = n.persistHardState()
				}
				n.mu.Unlock()
				return
			}

			if resp.VoteGranted {
				mu.Lock()
				votes++
				if votes >= needed {
					mu.Unlock()
					n.mu.Lock()
					if n.term == term && n.state == stateCandidate {
						n.becomeLeaderLocked()
					}
					n.mu.Unlock()
					return
				}
				mu.Unlock()
			}
		}(peerAddr)
	}
}

func (n *Node) becomeLeader() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.becomeLeaderLocked()
}

func (n *Node) becomeLeaderLocked() {
	n.state = stateLeader
	n.leaderID = n.config.NodeID
	nextIdx := uint64(len(n.log)) + 1
	for _, peer := range n.config.Peers {
		n.nextIndex[peer] = nextIdx
		n.matchIndex[peer] = 0
	}

	slog.Info("became leader", "term", n.term)

	n.wg.Add(1)
	go n.heartbeatLoop()
}

func (n *Node) heartbeatLoop() {
	defer n.wg.Done()
	ticker := time.NewTicker(n.config.heartbeatTimeout())
	defer ticker.Stop()

	for {
		select {
		case <-n.ctx.Done():
			return
		case <-n.stepDown:
			return
		case <-ticker.C:
			n.mu.RLock()
			state := n.state
			n.mu.RUnlock()
			if state != stateLeader {
				return
			}
			n.broadcastAppend()
		}
	}
}

func (n *Node) broadcastAppend() {
	n.mu.RLock()
	leaderID := n.config.NodeID
	term := n.term
	commitIndex := n.commitIndex
	logLen := uint64(len(n.log))
	peers := make([]string, 0, len(n.config.Peers))
	for _, p := range n.config.Peers {
		peers = append(peers, p)
	}
	n.mu.RUnlock()

	for _, peerAddr := range peers {
		go func(addr string) {
			n.sendAppendEntries(addr, leaderID, term, commitIndex, logLen)
		}(peerAddr)
	}
}

func (n *Node) sendAppendEntries(addr, leaderID string, term, commitIndex, logLen uint64) {
	n.mu.Lock()
	nextIdx := n.nextIndex[addr]
	var prevLogIndex, prevLogTerm uint64
	if nextIdx > 1 {
		prevLogIndex = nextIdx - 1
		if prevLogIndex-1 < uint64(len(n.log)) {
			prevLogTerm = n.log[prevLogIndex-1].Term
		}
	}
	var entries []LogEntry
	for i := nextIdx - 1; i < logLen; i++ {
		entries = append(entries, n.log[i])
	}
	n.mu.Unlock()

	req := appendReq{
		Term:         term,
		LeaderID:     leaderID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: commitIndex,
	}

	var resp appendResp
	if err := n.rpcAppend(addr, req, &resp); err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if resp.Term > n.term {
		n.term = resp.Term
		n.votedFor = ""
		n.stepToFollower("")
		_ = n.persistHardState()
		return
	}

	if n.state != stateLeader || term != n.term {
		return
	}

	if resp.Success {
		newMatch := prevLogIndex + uint64(len(entries))
		if newMatch > n.matchIndex[addr] {
			n.matchIndex[addr] = newMatch
			n.nextIndex[addr] = newMatch + 1
		}
		n.maybeAdvanceCommitLocked()
	} else {
		if n.nextIndex[addr] > 1 {
			n.nextIndex[addr]--
		}
	}
}

func (n *Node) maybeAdvanceCommitLocked() {
	for idx := uint64(len(n.log)); idx > n.commitIndex; idx-- {
		if n.log[idx-1].Term != n.term {
			continue
		}
		count := 1
		for _, match := range n.matchIndex {
			if match >= idx {
				count++
			}
		}
		if count > len(n.config.Peers)/2 {
			n.commitIndex = idx
			n.sendCommittedEntries()
			break
		}
	}
}

func (n *Node) applyLoop() {
	defer n.wg.Done()
	for {
		select {
		case <-n.ctx.Done():
			return
		case entry := <-n.applyCh:
			result := n.fsm.Apply(encodeRaftCommand(entry.Command))
			if result != nil {
				if _, ok := result.(error); ok {
					slog.Error("FSM apply failed", "result", result)
				}
			}
			n.mu.Lock()
			n.lastApplied = entry.Index
			n.mu.Unlock()
		}
	}
}

func (n *Node) IsLeader() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.state == stateLeader
}

func (n *Node) LeaderAddr() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.leaderID
}

func (n *Node) ApplyWrite(entries []server.BatchWriteEntry) error {
	if !n.IsLeader() {
		return fmt.Errorf("NOTLEADER: current leader is %s", n.LeaderAddr())
	}

	cmd := raftCommand{
		Op:      "batch_apply",
		Entries: entries,
	}

	n.mu.Lock()
	n.log = append(n.log, LogEntry{
		Term:    n.term,
		Index:   uint64(len(n.log)) + 1,
		Command: cmd,
	})
	entry := n.log[len(n.log)-1]
	n.mu.Unlock()

	// Persist log entry to disk before broadcasting to ensure durability.
	if err := n.persistLogEntry(entry); err != nil {
		slog.Error("failed to persist log entry", "index", entry.Index, "err", err)
	}

	n.broadcastAppend()

	deadline := time.After(5 * time.Second)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			return fmt.Errorf("timeout waiting for commit")
		case <-ticker.C:
			n.mu.RLock()
			applied := n.lastApplied
			n.mu.RUnlock()
			if applied >= entry.Index {
				return nil
			}
		case <-n.ctx.Done():
			return fmt.Errorf("node shutting down")
		}
	}
}

func (n *Node) RequestVote(candidateID string, candidateTerm uint64) bool {
	n.mu.Lock()
	defer n.mu.Unlock()

	if candidateTerm > n.term {
		n.term = candidateTerm
		n.votedFor = ""
		n.stepToFollower("")
		_ = n.persistHardState()
	}

	if n.votedFor == "" || n.votedFor == candidateID {
		n.votedFor = candidateID
		_ = n.persistHardState()
		return true
	}
	return false
}

func (n *Node) Stop() {
	n.cancel()
	if n.listener != nil {
		n.listener.Close()
	}
	n.mu.Lock()
	for _, peer := range n.peers {
		peer.mu.Lock()
		peer.closed = true
		if peer.conn != nil {
			peer.conn.Close()
		}
		peer.mu.Unlock()
	}
	n.mu.Unlock()
	n.wg.Wait()
	slog.Info("cluster node stopped", "id", n.config.NodeID)
}

// connectPeer establishes a persistent TCP connection to a peer.
func (n *Node) connectPeer(addr string) {
	pc := &peerConn{addr: addr}
	n.mu.Lock()
	n.peers[addr] = pc
	n.mu.Unlock()

	n.wg.Add(1)
	go n.maintainPeer(pc)
}

func (n *Node) maintainPeer(pc *peerConn) {
	defer n.wg.Done()
	for {
		select {
		case <-n.ctx.Done():
			return
		default:
		}

		conn, err := net.DialTimeout("tcp", pc.addr, 3*time.Second)
		if err != nil {
			time.Sleep(time.Second)
			continue
		}

		pc.mu.Lock()
		pc.conn = conn
		pc.mu.Unlock()

		// wait for context cancellation; connection health is detected
		// by RPC read/write errors, not by a dedicated reader goroutine
		select {
		case <-n.ctx.Done():
			return
		}
	}
}

func (n *Node) getConn(addr string) net.Conn {
	pc, ok := n.peers[addr]
	if !ok {
		return nil
	}
	pc.mu.Lock()
	defer pc.mu.Unlock()
	return pc.conn
}

func (n *Node) rpcVote(addr string, req voteReq, resp *voteResp) error {
	data, err := json.Marshal(req)
	if err != nil {
		return err
	}
	pc, ok := n.peers[addr]
	if !ok {
		return fmt.Errorf("no peer %s", addr)
	}
	pc.rpcMu.Lock()
	defer pc.rpcMu.Unlock()
	pc.mu.Lock()
	conn := pc.conn
	if conn == nil {
		pc.mu.Unlock()
		return fmt.Errorf("no connection to %s", addr)
	}
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	defer conn.SetDeadline(time.Time{})
	if err := writeMessage(conn, msgVoteRequest, data); err != nil {
		pc.mu.Unlock()
		return err
	}
	pc.mu.Unlock()
	respData, err := readMessageOfType(conn, msgVoteResp)
	if err != nil {
		return err
	}
	return json.Unmarshal(respData, resp)
}

func (n *Node) rpcAppend(addr string, req appendReq, resp *appendResp) error {
	data, err := json.Marshal(req)
	if err != nil {
		return err
	}
	pc, ok := n.peers[addr]
	if !ok {
		return fmt.Errorf("no peer %s", addr)
	}
	pc.rpcMu.Lock()
	defer pc.rpcMu.Unlock()
	pc.mu.Lock()
	conn := pc.conn
	if conn == nil {
		pc.mu.Unlock()
		return fmt.Errorf("no connection to %s", addr)
	}
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	defer conn.SetDeadline(time.Time{})
	if err := writeMessage(conn, msgAppendEntries, data); err != nil {
		pc.mu.Unlock()
		return err
	}
	pc.mu.Unlock()
	respData, err := readMessageOfType(conn, msgAppendResp)
	if err != nil {
		return err
	}
	return json.Unmarshal(respData, resp)
}

const (
	msgAppendEntries byte = iota + 1
	msgAppendResp
	msgVoteRequest
	msgVoteResp
)

type wireMsg struct {
	Type byte
	Data []byte
}

func writeMessage(conn net.Conn, typ byte, data []byte) error {
	var hdr [5]byte
	hdr[0] = typ
	binary.BigEndian.PutUint32(hdr[1:5], uint32(len(data)))
	if _, err := conn.Write(hdr[:]); err != nil {
		return err
	}
	_, err := conn.Write(data)
	return err
}

func readMessage(conn net.Conn) (wireMsg, error) {
	var hdr [5]byte
	if _, err := io.ReadFull(conn, hdr[:]); err != nil {
		return wireMsg{}, err
	}
	typ := hdr[0]
	length := binary.BigEndian.Uint32(hdr[1:5])
	data := make([]byte, length)
	if _, err := io.ReadFull(conn, data); err != nil {
		return wireMsg{}, err
	}
	return wireMsg{Type: typ, Data: data}, nil
}

func readMessageOfType(conn net.Conn, expectedType byte) ([]byte, error) {
	msg, err := readMessage(conn)
	if err != nil {
		return nil, err
	}
	if msg.Type != expectedType {
		return nil, fmt.Errorf("unexpected message type: %d", msg.Type)
	}
	return msg.Data, nil
}
