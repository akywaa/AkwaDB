package cluster

import (
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/akywaa/akwadb/server"
	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb/v2"
)

type Node struct {
	raftNode    *raft.Raft
	fsm         *EngineFSM
	db          server.DB
	nextVersion atomic.Uint64
	transport   *raft.NetworkTransport
	logStore    *boltdb.BoltStore
	stableStore *boltdb.BoltStore
}

func NewNode(nodeID, bindAddr, dataDir string, bootstrap bool, db server.DB) (*Node, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}

	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(nodeID)

	tcpAddr, err := net.ResolveTCPAddr("tcp", bindAddr)
	if err != nil {
		return nil, err
	}
	transport, err := raft.NewTCPTransport(bindAddr, tcpAddr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, err
	}

	logStore, err := boltdb.NewBoltStore(filepath.Join(dataDir, "raft-log.db"))
	if err != nil {
		return nil, err
	}
	stableStore, err := boltdb.NewBoltStore(filepath.Join(dataDir, "raft-stable.db"))
	if err != nil {
		return nil, err
	}

	snapshotStore, err := raft.NewFileSnapshotStore(dataDir, 2, os.Stderr)
	if err != nil {
		return nil, err
	}

	fsm := NewEngineFSM(db)
	r, err := raft.NewRaft(config, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, err
	}

	if bootstrap {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		r.BootstrapCluster(configuration)
	}

	node := &Node{
		raftNode:    r,
		fsm:         fsm,
		db:          db,
		transport:   transport,
		logStore:    logStore,
		stableStore: stableStore,
	}
	node.nextVersion.Store(db.CurrentVersion())
	return node, nil
}

func (n *Node) IsLeader() bool {
	return n.raftNode.State() == raft.Leader
}

func (n *Node) LeaderAddr() string {
	return string(n.raftNode.Leader())
}

func (n *Node) ApplyWrite(entries []server.BatchWriteEntry) error {
	cmd := raftCommand{Op: "batch_apply", Entries: entries, Version: n.nextVersion.Add(1)}
	data := encodeRaftCommand(cmd)

	future := n.raftNode.Apply(data, 5*time.Second)
	if err := future.Error(); err != nil {
		return err
	}
	res := future.Response()
	if err, ok := res.(error); ok && err != nil {
		return err
	}
	return nil
}

func (n *Node) ApplyCommand(op string, args []string) (interface{}, error) {
	cmd := raftCommand{Op: op, Args: args}
	data := encodeRaftCommand(cmd)

	future := n.raftNode.Apply(data, 5*time.Second)
	if err := future.Error(); err != nil {
		return nil, err
	}
	res := future.Response()
	if err, ok := res.(error); ok && err != nil {
		return nil, err
	}
	return res, nil
}

func (n *Node) Join(nodeID, addr string) error {
	configFuture := n.raftNode.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return err
	}

	future := n.raftNode.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, 0)
	return future.Error()
}

func (n *Node) Stop() {
	_ = n.raftNode.Shutdown().Error()
	if n.transport != nil {
		_ = n.transport.Close()
	}
	if n.logStore != nil {
		_ = n.logStore.Close()
	}
	if n.stableStore != nil {
		_ = n.stableStore.Close()
	}
}