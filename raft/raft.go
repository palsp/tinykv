// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"math/rand"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout           int
	randomizedElectionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	r := &Raft{
		id:               c.ID,
		State:            StateFollower,
		Term:             0,
		Prs:              make(map[uint64]*Progress, len(c.peers)),
		votes:            make(map[uint64]bool, len(c.peers)),
		electionTimeout:  c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,
		RaftLog:          newLog(c.Storage),
	}
	last := r.RaftLog.LastIndex()
	for _, peer := range c.peers {
		r.Prs[peer] = &Progress{Match: 0, Next: last + 1}
	}

	r.randomElectionTimeout()

	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	p := r.Prs[to]
	newEntries := r.RaftLog.NewEnts(p.Next)
	if len(newEntries) == 0 {
		return false
	}

	logTerm, err := r.RaftLog.Term(p.Next - 1)
	if err != nil {
	}

	m := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Index:   p.Next - 1,
		LogTerm: logTerm,
		Commit:  r.RaftLog.committed,
		Entries: make([]*pb.Entry, len(newEntries)),
	}
	for i := range newEntries {
		m.Entries[i] = &newEntries[i]
	}
	r.send(m)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	r.send(pb.Message{
		From:    r.id,
		To:      to,
		Term:    r.Term,
		MsgType: pb.MessageType_MsgHeartbeat,
	})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	r.electionElapsed += 1
	if r.State == StateLeader {
		r.Step(pb.Message{From: r.id, To: r.id, MsgType: pb.MessageType_MsgBeat})
	}

	if r.State == StateFollower || r.State == StateCandidate {
		if r.electionElapsed >= r.randomizedElectionTimeout {
			r.startElection()
		}
	}
}
func (r *Raft) randomElectionTimeout() {
	r.randomizedElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

func (r *Raft) startElection() {
	r.becomeCandidate()

	lastIndex := r.RaftLog.LastIndex()
	lastTerm, _ := r.RaftLog.Term(lastIndex)

	for peer := range r.Prs {
		if peer != r.id {
			r.send(pb.Message{
				From:    r.id,
				To:      peer,
				Term:    r.Term,
				MsgType: pb.MessageType_MsgRequestVote,
				LogTerm: lastTerm,
				Index:   lastIndex,
			})
		}
	}

	r.tryBecomeLeader()
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	if term != r.Term {
		r.Vote = None
	}
	r.electionElapsed = 0
	r.Term = term
	r.Lead = lead
	r.State = StateFollower
	r.randomElectionTimeout()
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	r.electionElapsed = 0
	r.Term += 1
	r.State = StateCandidate
	r.votes = make(map[uint64]bool, len(r.Prs))
	r.votes[r.id] = true
	r.Vote = r.id
	r.Lead = None
	r.randomElectionTimeout()
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	r.electionElapsed = 0
	r.State = StateLeader
	r.Lead = r.id
	nextIndex := r.RaftLog.LastIndex() + 1
	for peer := range r.Prs {
		// assume follower fully caught up
		r.Prs[peer].Next = nextIndex
	}
	r.Step(pb.Message{
		From:    r.id,
		To:      r.id,
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{{
			Data: nil,
		}},
	})
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// m.Term = 0 is local message
	if m.Term != 0 && m.Term > r.Term {
		lead := None
		if m.MsgType == pb.MessageType_MsgAppend || m.MsgType == pb.MessageType_MsgHeartbeat {
			lead = m.From
		}
		r.becomeFollower(m.Term, lead)
	}

	switch r.State {
	case StateFollower:
		return r.stepFollower(m)
	case StateCandidate:
		return r.stepCandidate(m)
	case StateLeader:
		return r.stepLeader(m)
	}
	return nil
}

func (r *Raft) stepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgAppend:
		if m.Term >= r.Term {
			r.electionElapsed = 0
			r.Lead = m.From
		}

		found := false || (m.LogTerm == 0 && m.Index == 0)
		lastIndex := r.RaftLog.LastIndex()
		for _, ent := range r.RaftLog.entries {
			if ent.Index == m.Index && m.LogTerm == ent.Term {
				found = true
			}
		}

		if !found {
			r.send(pb.Message{
				MsgType: pb.MessageType_MsgAppendResponse,
				Term:    r.Term,
				Index:   lastIndex,
				From:    r.id,
				To:      m.From,
				Reject:  true,
			})
			return nil
		}

		for _, ent := range m.Entries {
			entIdx := int(ent.Index) - 1
			if entIdx < len(r.RaftLog.entries) {
				conflict := r.RaftLog.entries[entIdx].Term != ent.Term
				// Found log conflict, replace
				if conflict {
					r.RaftLog.entries = r.RaftLog.entries[:entIdx]
					r.RaftLog.stabled = ent.Index - 1
					r.RaftLog.entries = append(r.RaftLog.entries, *ent)
				}
			} else {
				r.RaftLog.entries = append(r.RaftLog.entries, *ent)
			}
		}
		r.RaftLog.committed = m.Commit
		r.send(pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			Term:    r.Term,
			Index:   m.Commit,
			From:    r.id,
			To:      m.From,
			Reject:  false,
		})
	case pb.MessageType_MsgHeartbeat:
		if r.Term >= m.Term {
			r.Term = m.Term
			r.electionElapsed = 0
		}
	case pb.MessageType_MsgHup:
		r.startElection()
	case pb.MessageType_MsgRequestVote:
		canVote := r.Vote == None || r.Vote == m.From

		lastIndex := r.RaftLog.LastIndex()
		term, _ := r.RaftLog.Term(lastIndex)
		upToDate := m.LogTerm > term || (m.LogTerm == term && m.Index >= lastIndex)
		reject := m.Term < r.Term || !canVote || !upToDate
		if !reject {
			r.Vote = m.From
			r.electionElapsed = 0
		}

		r.send(pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
			Reject:  reject,
		})
	}
	return nil
}

func (r *Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.startElection()
	case pb.MessageType_MsgAppend, pb.MessageType_MsgHeartbeat:
		if m.Term >= r.Term {
			r.becomeFollower(m.Term, m.From)
		}
	case pb.MessageType_MsgRequestVote:
		r.send(pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
			Reject:  true,
		})
	case pb.MessageType_MsgRequestVoteResponse:
		if m.Term == r.Term && !m.Reject {
			r.votes[m.From] = true

			r.tryBecomeLeader()
		}
	}

	return nil
}

func (r *Raft) stepLeader(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		for peer := range r.Prs {
			if peer != r.id {
				r.sendHeartbeat(peer)
			}
		}
	case pb.MessageType_MsgAppendResponse:
		pr := r.Prs[m.From]
		if m.Reject {
			pr.Next = min(pr.Next-1, m.Index+1)
			if pr.Next < 1 {
				pr.Next = 1
			}
			r.sendAppend(m.From)
			return nil
		}
		pr.Match = m.Index
		pr.Next = m.Index + 1
		r.tryCommit(m.Index)
	case pb.MessageType_MsgPropose:
		lastIndex := r.RaftLog.LastIndex() + 1
		for i, e := range m.Entries {
			lastIndex += uint64(i)
			r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{
				Data:      e.Data,
				EntryType: e.EntryType,
				Term:      r.Term,
				Index:     lastIndex,
			})
		}
		r.Prs[r.id].Match = lastIndex
		r.Prs[r.id].Next = lastIndex + 1
		r.handleAppendEntries(m)
		r.tryCommit(lastIndex)
	case pb.MessageType_MsgRequestVote:
		r.send(pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
			Reject:  true,
		})
	case pb.MessageType_MsgAppend:
		if m.Term > r.Term {
			r.becomeFollower(m.Term, m.From)
		}
	}
	return nil
}

func (r *Raft) tryCommit(index uint64) bool {
	if r.RaftLog.committed > index {
		return false
	}

	ack := uint64(0)
	for _, progress := range r.Prs {
		if progress.Match >= index {
			ack += 1
		}
	}
	if ack*2 > uint64(len(r.Prs)) {
		r.RaftLog.committed = index
		return true
	}

	return false
}

func (r *Raft) tryBecomeLeader() bool {
	received := uint64(0)
	for _, vote := range r.votes {
		if vote {
			received++
		}
	}
	if received*2 > uint64(len(r.Prs)) {
		r.becomeLeader()
		return true
	}

	return false
}

func (r *Raft) send(m pb.Message) {
	r.msgs = append(r.msgs, m)
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	for peer := range r.Prs {
		if peer != r.id {
			r.sendAppend(peer)
		}
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
