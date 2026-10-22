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
	"fmt"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	firstIndex uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	r := &RaftLog{
		storage: storage,
		entries: make([]pb.Entry, 0),
	}

	first, err := storage.FirstIndex()
	if err != nil {
		fmt.Printf("failed to get first index from storage: %v\n", err)
		return r
	}
	r.firstIndex = first

	last, err := storage.LastIndex()
	if err != nil {
		fmt.Printf("failed to get last index from storage: %v\n", err)
		return r
	}

	entries, err := storage.Entries(first, last+1)
	if err != nil {
		fmt.Printf("failed to get log entries from storage: %v\n", err)
		return r
	}

	r.entries = entries
	r.stabled = last
	return r
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	return l.entries
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	if len(l.entries) == 0 {
		return []pb.Entry{}
	}
	return l.entries[l.toArrIndex(l.stabled+1):]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	if len(l.entries) == 0 || l.committed <= l.applied {
		return nil
	}

	return l.entries[l.toArrIndex(l.applied+1):l.toArrIndex(l.committed+1)]
}

func (l *RaftLog) NewEnts(next uint64) (ents []pb.Entry) {
	if len(l.entries) == 0 {
		return nil
	}
	return l.entries[l.toArrIndex(next):]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	return l.firstIndex + uint64(len(l.entries)) - 1
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	if i == 0 {
		return 0, nil
	}

	if len(l.entries) == 0 {
		return 0, nil
	}

	idx := l.toArrIndex(i)
	if int(idx) >= len(l.entries) {
		return 0, fmt.Errorf("index out of bound")
	}
	return l.entries[idx].Term, nil
}

func (l *RaftLog) toArrIndex(i uint64) uint64 {
	return i - l.firstIndex
}
