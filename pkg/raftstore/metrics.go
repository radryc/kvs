package raftstore

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	notLeaderTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kvs_raft_not_leader_total",
		Help: "Total KVS mutations that encountered a lost or unknown leader.",
	})
	writeRetryTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kvs_raft_write_retries_total",
		Help: "Total KVS mutation retries after a leadership change.",
	})
	forwardFailureTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kvs_raft_forward_failures_total",
		Help: "Total KVS writes that could not resolve a leader API endpoint.",
	})
	leaderTermGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "kvs_raft_leader_term",
		Help: "Current raft term reported by the local KVS store.",
	})
	isLeaderGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "kvs_raft_is_leader",
		Help: "1 when this node is the KVS raft leader, 0 otherwise.",
	})
)
