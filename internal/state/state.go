package state

// Role defines the current Raft role of a node (Follower, Candidate, Leader).
type Role string

const (
	Follower  Role = "Follower"
	Candidate Role = "Candidate"
	Leader    Role = "Leader"
)
