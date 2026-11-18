package state

// Role defines the current Raft role of a node (Follower, PreCandidate, Candidate, Leader).
type Role string

const (
	Follower     Role = "Follower"
	PreCandidate Role = "PreCandidate"
	Candidate    Role = "Candidate"
	Leader       Role = "Leader"
)
