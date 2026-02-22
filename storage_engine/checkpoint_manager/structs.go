package checkpoint

import "sync"

// CheckpointManager manages WAL checkpoints
type CheckpointManager struct {
	checkpointPath string
	mu             sync.RWMutex
}

// Checkpoint represents a recovery point in the WAL
type Checkpoint struct {
	LSN       uint64 `json:"lsn"`
	Timestamp int64  `json:"timestamp"` // only for writing the last checkpoint time, not used for replaying
	Database  string `json:"database"`
}
