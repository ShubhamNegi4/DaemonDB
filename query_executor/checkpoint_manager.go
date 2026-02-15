package executor

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

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

func NewCheckpointManager(dbPath string) *CheckpointManager {
	return &CheckpointManager{
		checkpointPath: filepath.Join(dbPath, "checkpoint.json"),
	}
}

// SaveCheckpoint atomically saves a checkpoint
func (cm *CheckpointManager) SaveCheckpoint(lsn uint64, database string) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	checkpoint := Checkpoint{
		LSN:       lsn,
		Timestamp: getCurrentTimestamp(),
		Database:  database,
	}

	// Serialize to JSON
	data, err := json.MarshalIndent(checkpoint, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal checkpoint: %w", err)
	}

	// ====================================================================
	// CRITICAL: Atomic write pattern to prevent corruption
	// 1. Write to temporary file
	// 2. Sync temp file to disk (fsync)
	// 3. Atomically rename temp to actual file
	// ====================================================================

	tempPath := cm.checkpointPath + ".tmp"

	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write temp checkpoint: %w", err)
	}

	// Sync temp file to disk (ensure data is durable)
	tempFile, err := os.OpenFile(tempPath, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed to open temp checkpoint: %w", err)
	}
	if err := tempFile.Sync(); err != nil {
		tempFile.Close()
		return fmt.Errorf("failed to sync temp checkpoint: %w", err)
	}
	tempFile.Close()

	// Atomically rename temp to actual
	// On Unix, rename is atomic - file is either old or new, never corrupted
	if err := os.Rename(tempPath, cm.checkpointPath); err != nil {
		return fmt.Errorf("failed to rename checkpoint: %w", err)
	}

	// Sync directory to ensure rename is durable
	dir, err := os.Open(filepath.Dir(cm.checkpointPath))
	if err == nil {
		dir.Sync()
		dir.Close()
	}

	fmt.Printf("Checkpoint saved at LSN %d\n", lsn)
	return nil
}

// LoadCheckpoint loads the last checkpoint
func (cm *CheckpointManager) LoadCheckpoint() (*Checkpoint, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	if _, err := os.Stat(cm.checkpointPath); os.IsNotExist(err) {
		// No checkpoint exists - start from beginning
		return &Checkpoint{LSN: 0}, nil
	}

	data, err := os.ReadFile(cm.checkpointPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read checkpoint: %w", err)
	}

	// Parse checkpoint
	var checkpoint Checkpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		// Checkpoint file is corrupted - start from beginning
		fmt.Println("Warning: Checkpoint file corrupted, starting from LSN 0")
		return &Checkpoint{LSN: 0}, nil
	}

	return &checkpoint, nil
}

// DeleteCheckpoint removes the checkpoint file
func (cm *CheckpointManager) DeleteCheckpoint() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if err := os.Remove(cm.checkpointPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete checkpoint: %w", err)
	}

	return nil
}

// getCurrentTimestamp returns current Unix timestamp
func getCurrentTimestamp() int64 {
	return time.Now().Unix()
}
