package fake

import (
	"context"
	"maps"
	"sync"

	"github.com/cybozu-go/fin/internal/model"
)

// NodeFreeSpaceRepository is a model.NodeFreeSpaceRepository returning what the test sets.
type NodeFreeSpaceRepository struct {
	mu        sync.Mutex
	freeSpace map[string]float64
	err       error
}

var _ model.NodeFreeSpaceRepository = &NodeFreeSpaceRepository{}

func NewNodeFreeSpaceRepository(freeSpace map[string]float64) *NodeFreeSpaceRepository {
	return &NodeFreeSpaceRepository{freeSpace: maps.Clone(freeSpace)}
}

// SetFreeSpace replaces the free space returned by GetNodeFreeSpace.
func (r *NodeFreeSpaceRepository) SetFreeSpace(freeSpace map[string]float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.freeSpace = maps.Clone(freeSpace)
}

// SetError makes GetNodeFreeSpace fail with err until it is reset with nil.
func (r *NodeFreeSpaceRepository) SetError(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.err = err
}

func (r *NodeFreeSpaceRepository) GetNodeFreeSpace(_ context.Context) (map[string]float64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return nil, r.err
	}
	return maps.Clone(r.freeSpace), nil
}
