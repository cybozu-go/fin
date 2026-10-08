package prometheus

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/cybozu-go/fin/internal/model"
	"github.com/prometheus/client_golang/api"
	prometheusv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	prommodel "github.com/prometheus/common/model"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// defaultQueryTimeout bounds every query, so an unresponsive Prometheus neither holds up
// the controller startup nor blocks the worker of the FinBackupConfig controller.
const defaultQueryTimeout = 10 * time.Second

// errInvalidQuery means the query itself is wrong, which retrying cannot fix.
var errInvalidQuery = errors.New("invalid Prometheus query")

// NodeFreeSpaceRepository reads the free space of each node from Prometheus.
// The result is cached for the TTL, so reconciling many FinBackupConfigs at once
// does not send a query per FinBackupConfig.
type NodeFreeSpaceRepository struct {
	api          prometheusv1.API
	query        string
	nodeLabel    prommodel.LabelName
	ttl          time.Duration
	queryTimeout time.Duration
	now          func() time.Time

	mu        sync.Mutex
	cached    map[string]float64
	fetchedAt time.Time
}

var _ model.NodeFreeSpaceRepository = &NodeFreeSpaceRepository{}

// NewNodeFreeSpaceRepository returns a repository that runs query against the Prometheus
// at url and takes the node name from the nodeLabel label of each sample.
//
// It runs the query once to fail fast on an invalid query. Any other failure, such as
// Prometheus being down, is only logged, since the node selection retries it anyway.
func NewNodeFreeSpaceRepository(
	ctx context.Context,
	url, query, nodeLabel string,
	ttl time.Duration,
) (*NodeFreeSpaceRepository, error) {
	client, err := api.NewClient(api.Config{
		Address: url,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create a Prometheus client: %w", err)
	}
	r := &NodeFreeSpaceRepository{
		api:          prometheusv1.NewAPI(client),
		query:        query,
		nodeLabel:    prommodel.LabelName(nodeLabel),
		ttl:          ttl,
		queryTimeout: defaultQueryTimeout,
		now:          time.Now,
	}

	if _, err := r.GetNodeFreeSpace(ctx); err != nil {
		if errors.Is(err, errInvalidQuery) {
			return nil, err
		}
		log.FromContext(ctx).Error(err, "failed to check the Prometheus query on startup", "query", query)
	}
	return r, nil
}

// GetNodeFreeSpace implements model.NodeFreeSpaceRepository. The returned map is
// shared with later calls until the cache expires, so callers must not modify it.
func (r *NodeFreeSpaceRepository) GetNodeFreeSpace(ctx context.Context) (map[string]float64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := r.now()
	if r.cached != nil && now.Sub(r.fetchedAt) < r.ttl {
		return r.cached, nil
	}

	freeSpace, err := r.fetch(ctx, now)
	if err != nil {
		return nil, err
	}
	r.cached = freeSpace
	r.fetchedAt = now
	return freeSpace, nil
}

func (r *NodeFreeSpaceRepository) fetch(ctx context.Context, now time.Time) (map[string]float64, error) {
	// The context of Reconcile has no deadline, and the HTTP client does not time out
	// while waiting for a response.
	ctx, cancel := context.WithTimeout(ctx, r.queryTimeout)
	defer cancel()
	result, _, err := r.api.Query(ctx, r.query, now)
	if err != nil {
		var apiErr *prometheusv1.Error
		if errors.As(err, &apiErr) && apiErr.Type == prometheusv1.ErrBadData {
			return nil, fmt.Errorf("%w %q: %w", errInvalidQuery, r.query, err)
		}
		return nil, fmt.Errorf("failed to query Prometheus: %w", err)
	}
	vector, ok := result.(prommodel.Vector)
	if !ok {
		return nil, fmt.Errorf("%w %q: the result type is %s, not vector", errInvalidQuery, r.query, result.Type())
	}

	freeSpace := make(map[string]float64, len(vector))
	for _, sample := range vector {
		node := string(sample.Metric[r.nodeLabel])
		if node == "" {
			return nil, fmt.Errorf("prometheus sample %s has no %q label", sample.Metric, r.nodeLabel)
		}
		if _, dup := freeSpace[node]; dup {
			return nil, fmt.Errorf("prometheus returned more than one sample for node %q", node)
		}
		freeSpace[node] = float64(sample.Value)
	}
	return freeSpace, nil
}
