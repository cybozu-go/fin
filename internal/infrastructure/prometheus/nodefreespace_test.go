package prometheus

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/api"
	prometheusv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/stretchr/testify/require"
)

const (
	testQuery = `node_filesystem_avail_bytes{mountpoint="/mnt/fin-volume"}`

	badDataBody = `{"status":"error","errorType":"bad_data","error":"parse error"}`
	scalarBody  = `{"status":"success","data":{"resultType":"scalar","result":[0,"1"]}}`
)

// newTestServer serves body for every instant query and counts the queries.
func newTestServer(t *testing.T, body string) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var count atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/query" {
			http.NotFound(w, r)
			return
		}
		require.NoError(t, r.ParseForm())
		require.Equal(t, testQuery, r.Form.Get("query"))
		count.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)
	return server, &count
}

// unreachableURL returns the URL of a server that is already closed.
func unreachableURL() string {
	server := httptest.NewServer(http.NotFoundHandler())
	server.Close()
	return server.URL
}

func newTestRepository(t *testing.T, url string, now func() time.Time) *NodeFreeSpaceRepository {
	t.Helper()
	client, err := api.NewClient(api.Config{Address: url})
	require.NoError(t, err)
	return &NodeFreeSpaceRepository{
		api:          prometheusv1.NewAPI(client),
		query:        testQuery,
		nodeLabel:    "address",
		ttl:          time.Minute,
		queryTimeout: defaultQueryTimeout,
		now:          now,
	}
}

// sample is a sample of an instant vector in a Prometheus response.
type sample struct {
	metric map[string]string
	value  float64
}

// vectorBody builds the response of an instant query returning samples.
func vectorBody(samples ...sample) string {
	type jsonSample struct {
		Metric map[string]string `json:"metric"`
		Value  [2]any            `json:"value"`
	}
	result := make([]jsonSample, 0, len(samples))
	for _, s := range samples {
		result = append(result, jsonSample{
			Metric: s.metric,
			Value:  [2]any{0, strconv.FormatFloat(s.value, 'f', -1, 64)},
		})
	}
	body, err := json.Marshal(map[string]any{
		"status": "success",
		"data":   map[string]any{"resultType": "vector", "result": result},
	})
	if err != nil {
		panic(err)
	}
	return string(body)
}

func TestGetNodeFreeSpace(t *testing.T) {
	testCases := map[string]struct {
		// body is the response of Prometheus. Empty means Prometheus is unreachable.
		body         string
		want         map[string]float64
		wantErr      bool
		invalidQuery bool
	}{
		"success": {
			body: vectorBody(
				sample{metric: map[string]string{"address": "10.0.0.1", "mountpoint": "/mnt/fin-volume"}, value: 100},
				sample{metric: map[string]string{"address": "10.0.0.2", "mountpoint": "/mnt/fin-volume"}, value: 200},
			),
			want: map[string]float64{"10.0.0.1": 100, "10.0.0.2": 200},
		},
		"missing node label": {
			body:    vectorBody(sample{metric: map[string]string{"instance": "10.0.0.1:9100"}, value: 100}),
			wantErr: true,
		},
		"duplicate node": {
			body: vectorBody(
				sample{metric: map[string]string{"address": "10.0.0.1", "device": "sda"}, value: 100},
				sample{metric: map[string]string{"address": "10.0.0.1", "device": "sdb"}, value: 200},
			),
			wantErr: true,
		},
		"not a vector": {
			body:         scalarBody,
			wantErr:      true,
			invalidQuery: true,
		},
		"syntax error": {
			body:         badDataBody,
			wantErr:      true,
			invalidQuery: true,
		},
		"unreachable": {
			wantErr: true,
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			var url string
			var count *atomic.Int32
			if tc.body == "" {
				url = unreachableURL()
			} else {
				var server *httptest.Server
				server, count = newTestServer(t, tc.body)
				url = server.URL
			}
			repo := newTestRepository(t, url, time.Now)

			freeSpace, err := repo.GetNodeFreeSpace(context.Background())
			if !tc.wantErr {
				require.NoError(t, err)
				require.Equal(t, tc.want, freeSpace)
				return
			}
			require.Error(t, err)
			if tc.invalidQuery {
				require.ErrorIs(t, err, errInvalidQuery)
			} else {
				require.NotErrorIs(t, err, errInvalidQuery)
			}

			if count != nil {
				_, err = repo.GetNodeFreeSpace(context.Background())
				require.Error(t, err)
				require.EqualValues(t, 2, count.Load(), "a failed query should not be cached")
			}
		})
	}
}

func TestGetNodeFreeSpace_Timeout(t *testing.T) {
	// The server accepts the request but never responds. The handler is released before
	// Close, which waits for it to return.
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
	}))
	t.Cleanup(server.Close)
	t.Cleanup(func() { close(release) })
	repo := newTestRepository(t, server.URL, time.Now)
	repo.queryTimeout = 100 * time.Millisecond

	// The context has no deadline, as that of Reconcile.
	start := time.Now()
	_, err := repo.GetNodeFreeSpace(context.Background())
	require.Error(t, err)
	require.NotErrorIs(t, err, errInvalidQuery)
	require.Less(t, time.Since(start), 5*time.Second, "the query should be cut off by the timeout")
}

func TestGetNodeFreeSpace_Cache(t *testing.T) {
	server, count := newTestServer(t, vectorBody(sample{metric: map[string]string{"address": "10.0.0.1"}, value: 100}))
	now := time.Unix(1000, 0)
	repo := newTestRepository(t, server.URL, func() time.Time { return now })

	_, err := repo.GetNodeFreeSpace(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, 1, count.Load())

	// 59s after the first fetch is within the 1m TTL, so the cache is still valid.
	now = now.Add(59 * time.Second)
	_, err = repo.GetNodeFreeSpace(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, 1, count.Load(), "a query within the TTL should be served from the cache")

	// 60s after the first fetch is no longer less than the TTL, so the cache has expired.
	now = now.Add(time.Second)
	_, err = repo.GetNodeFreeSpace(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, 2, count.Load(), "a query after the TTL should reach Prometheus")
}

func TestNewNodeFreeSpaceRepository(t *testing.T) {
	t.Run("success fills the cache", func(t *testing.T) {
		server, count := newTestServer(t, vectorBody(sample{metric: map[string]string{"address": "10.0.0.1"}, value: 100}))

		// The constructor runs the query once to check it.
		repo, err := NewNodeFreeSpaceRepository(context.Background(), server.URL, testQuery, "address", time.Minute)
		require.NoError(t, err)
		require.EqualValues(t, 1, count.Load())

		// The result of the check is cached, so no further query is sent within the TTL.
		freeSpace, err := repo.GetNodeFreeSpace(context.Background())
		require.NoError(t, err)
		require.Equal(t, map[string]float64{"10.0.0.1": 100}, freeSpace)
		require.EqualValues(t, 1, count.Load(), "the startup query should be reused within the TTL")
	})

	// A wrong query cannot be fixed by retrying, so the constructor fails.
	invalidQueries := map[string]string{
		"syntax error": badDataBody,
		"not a vector": scalarBody,
	}
	for name, body := range invalidQueries {
		t.Run(name+" fails", func(t *testing.T) {
			server, _ := newTestServer(t, body)

			_, err := NewNodeFreeSpaceRepository(context.Background(), server.URL, testQuery, "address", time.Minute)
			require.ErrorIs(t, err, errInvalidQuery)
		})
	}

	t.Run("unreachable Prometheus does not fail", func(t *testing.T) {
		// Prometheus being down is only logged, since the node selection retries it.
		repo, err := NewNodeFreeSpaceRepository(context.Background(), unreachableURL(), testQuery, "address", time.Minute)
		require.NoError(t, err)

		// Nothing is cached, so the next call queries Prometheus again and fails.
		_, err = repo.GetNodeFreeSpace(context.Background())
		require.Error(t, err)
		require.NotErrorIs(t, err, errInvalidQuery)
	})
}
