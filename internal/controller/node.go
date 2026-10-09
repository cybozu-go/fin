package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	finv1 "github.com/cybozu-go/fin/api/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// nodeGVK identifies a Node in the metadata lookup below. Reading it out of a scheme
// would only trade the literal for a call that cannot fail, since core/v1 fixes these.
var nodeGVK = corev1.SchemeGroupVersion.WithKind("Node")

// nodeListGVK is the list counterpart of nodeGVK.
var nodeListGVK = corev1.SchemeGroupVersion.WithKind("NodeList")

// ParseNodeSelectorTerms parses a JSON array of NodeSelectorTerms, as written in
// a Pod's spec.affinity.nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution.
func ParseNodeSelectorTerms(s string) (*nodeaffinity.NodeSelector, error) {
	var terms []corev1.NodeSelectorTerm
	if err := json.Unmarshal([]byte(s), &terms); err != nil {
		return nil, fmt.Errorf("failed to parse node selector terms: %w", err)
	}
	if len(terms) == 0 {
		return nil, errors.New("node selector terms must not be empty")
	}
	selector, err := nodeaffinity.NewNodeSelector(&corev1.NodeSelector{NodeSelectorTerms: terms})
	if err != nil {
		return nil, fmt.Errorf("invalid node selector terms: %w", err)
	}
	return selector, nil
}

// lookupNode reads PartialObjectMetadata so a cached reader serves it from the metadata
// informer; mixing that with corev1.Node would run two informers over the same nodes.
func lookupNode(ctx context.Context, r client.Reader, nodeName string) (types.UID, bool, error) {
	var node metav1.PartialObjectMetadata
	node.SetGroupVersionKind(nodeGVK)

	if err := r.Get(ctx, client.ObjectKey{Name: nodeName}, &node); err != nil {
		if k8serrors.IsNotFound(err) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("failed to get node %q: %w", nodeName, err)
	}
	return node.GetUID(), true, nil
}

// nodeHoldsBackupData treats an empty status.nodeUID as a match: nothing is recorded
// until the first successful reconcile.
func nodeHoldsBackupData(backup *finv1.FinBackup, nodeUID types.UID) bool {
	return backup.Status.NodeUID == "" || backup.Status.NodeUID == nodeUID
}
