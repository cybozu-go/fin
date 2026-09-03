package controller

import (
	"context"
	"fmt"
	"slices"

	finv1 "github.com/cybozu-go/fin/api/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// nodeGVK identifies a Node in owner references and in the metadata lookup below, so the
// two cannot drift apart. Reading it out of a scheme would only trade the literal for a
// call that cannot fail, since core/v1 fixes these.
var nodeGVK = corev1.SchemeGroupVersion.WithKind("Node")

// findNodeOwnerReference reports whether a Node owner reference is present, and returns a
// copy of it. Kubernetes offers no helper: HasOwnerReference matches by name and ignores
// the UID, and IsControlledBy reads only a controller reference, which a Node must not be.
func findNodeOwnerReference(ownerRefs []metav1.OwnerReference) (metav1.OwnerReference, bool) {
	i := slices.IndexFunc(ownerRefs, func(ref metav1.OwnerReference) bool {
		return ref.APIVersion == nodeGVK.GroupVersion().String() && ref.Kind == nodeGVK.Kind
	})
	if i < 0 {
		return metav1.OwnerReference{}, false
	}
	return ownerRefs[i], true
}

// lookupNode reports whether the node exists and returns its UID. It reads
// PartialObjectMetadata so a cached reader serves it from the metadata informer; mixing
// that with corev1.Node would run two informers over the same nodes.
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

// nodeHoldsBackupData reports whether the node identified by uid still holds this
// FinBackup's data. A node recreated under the same name has a different UID, which the
// owner reference records, and holds none of it.
func nodeHoldsBackupData(backup *finv1.FinBackup, uid types.UID) bool {
	ref, found := findNodeOwnerReference(backup.GetOwnerReferences())
	return !found || ref.UID == uid
}
