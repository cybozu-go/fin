package controller

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	finv1 "github.com/cybozu-go/fin/api/v1"
	"github.com/cybozu-go/fin/internal/model"
	"github.com/cybozu-go/fin/internal/pkg/metrics"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

const (
	finBackupConfigFinalizerName = "finbackupconfig.fin.cybozu.io/finalizer"

	// unassignedRequeueInterval retries the node selection of a FinBackupConfig without a
	// node. No event tells that a node got room or that a new node started reporting its
	// free space, which may happen only after the Node creation event.
	unassignedRequeueInterval = time.Minute
)

// errNoNodeAvailable means no node can take the FinBackupConfig. It is not a reconcile
// error: status.node is cleared so that creating FinBackups fails until a node is added.
var errNoNodeAvailable = errors.New("no node is available for FinBackupConfig")

// FinBackupConfigReconciler reconciles a FinBackupConfig object
type FinBackupConfigReconciler struct {
	client.Client
	Scheme               *runtime.Scheme
	managedCephClusterID string
	overwriteFBCSchedule string
	podImage             string
	serviceAccountName   string
	nodeSelector         *nodeaffinity.NodeSelector
	nodeFreeSpaceRepo    model.NodeFreeSpaceRepository
	nodeFreeSpaceMargin  resource.Quantity

	// assignments holds the status.node this reconciler has written but the cache may
	// not show yet, so FinBackupConfigs created together do not all land on one node.
	assignmentsMu sync.Mutex
	assignments   map[types.NamespacedName]string
}

func NewFinBackupConfigReconciler(
	client client.Client,
	scheme *runtime.Scheme,
	overwriteFBCSchedule string,
	managedCephClusterID string,
	podImage string,
	serviceAccountName string,
	nodeSelector *nodeaffinity.NodeSelector,
	nodeFreeSpaceRepo model.NodeFreeSpaceRepository,
	nodeFreeSpaceMargin resource.Quantity,
) *FinBackupConfigReconciler {
	return &FinBackupConfigReconciler{
		Client:               client,
		Scheme:               scheme,
		managedCephClusterID: managedCephClusterID,
		overwriteFBCSchedule: overwriteFBCSchedule,
		podImage:             podImage,
		serviceAccountName:   serviceAccountName,
		nodeSelector:         nodeSelector,
		nodeFreeSpaceRepo:    nodeFreeSpaceRepo,
		nodeFreeSpaceMargin:  nodeFreeSpaceMargin,
		assignments:          make(map[types.NamespacedName]string),
	}
}

//+kubebuilder:rbac:groups=fin.cybozu.io,resources=finbackupconfigs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=fin.cybozu.io,resources=finbackupconfigs/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=fin.cybozu.io,resources=finbackupconfigs/finalizers,verbs=update
//+kubebuilder:rbac:groups=batch,resources=cronjobs,verbs=get;list;watch;create;update;patch;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the FinBackupConfig object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.16.3/pkg/reconcile
func (r *FinBackupConfigReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var fbc finv1.FinBackupConfig
	if err := r.Get(ctx, req.NamespacedName, &fbc); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !fbc.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&fbc, finBackupConfigFinalizerName) {
			controllerutil.RemoveFinalizer(&fbc, finBackupConfigFinalizerName)
			if err := r.Update(ctx, &fbc); err != nil {
				return ctrl.Result{}, fmt.Errorf("failed to remove finalizer from FinBackupConfig: %w", err)
			}
		}
		metrics.DeleteFinBackupConfigInfo(&fbc, r.managedCephClusterID)
		return ctrl.Result{}, nil
	}

	var pvc corev1.PersistentVolumeClaim
	if err := r.Get(ctx, types.NamespacedName{Namespace: fbc.Spec.PVCNamespace, Name: fbc.Spec.PVC}, &pvc); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to get PVC %s/%s: %w", fbc.Spec.PVCNamespace, fbc.Spec.PVC, err)
	}

	ok, err := checkCephCluster(ctx, r.Client, &pvc, r.managedCephClusterID)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to check Ceph cluster: %w", err)
	}
	if !ok {
		logger.Info("the target pvc is not managed by this controller")
		return ctrl.Result{}, nil
	}

	if !controllerutil.ContainsFinalizer(&fbc, finBackupConfigFinalizerName) {
		controllerutil.AddFinalizer(&fbc, finBackupConfigFinalizerName)
		if err := r.Update(ctx, &fbc); err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to add finalizer to FinBackupConfig: %w", err)
		}
	}

	// The metric is set even when no node can be selected yet, reporting an empty node.
	err = r.maintainStatusNode(ctx, &fbc, &pvc)
	metrics.SetFinBackupConfigInfo(&fbc, r.managedCephClusterID)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to maintain status node: %w", err)
	}

	image := r.podImage
	serviceAccountName := r.serviceAccountName
	if err := r.createOrUpdateCronJob(ctx, &fbc, fbc.Namespace, serviceAccountName, image); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to create or update CronJob: %w", err)
	}
	if fbc.Status.Node == "" {
		return ctrl.Result{RequeueAfter: unassignedRequeueInterval}, nil
	}
	return ctrl.Result{}, nil
}

// enqueueFinBackupConfigsOnNode maps a Node creation, deletion or label change to the FinBackupConfigs
// whose status.node may change: those pinned to the node, and those still without a node.
// A FinBackupConfig with spec.node always follows it, so a Node event cannot affect it.
func (r *FinBackupConfigReconciler) enqueueFinBackupConfigsOnNode(ctx context.Context, node client.Object) []ctrl.Request {
	var requests []ctrl.Request
	for _, statusNode := range []string{node.GetName(), ""} {
		var fbcs finv1.FinBackupConfigList
		if err := r.List(ctx, &fbcs, client.MatchingFields{indexFinBackupConfigStatusNode: statusNode}); err != nil {
			log.FromContext(ctx).Error(err, "failed to list FinBackupConfigs for a node event", "node", node.GetName())
			return nil
		}
		for i := range fbcs.Items {
			if fbcs.Items[i].Spec.Node != "" {
				continue
			}
			requests = append(requests, ctrl.Request{
				NamespacedName: client.ObjectKeyFromObject(&fbcs.Items[i]),
			})
		}
	}
	return requests
}

// SetupWithManager sets up the controller with the Manager.
func (r *FinBackupConfigReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &finv1.FinBackupConfig{},
		indexFinBackupConfigStatusNode,
		func(o client.Object) []string {
			fbc, ok := o.(*finv1.FinBackupConfig)
			if !ok {
				return nil
			}
			return []string{fbc.Status.Node}
		}); err != nil {
		return fmt.Errorf("failed to index FinBackupConfig by %s: %w", indexFinBackupConfigStatusNode, err)
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&finv1.FinBackupConfig{}).
		Owns(&batchv1.CronJob{}).
		// OnlyMetadata shares the metadata informer with the FinBackup controller. The node
		// selection looks at labels but not at conditions, so an update matters only when
		// it changes the labels.
		Watches(&corev1.Node{},
			handler.EnqueueRequestsFromMapFunc(r.enqueueFinBackupConfigsOnNode),
			builder.OnlyMetadata,
			builder.WithPredicates(predicate.Funcs{
				UpdateFunc:  predicate.LabelChangedPredicate{}.Update,
				GenericFunc: func(event.GenericEvent) bool { return false },
			})).
		Complete(r)
}

// maintainStatusNode copies spec.node to status.node when set; spec.node is managed by
// humans and never changed here. Otherwise it keeps status.node while the node exists
// and matches the node selector, and selects a node again when it does not, e.g. when
// the node is being retired and its label is removed. When no node is available, it
// clears status.node, so that the FinBackup creation fails and lets administrators
// notice that a node for fin is needed.
func (r *FinBackupConfigReconciler) maintainStatusNode(
	ctx context.Context,
	fbc *finv1.FinBackupConfig,
	pvc *corev1.PersistentVolumeClaim,
) error {
	specNode := fbc.Spec.Node
	statusNode := fbc.Status.Node
	var updateStatusNode string

	if len(specNode) == 0 {
		if len(statusNode) != 0 {
			selectable, err := r.isSelectableNode(ctx, statusNode)
			if err != nil {
				return err
			}
			if selectable {
				return nil
			}
		}
		var err error
		updateStatusNode, err = r.selectNode(ctx, pvc)
		if errors.Is(err, errNoNodeAvailable) {
			log.FromContext(ctx).Info("clearing status.node since no node is available",
				"reason", err.Error(), "previousNode", statusNode)
			updateStatusNode = ""
		} else if err != nil {
			return err
		}
	} else {
		updateStatusNode = specNode
	}

	// update status of FBC
	if updateStatusNode != statusNode {
		fbc.Status.Node = updateStatusNode
		if err := r.Status().Update(ctx, fbc); err != nil {
			// Keep fbc as stored, since the caller reports status.node in a metric.
			fbc.Status.Node = statusNode
			return err
		}
		r.assignmentsMu.Lock()
		r.assignments[client.ObjectKeyFromObject(fbc)] = updateStatusNode
		r.assignmentsMu.Unlock()
	}

	return nil
}

// isSelectableNode reports whether the node exists and matches the node selector.
func (r *FinBackupConfigReconciler) isSelectableNode(ctx context.Context, nodeName string) (bool, error) {
	var node metav1.PartialObjectMetadata
	node.SetGroupVersionKind(nodeGVK)
	if err := r.Get(ctx, client.ObjectKey{Name: nodeName}, &node); err != nil {
		if k8serrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("failed to get node %q: %w", nodeName, err)
	}
	return r.nodeSelector.Match(&corev1.Node{ObjectMeta: node.ObjectMeta}), nil
}

type nodeCandidate struct {
	name      string
	freeSpace float64
}

// selectNode picks, among the quarter of the matching nodes with the most free space,
// the one with the fewest FinBackupConfigs. A node whose free space is unknown is
// skipped, since a node that has just joined may not report it yet, and so is a node in
// the quarter without room for the PVC plus the margin. The count rather than the size
// is balanced, because the backups of the volumes grow to similar sizes over time
// whatever their initial sizes are.
func (r *FinBackupConfigReconciler) selectNode(
	ctx context.Context,
	pvc *corev1.PersistentVolumeClaim,
) (string, error) {
	logger := log.FromContext(ctx)

	var nodes metav1.PartialObjectMetadataList
	nodes.SetGroupVersionKind(nodeListGVK)
	if err := r.List(ctx, &nodes); err != nil {
		return "", fmt.Errorf("failed to list nodes: %w", err)
	}

	freeSpace, err := r.nodeFreeSpaceRepo.GetNodeFreeSpace(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to get the free space of nodes: %w", err)
	}

	candidates := make([]nodeCandidate, 0, len(nodes.Items))
	for i := range nodes.Items {
		node := &nodes.Items[i]
		if !r.nodeSelector.Match(&corev1.Node{ObjectMeta: node.ObjectMeta}) {
			continue
		}
		free, ok := freeSpace[node.Name]
		if !ok {
			logger.Info("skipping a node whose free space is unknown", "node", node.Name)
			continue
		}
		candidates = append(candidates, nodeCandidate{name: node.Name, freeSpace: free})
	}
	if len(candidates) == 0 {
		return "", fmt.Errorf("%w: no node matching the selector reports its free space", errNoNodeAvailable)
	}

	size, err := pvcSize(pvc)
	if err != nil {
		return "", err
	}
	required := size.DeepCopy()
	required.Add(r.nodeFreeSpaceMargin)
	requiredBytes := required.AsApproximateFloat64()

	var fitting []nodeCandidate
	for _, c := range topByFreeSpace(candidates) {
		if c.freeSpace < requiredBytes {
			logger.Info("skipping a node without enough free space",
				"node", c.name, "freeSpace", c.freeSpace, "required", required.String())
			continue
		}
		fitting = append(fitting, c)
	}
	if len(fitting) == 0 {
		return "", fmt.Errorf("%w: no node in the top quarter has %s of free space", errNoNodeAvailable, required.String())
	}

	counts, err := r.countFinBackupConfigsPerNode(ctx)
	if err != nil {
		return "", err
	}
	return fewestFinBackupConfigs(fitting, counts), nil
}

// pvcSize prefers the capacity actually provisioned, which can exceed the request or
// grow by expansion, and falls back to the request before the PVC is bound.
func pvcSize(pvc *corev1.PersistentVolumeClaim) (resource.Quantity, error) {
	if size, ok := pvc.Status.Capacity[corev1.ResourceStorage]; ok {
		return size, nil
	}
	if size, ok := pvc.Spec.Resources.Requests[corev1.ResourceStorage]; ok {
		return size, nil
	}
	return resource.Quantity{}, fmt.Errorf("PVC %s/%s has neither a storage capacity nor a request",
		pvc.Namespace, pvc.Name)
}

// topByFreeSpace sorts candidates by free space and returns the top quarter.
func topByFreeSpace(candidates []nodeCandidate) []nodeCandidate {
	slices.SortFunc(candidates, func(a, b nodeCandidate) int {
		if c := cmp.Compare(b.freeSpace, a.freeSpace); c != 0 {
			return c
		}
		return cmp.Compare(a.name, b.name)
	})
	return candidates[:(len(candidates)+3)/4]
}

// fewestFinBackupConfigs returns the candidate with the fewest FinBackupConfigs.
// Ties go to the earlier candidate.
func fewestFinBackupConfigs(candidates []nodeCandidate, counts map[string]int) string {
	selected := candidates[0]
	for _, c := range candidates[1:] {
		if counts[c.name] < counts[selected.name] {
			selected = c
		}
	}
	return selected.name
}

// countFinBackupConfigsPerNode counts the FinBackupConfigs managed by this controller
// on each node. The recorded assignments take precedence over the cache, and are
// dropped once the cache catches up or the FinBackupConfig is gone.
func (r *FinBackupConfigReconciler) countFinBackupConfigsPerNode(ctx context.Context) (map[string]int, error) {
	var fbcs finv1.FinBackupConfigList
	if err := r.List(ctx, &fbcs); err != nil {
		return nil, fmt.Errorf("failed to list FinBackupConfigs: %w", err)
	}

	r.assignmentsMu.Lock()
	defer r.assignmentsMu.Unlock()

	counts := make(map[string]int)
	listed := make(map[types.NamespacedName]struct{}, len(fbcs.Items))
	for i := range fbcs.Items {
		fbc := &fbcs.Items[i]
		key := client.ObjectKeyFromObject(fbc)
		listed[key] = struct{}{}
		if !fbc.DeletionTimestamp.IsZero() {
			continue
		}

		node, assigned := r.assignments[key]
		if assigned {
			if node == fbc.Status.Node {
				delete(r.assignments, key)
			}
		} else {
			// Only FinBackupConfigs whose PVC is in the managed Ceph cluster get the
			// finalizer, so this skips those of other clusters without reading their PVCs.
			if !controllerutil.ContainsFinalizer(fbc, finBackupConfigFinalizerName) {
				continue
			}
			node = fbc.Status.Node
		}
		if node != "" {
			counts[node]++
		}
	}

	for key := range r.assignments {
		if _, ok := listed[key]; !ok {
			delete(r.assignments, key)
		}
	}
	return counts, nil
}

func (r *FinBackupConfigReconciler) createOrUpdateCronJob(
	ctx context.Context,
	fbc *finv1.FinBackupConfig,
	namespace string,
	serviceAccountName string,
	image string,
) error {
	cronJobName := "fbc-" + string(fbc.UID)

	cronJob := &batchv1.CronJob{}
	cronJob.SetName(cronJobName)
	cronJob.SetNamespace(namespace)

	_, err := ctrl.CreateOrUpdate(ctx, r.Client, cronJob, func() error {
		cronJob.Spec.Schedule = fbc.Spec.Schedule
		if r.overwriteFBCSchedule != "" {
			cronJob.Spec.Schedule = r.overwriteFBCSchedule
		}
		cronJob.Spec.Suspend = &fbc.Spec.Suspend
		var startingDeadlineSeconds int64 = 3600
		cronJob.Spec.StartingDeadlineSeconds = &startingDeadlineSeconds
		cronJob.Spec.ConcurrencyPolicy = batchv1.ForbidConcurrent
		var backoffLimit int32 = 65535
		cronJob.Spec.JobTemplate.Spec.BackoffLimit = &backoffLimit

		podSpec := &cronJob.Spec.JobTemplate.Spec.Template.Spec
		podSpec.ServiceAccountName = serviceAccountName
		podSpec.RestartPolicy = corev1.RestartPolicyOnFailure

		if len(podSpec.Containers) == 0 {
			podSpec.Containers = make([]corev1.Container, 1)
		}
		container := &podSpec.Containers[0]
		container.Name = "create-finbackup-job"
		container.Image = image
		container.ImagePullPolicy = corev1.PullIfNotPresent
		container.Command = []string{
			"/manager",
			"create-finbackup-job",
			"--fin-backup-config-name=" + fbc.GetName(),
			"--fin-backup-config-namespace=" + fbc.GetNamespace(),
		}

		container.Env = []corev1.EnvVar{
			{
				Name: "JOB_NAME",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "metadata.labels['batch.kubernetes.io/job-name']",
					},
				},
			},
			{
				Name: "POD_NAMESPACE",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "metadata.namespace",
					},
				},
			},
		}

		if err := controllerutil.SetControllerReference(fbc, cronJob, r.Scheme); err != nil {
			return fmt.Errorf("failed to set owner reference on CronJob: %w", err)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to create CronJob: %s: %w", cronJobName, err)
	}
	return nil
}
