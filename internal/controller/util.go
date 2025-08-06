package controller

import (
	"context"
	"fmt"

	finv1 "github.com/cybozu-go/fin/api/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func jobCompleted(job *batchv1.Job) (done bool, err error) {
	for _, c := range job.Status.Conditions {
		switch c.Type {
		case batchv1.JobComplete:
			if c.Status == corev1.ConditionTrue {
				return true, nil
			}
		case batchv1.JobFailed:
			if c.Status == corev1.ConditionTrue {
				return false, fmt.Errorf("job %s/%s failed: %s", job.Namespace, job.Name, c.Message)
			}
		}
	}
	return false, nil
}

func enqueueOnJobEvent(resourceName, resourceNamespace string) handler.MapFunc {
	return func(ctx context.Context, obj client.Object) []reconcile.Request {
		job, ok := obj.(*batchv1.Job)
		if !ok {
			return []reconcile.Request{}
		}
		if name, exist := job.GetAnnotations()[resourceName]; exist {
			if namespace, exist := job.GetAnnotations()[resourceNamespace]; exist {
				return []reconcile.Request{
					{
						NamespacedName: types.NamespacedName{
							Name:      name,
							Namespace: namespace,
						},
					},
				}
			}
		}
		return []reconcile.Request{}
	}
}

func enqueueOnJobCompletion(e event.UpdateEvent) bool {
	newJob, ok := e.ObjectNew.(*batchv1.Job)
	if !ok {
		return false
	}
	newCompleted, _ := jobCompleted(newJob)
	if !newCompleted {
		return false
	}

	oldJob, ok := e.ObjectOld.(*batchv1.Job)
	if !ok {
		return false
	}
	oldCompleted, _ := jobCompleted(oldJob)
	return !oldCompleted
}

func finVolumePVCName(backup *finv1.FinBackup) string {
	return "fin-" + backup.Spec.Node
}
