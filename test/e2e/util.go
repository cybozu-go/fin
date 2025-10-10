package e2e

import (
	"bytes"
	"context"
	"fmt"
	"html/template"
	"os"
	"os/exec"
	"time"

	finv1 "github.com/cybozu-go/fin/api/v1"
	g "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"
)

const (
	rookNamespace = "rook-ceph"
	pvcNamespace  = "test-ns"
	pvcName       = "test-pvc"
	poolName      = "rook-ceph-block-pool"
)

var (
	minikube = "minikube"
)

func init() {
	if m := os.Getenv("MINIKUBE"); m != "" {
		minikube = os.Getenv("MINIKUBE")
	}
}

//nolint:unparam
func execWrapper(cmd string, input []byte, args ...string) ([]byte, []byte, error) {
	var stdout, stderr bytes.Buffer
	command := exec.Command(cmd, args...)
	command.Stdout = &stdout
	command.Stderr = &stderr

	if len(input) != 0 {
		command.Stdin = bytes.NewReader(input)
	}

	err := command.Run()
	return stdout.Bytes(), stderr.Bytes(), err
}

func kubectl(args ...string) ([]byte, []byte, error) {
	return execWrapper("kubectl", nil, args...)
}

func checkDeploymentReady(namespace, name string) error {
	_, stderr, err := kubectl("-n", namespace, "wait", "--for=condition=Available", "deploy", name, "--timeout=1m")
	if err != nil {
		return fmt.Errorf("kubectl wait deploy failed. stderr: %s, err: %w", string(stderr), err)
	}
	return nil
}

func waitEnvironment() {
	g.It("wait for fin-controller to be ready", func() {
		gomega.Eventually(func() error {
			return checkDeploymentReady(rookNamespace, "fin-controller-manager")
		}).Should(gomega.Succeed())
	})
}

func GetPVC(namespace, name, volumeMode, storageClassName, accessModes, size string) (*corev1.PersistentVolumeClaim, error) {
	tmpl := `apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: {{.Name}}
  namespace: {{.Namespace}}
spec:
  volumeMode: {{.VolumeMode}}
  storageClassName: {{.StorageClassName}}
  accessModes:
    - {{.AccessModes}}
  resources:
    requests:
      storage: {{.Size}}`

	t := template.Must(template.New("pvc").Parse(tmpl))
	var buf bytes.Buffer
	err := t.Execute(&buf, struct {
		Name             string
		Namespace        string
		VolumeMode       string
		StorageClassName string
		AccessModes      string
		Size             string
	}{
		Name:             name,
		Namespace:        namespace,
		VolumeMode:       volumeMode,
		StorageClassName: storageClassName,
		AccessModes:      accessModes,
		Size:             size,
	})
	if err != nil {
		return nil, err
	}

	var pvc corev1.PersistentVolumeClaim
	err = yaml.Unmarshal(buf.Bytes(), &pvc)
	if err != nil {
		return nil, err
	}
	return &pvc, nil
}

func CreatePVC(ctx context.Context, client kubernetes.Interface, pvc *corev1.PersistentVolumeClaim) error {
	_, err := client.CoreV1().PersistentVolumeClaims(pvc.GetNamespace()).Create(ctx, pvc, metav1.CreateOptions{})
	return err
}

func DeletePVC(ctx context.Context, client kubernetes.Interface, namespace, name string) error {
	policy := metav1.DeletePropagationForeground
	return client.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, name, metav1.DeleteOptions{PropagationPolicy: &policy})
}

func GetPodMountingFilesystem(namespace, name, pvcName, image, mountPath string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:    name,
					Image:   image,
					Command: []string{"pause"},
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      "data",
							MountPath: mountPath,
						},
					},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: "data",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: pvcName,
						},
					},
				},
			},
		},
	}
}

func GetPod(namespace, name, pvcName, image, devicePath string) (*corev1.Pod, error) {
	tmpl := `apiVersion: v1
kind: Pod
metadata:
  name: {{.Name}}
  namespace: {{.Namespace}}
spec:
  volumes:
    - name: {{.PVCName}}
      persistentVolumeClaim:
        claimName: {{.PVCName}}
  containers:
    - name: {{.Name}}
      image: {{.Image}}
      command: ["pause"]
      volumeDevices:
        - devicePath: "{{.DevicePath}}"
          name: {{.PVCName}}`

	t := template.Must(template.New("pod").Parse(tmpl))
	var buf bytes.Buffer
	err := t.Execute(&buf, struct {
		Name       string
		Namespace  string
		PVCName    string
		Image      string
		DevicePath string
	}{
		Name:       name,
		Namespace:  namespace,
		PVCName:    pvcName,
		Image:      image,
		DevicePath: devicePath,
	})
	if err != nil {
		return nil, err
	}

	var pod corev1.Pod
	err = yaml.Unmarshal(buf.Bytes(), &pod)
	if err != nil {
		return nil, err
	}
	return &pod, nil
}

func CreatePod(ctx context.Context, client kubernetes.Interface, pod *corev1.Pod) error {
	_, err := client.CoreV1().Pods(pod.GetNamespace()).Create(ctx, pod, metav1.CreateOptions{})
	return err
}

func DeletePod(ctx context.Context, client kubernetes.Interface, namespace, name string) error {
	policy := metav1.DeletePropagationForeground
	return client.CoreV1().Pods(namespace).Delete(ctx, name, metav1.DeleteOptions{PropagationPolicy: &policy})
}

func GetNamespace(name string) *corev1.Namespace {
	return &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
}

func CreateNamespace(ctx context.Context, client kubernetes.Interface, namespace *corev1.Namespace) error {
	_, err := client.CoreV1().Namespaces().Create(ctx, namespace, metav1.CreateOptions{})
	return err
}

func DeleteNamespace(ctx context.Context, client kubernetes.Interface, namespace string) error {
	policy := metav1.DeletePropagationForeground
	err := client.CoreV1().Namespaces().Delete(ctx, namespace, metav1.DeleteOptions{PropagationPolicy: &policy})
	if err != nil {
		return err
	}

	return wait.PollUntilContextTimeout(ctx, 5*time.Second, 3*time.Minute, true, func(ctx context.Context) (bool, error) {
		_, err := client.CoreV1().Namespaces().Get(ctx, namespace, metav1.GetOptions{})
		if errors.IsNotFound(err) {
			return true, nil
		}
		if err != nil {
			return false, err
		}
		return false, nil
	})
}

func WaitForPodReady(ctx context.Context, client kubernetes.Interface, namespace, name string, timeout time.Duration) error {
	return wait.PollUntilContextTimeout(ctx, time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		pod, err := client.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		for _, condition := range pod.Status.Conditions {
			if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
				return true, nil
			}
		}
		return false, nil
	})
}

func GetFinBackup(namespace, name, pvcNamespace, pvcName, node string) (*finv1.FinBackup, error) {
	tmpl := `apiVersion: fin.cybozu.io/v1
kind: FinBackup
metadata:
  name: {{.Name}}
  namespace: {{.Namespace}}
spec:
  pvc: {{.PVCName}}
  pvcNamespace: {{.PVCNamespace}}
  node: {{.Node}}`

	t := template.Must(template.New("finbackup").Parse(tmpl))
	var buf bytes.Buffer
	err := t.Execute(&buf, struct {
		Name         string
		Namespace    string
		PVCName      string
		PVCNamespace string
		Node         string
	}{
		Name:         name,
		Namespace:    namespace,
		PVCName:      pvcName,
		PVCNamespace: pvcNamespace,
		Node:         node,
	})
	if err != nil {
		return nil, err
	}

	var finbackup finv1.FinBackup
	err = yaml.Unmarshal(buf.Bytes(), &finbackup)
	if err != nil {
		return nil, err
	}
	return &finbackup, nil
}

func CreateFinBackup(ctx context.Context, client client.Client, finbackup *finv1.FinBackup) error {
	return client.Create(ctx, finbackup)
}

func GetFinRestore(namespace, name, backupName, pvcName, pvcNamespace string) (*finv1.FinRestore, error) {
	tmpl := `apiVersion: fin.cybozu.io/v1
kind: FinRestore
metadata:
  name: {{.Name}}
  namespace: {{.Namespace}}
spec:
  backup: {{.BackupName}}
  pvc: {{.PVCName}}
  pvcNamespace: {{.PVCNamespace}}`

	t := template.Must(template.New("finrestore").Parse(tmpl))
	var buf bytes.Buffer
	err := t.Execute(&buf, struct {
		Name         string
		Namespace    string
		BackupName   string
		PVCName      string
		PVCNamespace string
	}{
		Name:         name,
		Namespace:    namespace,
		BackupName:   backupName,
		PVCName:      pvcName,
		PVCNamespace: pvcNamespace,
	})
	if err != nil {
		return nil, err
	}

	var finrestore finv1.FinRestore
	err = yaml.Unmarshal(buf.Bytes(), &finrestore)
	if err != nil {
		return nil, err
	}
	return &finrestore, nil
}

func CreateFinRestore(ctx context.Context, client client.Client, finrestore *finv1.FinRestore) error {
	return client.Create(ctx, finrestore)
}

func DeleteFinBackup(ctx context.Context, client client.Client, namespace, name string) error {
	finbackup := &finv1.FinBackup{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}}
	return client.Delete(ctx, finbackup)
}

func DeleteFinRestore(ctx context.Context, client client.Client, namespace, name string) error {
	finrestore := &finv1.FinRestore{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}}
	return client.Delete(ctx, finrestore)
}

func WaitForFinBackupStoredToNodeAndVerified(ctx context.Context, client client.Client, namespace, name string, timeout time.Duration) error {
	return wait.PollUntilContextTimeout(ctx, time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		finbackup := &finv1.FinBackup{}
		err := client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, finbackup)
		if err != nil {
			return false, err
		}

		return finbackup.IsStoredToNode() && finbackup.IsVerifiedTrue(), nil
	})
}

func WaitForFinRestoreReady(ctx context.Context, client client.Client, namespace, name string, timeout time.Duration) error {
	return wait.PollUntilContextTimeout(ctx, time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		finrestore := &finv1.FinRestore{}
		err := client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, finrestore)
		if err != nil {
			return false, err
		}
		return finrestore.IsReady(), nil
	})
}

// WaitForDeletion waits for any client.Object to be deleted using controller-runtime client (for custom resources)
func WaitForDeletion(ctx context.Context, ctrlClient client.Client, obj client.Object, namespace, name string, timeout time.Duration) error {
	return wait.PollUntilContextTimeout(ctx, 2*time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		err := ctrlClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, obj)
		if err != nil {
			if errors.IsNotFound(err) {
				return true, nil
			}
			return false, err
		}
		return false, nil
	})
}

// WaitForCoreDeletion waits for core Kubernetes resources to be deleted using kubernetes.Interface
func WaitForCoreDeletion(ctx context.Context, timeout time.Duration, getFunc func(ctx context.Context) error) error {
	return wait.PollUntilContextTimeout(ctx, 2*time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		err := getFunc(ctx)
		if err != nil {
			if errors.IsNotFound(err) {
				return true, nil
			}
			return false, err
		}
		return false, nil
	})
}

func WaitForFinBackupDeletion(ctx context.Context, ctrlClient client.Client, namespace, name string, timeout time.Duration) error {
	return WaitForDeletion(ctx, ctrlClient, &finv1.FinBackup{}, namespace, name, timeout)
}

func WaitForFinRestoreDeletion(ctx context.Context, ctrlClient client.Client, namespace, name string, timeout time.Duration) error {
	return WaitForDeletion(ctx, ctrlClient, &finv1.FinRestore{}, namespace, name, timeout)
}

// WaitForJobDeletion waits for a Job to be deleted using kubernetes.Interface
func WaitForJobDeletion(ctx context.Context, k8sClient kubernetes.Interface, namespace, name string, timeout time.Duration) error {
	return WaitForCoreDeletion(ctx, timeout, func(ctx context.Context) error {
		_, err := k8sClient.BatchV1().Jobs(namespace).Get(ctx, name, metav1.GetOptions{})
		return err
	})
}

// WaitForPodDeletion waits for a Pod to be deleted using kubernetes.Interface
func WaitForPodDeletion(ctx context.Context, k8sClient kubernetes.Interface, namespace, name string, timeout time.Duration) error {
	return WaitForCoreDeletion(ctx, timeout, func(ctx context.Context) error {
		_, err := k8sClient.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
		return err
	})
}

// WaitForPVCDeletion waits for a PVC to be deleted using kubernetes.Interface
func WaitForPVCDeletion(ctx context.Context, k8sClient kubernetes.Interface, namespace, name string, timeout time.Duration) error {
	return WaitForCoreDeletion(ctx, timeout, func(ctx context.Context) error {
		_, err := k8sClient.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, name, metav1.GetOptions{})
		return err
	})
}
