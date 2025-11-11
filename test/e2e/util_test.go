package e2e

import (
	"bytes"
	"context"
	"fmt"
	"html/template"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	finv1 "github.com/cybozu-go/fin/api/v1"
	"github.com/cybozu-go/fin/test/utils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"
)

const (
	rookNamespace          = "rook-ceph"
	rookStorageClass       = "rook-ceph-block"
	poolName               = "rook-ceph-block-pool"
	devicePathInPodForPVC  = "/data"
	mountPathInPodForFSPVC = "/data"
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

//nolint:unparam
func minikubeSSH(node string, input []byte, args ...string) ([]byte, []byte, error) {
	args = append([]string{"--profile", minikubeProfile,
		"ssh", "--native-ssh=false", "--node", node, "--"}, args...)
	return execWrapper(minikube, input, args...)
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
	It("wait for fin-controller to be ready", func() {
		Eventually(func() error {
			return checkDeploymentReady(rookNamespace, "fin-controller-manager")
		}).Should(Succeed())
	})
}

func NewPVC(namespace, name, volumeMode, storageClassName, accessModes, size string) (*corev1.PersistentVolumeClaim, error) {
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

func DeletePVC(ctx context.Context, client kubernetes.Interface, pvc *corev1.PersistentVolumeClaim) error {
	policy := metav1.DeletePropagationForeground
	return client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Delete(ctx, pvc.Name, metav1.DeleteOptions{PropagationPolicy: &policy})
}

func DeleteRestorePVC(ctx context.Context, client kubernetes.Interface, finrestore *finv1.FinRestore) error {
	policy := metav1.DeletePropagationForeground
	return client.CoreV1().PersistentVolumeClaims(finrestore.Spec.PVCNamespace).Delete(
		ctx, finrestore.Spec.PVC, metav1.DeleteOptions{PropagationPolicy: &policy})
}

func NewPodMountingFilesystem(namespace, name, pvcName, image, mountPath string) *corev1.Pod {
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

func NewPod(namespace, name, pvcName, image, devicePath string) (*corev1.Pod, error) {
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
	_, err := client.CoreV1().Pods(pod.Namespace).Create(ctx, pod, metav1.CreateOptions{})
	return err
}

func DeletePod(ctx context.Context, client kubernetes.Interface, pod *corev1.Pod) error {
	policy := metav1.DeletePropagationForeground
	return client.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{PropagationPolicy: &policy})
}

func NewNamespace(name string) *corev1.Namespace {
	return &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
}

func CreateNamespace(ctx context.Context, client kubernetes.Interface, namespace *corev1.Namespace) error {
	_, err := client.CoreV1().Namespaces().Create(ctx, namespace, metav1.CreateOptions{})
	return err
}

func DeleteNamespace(ctx context.Context, client kubernetes.Interface, namespace *corev1.Namespace) error {
	policy := metav1.DeletePropagationForeground
	err := client.CoreV1().Namespaces().Delete(ctx, namespace.Name, metav1.DeleteOptions{PropagationPolicy: &policy})
	if err != nil {
		return err
	}

	return wait.PollUntilContextTimeout(ctx, 5*time.Second, 3*time.Minute, true, func(ctx context.Context) (bool, error) {
		_, err := client.CoreV1().Namespaces().Get(ctx, namespace.Name, metav1.GetOptions{})
		if errors.IsNotFound(err) {
			return true, nil
		}
		if err != nil {
			return false, err
		}
		return false, nil
	})
}

func WaitForPodReady(ctx context.Context, client kubernetes.Interface, pod *corev1.Pod, timeout time.Duration) error {
	return wait.PollUntilContextTimeout(ctx, time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		p, err := client.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		for _, condition := range p.Status.Conditions {
			if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
				return true, nil
			}
		}
		return false, nil
	})
}

func NewFinBackup(namespace, name string, pvc *corev1.PersistentVolumeClaim, node string) (*finv1.FinBackup, error) {
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
		PVCName:      pvc.Name,
		PVCNamespace: pvc.Namespace,
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

func NewFinRestore(name string, backup *finv1.FinBackup, restorePVCNamespace, restorePVCName string) (*finv1.FinRestore, error) {
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
		Namespace:    backup.Namespace,
		BackupName:   backup.Name,
		PVCName:      restorePVCName,
		PVCNamespace: restorePVCNamespace,
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

func DeleteFinBackup(ctx context.Context, client client.Client, finbackup *finv1.FinBackup) error {
	target := &finv1.FinBackup{ObjectMeta: metav1.ObjectMeta{Name: finbackup.Name, Namespace: finbackup.Namespace}}
	return client.Delete(ctx, target)
}

func DeleteFinRestore(ctx context.Context, client client.Client, finrestore *finv1.FinRestore) error {
	target := &finv1.FinRestore{ObjectMeta: metav1.ObjectMeta{Name: finrestore.Name, Namespace: finrestore.Namespace}}
	return client.Delete(ctx, target)
}

func WaitForFinBackupStoredToNodeAndVerified(ctx context.Context, c client.Client, finbackup *finv1.FinBackup, timeout time.Duration) error {
	return wait.PollUntilContextTimeout(ctx, time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		fb := &finv1.FinBackup{}
		err := c.Get(ctx, client.ObjectKeyFromObject(finbackup), fb)
		if err != nil {
			return false, err
		}

		return fb.IsStoredToNode() && fb.IsVerifiedTrue(), nil
	})
}

func WaitForFinRestoreReady(ctx context.Context, c client.Client, finrestore *finv1.FinRestore, timeout time.Duration) error {
	return wait.PollUntilContextTimeout(ctx, time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		fr := &finv1.FinRestore{}
		err := c.Get(ctx, client.ObjectKeyFromObject(finrestore), fr)
		if err != nil {
			return false, err
		}
		return fr.IsReady(), nil
	})
}

// WaitForDeletion waits for any client.Object to be deleted using controller-runtime client (for custom resources)
func WaitForCustomResourceDeletion(ctx context.Context, ctrlClient client.Client, dummy, obj client.Object, timeout time.Duration) error {
	return wait.PollUntilContextTimeout(ctx, 2*time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		err := ctrlClient.Get(ctx, client.ObjectKeyFromObject(obj), dummy)
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

func WaitForFinBackupDeletion(ctx context.Context, ctrlClient client.Client, finbackup *finv1.FinBackup, timeout time.Duration) error {
	return WaitForCustomResourceDeletion(ctx, ctrlClient, &finv1.FinBackup{}, finbackup, timeout)
}

func WaitForFinRestoreDeletion(ctx context.Context, ctrlClient client.Client, finrestore *finv1.FinRestore, timeout time.Duration) error {
	return WaitForCustomResourceDeletion(ctx, ctrlClient, &finv1.FinRestore{}, finrestore, timeout)
}

// WaitForJobDeletion waits for a Job to be deleted using kubernetes.Interface
func WaitForJobDeletion(ctx context.Context, k8sClient kubernetes.Interface, namespace, name string, timeout time.Duration) error {
	return WaitForCoreDeletion(ctx, timeout, func(ctx context.Context) error {
		_, err := k8sClient.BatchV1().Jobs(namespace).Get(ctx, name, metav1.GetOptions{})
		return err
	})
}

// WaitForPodDeletion waits for a Pod to be deleted using kubernetes.Interface
func WaitForPodDeletion(ctx context.Context, k8sClient kubernetes.Interface, pod *corev1.Pod, timeout time.Duration) error {
	return WaitForCoreDeletion(ctx, timeout, func(ctx context.Context) error {
		_, err := k8sClient.CoreV1().Pods(pod.GetNamespace()).Get(ctx, pod.GetName(), metav1.GetOptions{})
		return err
	})
}

// WaitForPVCDeletion waits for a PVC to be deleted using kubernetes.Interface
func WaitForPVCDeletion(ctx context.Context, k8sClient kubernetes.Interface, pvc *corev1.PersistentVolumeClaim, timeout time.Duration) error {
	return WaitForCoreDeletion(ctx, timeout, func(ctx context.Context) error {
		_, err := k8sClient.CoreV1().PersistentVolumeClaims(pvc.GetNamespace()).Get(ctx, pvc.GetName(), metav1.GetOptions{})
		return err
	})
}

func VerifySizeOfRestorePVC(ctx context.Context, c client.Client, restore *finv1.FinRestore) {
	GinkgoHelper()

	By("verifying the size of the restore PVC")
	var ret []byte
	ret, stderr, err := kubectl("get", "pvc", "-n",
		restore.Spec.PVCNamespace,
		restore.Spec.PVC,
		"-o", "jsonpath={.spec.resources.requests.storage}")
	Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))

	var size resource.Quantity
	size, err = resource.ParseQuantity(string(ret))
	Expect(err).NotTo(HaveOccurred())
	sizeBytes, ok := size.AsInt64()
	Expect(ok).To(BeTrue())

	fb := &finv1.FinBackup{}
	err = c.Get(ctx, client.ObjectKey{Namespace: restore.Namespace, Name: restore.Spec.Backup}, fb)
	Expect(err).NotTo(HaveOccurred())
	Expect(sizeBytes).To(Equal(*fb.Status.SnapSize),
		"Size of restore PVC does not match the snapshot size")
}

func DeleteFinRestoreAndRestorePVC(
	ctx context.Context,
	c client.Client,
	k8sClient kubernetes.Interface,
	restore *finv1.FinRestore,
) error {
	_ = DeleteRestorePVC(ctx, k8sClient, restore)
	return DeleteFinRestore(ctx, c, restore)
}

// VerifyDataInRestorePVC verifies that the first `len(expected)`
// bytes of restore PVC matches `expected`.
func VerifyDataInRestorePVC(
	ctx context.Context,
	k8sClient kubernetes.Interface,
	restore *finv1.FinRestore,
	expected []byte,
) {
	GinkgoHelper()

	By("verifying the existence of the restore PVC")
	_, stderr, err := kubectl("wait", "pvc", "-n",
		restore.Spec.PVCNamespace, restore.Name,
		"--for=jsonpath={.status.phase}=Bound", "--timeout=2m")
	Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))

	By("creating a pod to verify the contents in the restore PVC")
	pod, err := NewPod(
		restore.Spec.PVCNamespace,
		utils.GetUniqueName("test-restore-pod-"),
		restore.Spec.PVC,
		"ghcr.io/cybozu/ubuntu:24.04",
		"/restore",
	)
	Expect(err).NotTo(HaveOccurred())
	err = CreatePod(ctx, k8sClient, pod)
	Expect(err).NotTo(HaveOccurred())
	err = WaitForPodReady(ctx, k8sClient, pod, 2*time.Minute)
	Expect(err).NotTo(HaveOccurred())

	By("verifying the data in the restore PVC")
	restoredData, stderr, err := kubectl("exec", "-n",
		restore.Spec.PVCNamespace, pod.Name, "--",
		"dd", "if=/restore", fmt.Sprintf("bs=%d", len(expected)), "count=1")
	Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))
	Expect(restoredData).To(Equal(expected),
		"Data in restore PVC does not match the expected data")

	Expect(DeletePod(context.Background(), k8sClient, pod)).NotTo(HaveOccurred())
}

func CreateBackupTargetPVC(
	ctx context.Context,
	k8sClient kubernetes.Interface,
	namespace *corev1.Namespace,
	volumeMode, storageClassName, accessModes, size string,
) *corev1.PersistentVolumeClaim {
	GinkgoHelper()

	By("creating a backup target PVC")
	pvc, err := NewPVC(namespace.Name, utils.GetUniqueName("test-pvc-"),
		volumeMode, storageClassName, accessModes, size)
	Expect(err).NotTo(HaveOccurred())
	Expect(CreatePVC(ctx, k8sClient, pvc)).NotTo(HaveOccurred())

	return pvc
}

func CreatePodForBlockPVC(
	ctx context.Context,
	k8sClient kubernetes.Interface,
	pvc *corev1.PersistentVolumeClaim,
) *corev1.Pod {
	GinkgoHelper()

	By("creating a pod for block PVC")
	pod, err := NewPod(
		pvc.Namespace,
		utils.GetUniqueName("test-pod-for-block-pvc-"),
		pvc.Name,
		"ghcr.io/cybozu/ubuntu:24.04",
		"/data",
	)
	Expect(err).NotTo(HaveOccurred())
	Expect(CreatePod(ctx, k8sClient, pod)).NotTo(HaveOccurred())
	Expect(WaitForPodReady(ctx, k8sClient, pod, 2*time.Minute)).NotTo(HaveOccurred())
	return pod
}

func CreatePodForFilesystemPVC(
	ctx context.Context,
	k8sClient kubernetes.Interface,
	pvc *corev1.PersistentVolumeClaim,
) *corev1.Pod {
	GinkgoHelper()

	By("creating a pod for filesystem PVC")
	pod := NewPodMountingFilesystem(pvc.Namespace, utils.GetUniqueName("test-pod-"),
		pvc.Name, "ghcr.io/cybozu/ubuntu:24.04", "/data")
	err := CreatePod(ctx, k8sClient, pod)
	Expect(err).NotTo(HaveOccurred())
	err = WaitForPodReady(ctx, k8sClient, pod, 2*time.Minute)
	Expect(err).NotTo(HaveOccurred())
	return pod
}

// WriteRandomDataToBlockPVC writes random data to the block PVC
// consumed by the given pod and return the written data.
func WriteRandomDataToPVC(
	ctx context.Context,
	pod *corev1.Pod,
	path string,
	length int64,
) []byte {
	GinkgoHelper()

	By("writing random data to the block PVC")
	_, stderr, err := kubectl("exec", "-n", pod.Namespace, pod.Name, "--",
		"dd", "if=/dev/urandom", fmt.Sprintf("of=%s", path),
		fmt.Sprintf("bs=%d", length), "count=1")
	Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))
	_, stderr, err = kubectl("exec", "-n", pod.Namespace, pod.Name, "--", "sync")
	Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))

	By("reading the data from the PVC")
	var writtenData []byte
	writtenData, stderr, err = kubectl("exec", "-n", pod.Namespace, pod.Name, "--",
		"dd", fmt.Sprintf("if=%s", path), "bs=4K", "count=1")
	Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))

	return writtenData
}

func CreateBackup(
	ctx context.Context,
	ctrlClient client.Client,
	namespace string,
	pvc *corev1.PersistentVolumeClaim,
	node string,
) *finv1.FinBackup {
	GinkgoHelper()

	By("creating a backup")
	backup, err := NewFinBackup(rookNamespace, utils.GetUniqueName("test-finbackup-"),
		pvc, node)
	Expect(err).NotTo(HaveOccurred())
	Expect(CreateFinBackup(ctx, ctrlClient, backup)).NotTo(HaveOccurred())
	Expect(WaitForFinBackupStoredToNodeAndVerified(ctx, ctrlClient, backup, 1*time.Minute)).
		NotTo(HaveOccurred())
	return backup
}

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

func CreateRestore(
	ctx context.Context,
	ctrlClient client.Client,
	finbackup *finv1.FinBackup,
	namespace *corev1.Namespace,
	name string,
) *finv1.FinRestore {
	GinkgoHelper()

	By("creating a restore")
	restore, err := NewFinRestore(name, finbackup,
		namespace.Name, name)
	Expect(err).NotTo(HaveOccurred())
	Expect(CreateFinRestore(ctx, ctrlClient, restore)).NotTo(HaveOccurred())
	Expect(WaitForFinRestoreReady(ctx, ctrlClient, restore, 1*time.Minute)).
		NotTo(HaveOccurred())
	return restore
}

func GetNodeNames(ctx context.Context, k8sClient kubernetes.Interface) ([]string, error) {
	nodes, err := k8sClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	nodeNames := make([]string, 0, len(nodes.Items))
	for _, node := range nodes.Items {
		nodeNames = append(nodeNames, node.Name)
	}
	return nodeNames, nil
}

func VerifyRawImage(pvc *corev1.PersistentVolumeClaim, node string, expected []byte) {
	GinkgoHelper()

	By("verifying the data in raw.img")
	expectedData, stderr, err := minikubeSSH(node, nil,
		"dd", fmt.Sprintf("if=/fin/%s/%s/raw.img", pvc.Namespace, pvc.Name),
		fmt.Sprintf("bs=%d", len(expected)), "count=1", "status=none")
	Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))
	Expect(expectedData).To(Equal(expected), "Data in raw.img does not match the expected data")
}

func VerifyNonExistenceOfRawImage(pvc *corev1.PersistentVolumeClaim, node string) {
	GinkgoHelper()

	By("verifying the deletion of raw.img")
	rawImgPath := filepath.Join("/fin", pvc.Namespace, pvc.Name, "raw.img")
	stdout, stderr, err := minikubeSSH(node, nil, "test", "!", "-e", rawImgPath)
	Expect(err).NotTo(HaveOccurred(), "raw.img file should be deleted. stdout: %s, stderr: %s", stdout, stderr)
}

func VerifyDeletionOfJobsForBackup(ctx context.Context, client kubernetes.Interface, finbackup *finv1.FinBackup) {
	err := WaitForJobDeletion(ctx, k8sClient, rookNamespace, fmt.Sprintf("fin-cleanup-%s", finbackup.UID), 10*time.Second)
	Expect(err).NotTo(HaveOccurred(), "Cleanup job should be deleted.")
	err = WaitForJobDeletion(ctx, k8sClient, rookNamespace, fmt.Sprintf("fin-deletion-%s", finbackup.UID), 10*time.Second)
	Expect(err).NotTo(HaveOccurred(), "Deletion job should be deleted.")
}

func VerifyDeletionOfSnapshotInFinBackup(ctx context.Context, ctrlClient client.Client, finbackup *finv1.FinBackup) {
	GinkgoHelper()

	rbdImage := finbackup.Annotations["fin.cybozu.io/backup-target-rbd-image"]
	stdout, stderr, err := kubectl("exec", "-n", rookNamespace, "deploy/rook-ceph-tools", "--",
		"rbd", "info", fmt.Sprintf("%s/%s@fin-backup-%s", poolName, rbdImage, finbackup.UID))
	Expect(err).To(HaveOccurred(), "Snapshot should be deleted. stdout: %s, stderr: %s", stdout, stderr)
}

func VerifyDeletionOfResourcesForRestore(
	ctx context.Context, k8sClient kubernetes.Interface, finrestore *finv1.FinRestore,
) {
	GinkgoHelper()

	By("verifying the deletion of the restore job")
	restoreJobName := fmt.Sprintf("fin-restore-%s", finrestore.UID)
	err := WaitForJobDeletion(ctx, k8sClient, rookNamespace, restoreJobName, 10*time.Second)
	Expect(err).NotTo(HaveOccurred())

	By("verifying the deletion of the restore job PVC")
	_, stderr, err := kubectl("wait", "pvc", "-n", rookNamespace, restoreJobName, "--for=delete", "--timeout=3m")
	Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))

	By("verifying the deletion of the restore job PV")
	_, stderr, err = kubectl("wait", "pv", restoreJobName, "--for=delete", "--timeout=3m")
	Expect(err).NotTo(HaveOccurred(), "stderr: "+string(stderr))
}
