# How to make test data in `internal/infrastructure/ceph/testdata`

We use two types of data for testing: full backup and incremental backup.
The following is a list of data used for testing, each compressed with gzip.
- full: Created by taking a snapshot from the full backup volume as shown below and running `rbd export-diff`
- full-raw-img: Complete dump of the full backup volume
- diff: Created by taking a snapshot from the incremental backup volume as shown below and running `rbd export-diff`
- diff-raw-img: Complete dump of the incremental backup volume

- Full backup volume
  - Contains random data from the beginning for 1KiB, and random data from 9.999MiB to 10MiB. Other areas are unused
  - Total size is 100MiB
  - Snapshot name is `snap20`
- Incremental backup volume
  - Contains 1KiB of random data starting from 5MiB. Other areas are unused and discarded.
  - Total size remains 100MiB, same as the full backup
  - Snapshot name is `snap21`

## How to create test data

Create PVC and Pod using the following manifest on Kubernetes where Rook/Ceph is running.
```yaml
apiVersion: v1
kind: Pod
metadata:
  name: test-pod-block
spec:
  volumes:
    - name: task-pv-storage
      persistentVolumeClaim:
        claimName: test-pvc-block
  containers:
    - name: task-pv-container
      image: ghcr.io/cybozu/ubuntu:24.04
      command: ["pause"]
      volumeDevices:
        - devicePath: "/data"
          name: task-pv-storage
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: test-pvc-block
spec:
  volumeMode: Block
  storageClassName: <the strage class for a rbd volume>
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 100Mi
```

Write test data to the full backup volume and obtain raw data using the following commands.
```sh
# At host that can run kubectl
kubectl exec test-pod-block -- blkdiscard -z /data
kubectl exec test-pod-block -- dd if=/dev/urandom of=/data bs=1K count=1
kubectl exec test-pod-block -- dd if=/dev/urandom of=/data bs=1K count=1 seek=10239
kubectl exec test-pod-block -- sync
kubectl exec test-pod-block -- dd if=/data bs=1M > full-raw-img
```

Create a snapshot of the full backup volume and export it using the following commands.
```sh
NS=<namespace of the tools pod>
POOL=<pool name of the volume>
VOLUME=<volume id>
kubectl exec -n $NS deployments/rook-ceph-tools -- rbd snap create -p $POOL $VOLUME@snap20
kubectl exec -n $NS deployments/rook-ceph-tools -- rbd export-diff -p $POOL $VOLUME@snap20 - > full
```

Write test data to the incremental backup volume and obtain raw data using the following commands.
```sh
# At host that can run kubectl
kubectl exec test-pod-block -- blkdiscard -z /data
kubectl exec test-pod-block -- dd if=/dev/urandom of=/data bs=1K count=1 seek=5120
kubectl exec test-pod-block -- sync
kubectl exec test-pod-block -- dd if=/data bs=1M > diff-raw-img
```

Create a snapshot of the incremental backup volume and export it using the following commands.
```sh
NS=<namespace of the tools pod>
POOL=<pool name of the volume>
VOLUME=<volume id>

kubectl exec -n $NS deployments/rook-ceph-tools -- rbd snap create -p $POOL $VOLUME@snap21
kubectl exec -n $NS deployments/rook-ceph-tools -- rbd export-diff -p $POOL --from-snap snap20 $VOLUME@snap21 - > diff
```

Compress with gzip to reduce size, then copy to the `testdata` folder.
```sh
gzip full
gzip full-raw-img

gzip diff
gzip diff-raw-img
```
