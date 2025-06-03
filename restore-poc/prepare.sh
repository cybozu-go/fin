#!/bin/sh -uex

NS="ceph-ssd"

kubectl apply -f sts.yaml

kubectl wait -n $NS pod/restore-poc-0 --for=condition=Ready --timeout=600s

IMAGE_NAME=$(kubectl get pv $(kubectl get pvc -n ${NS} data-restore-poc-0 -o json | jq -r .spec.volumeName) -o json | jq -r '.spec.csi.volumeAttributes.imageName')
echo "IMAGE_NAME: ${IMAGE_NAME}"

MANTLE=$(kubectl get pod -n ceph-ssd -lapp.kubernetes.io/instance=mantle -ojsonpath='{.items[0].metadata.name}')

kubectl exec -n ${NS} restore-poc-0 -- dd if=/dev/urandom of=/data/test0.img bs=1M count=100
kubectl exec -n ${NS} restore-poc-0 -- sync
kubectl exec -n ${NS} ${MANTLE} -c mantle -- rbd -p ceph-ssd-block-pool snap create ${IMAGE_NAME}@s0
kubectl exec -n ${NS} ${MANTLE} -c mantle -- rbd -p ceph-ssd-block-pool export ${IMAGE_NAME}@s0 /tmp/raw.img
kubectl cp ${NS}/${MANTLE}:/tmp/raw.img raw.img
kubectl exec -n ${NS} restore-poc-0 -- dd if=/dev/urandom of=/data/test1.img bs=1M count=10
kubectl exec -n ${NS} restore-poc-0 -- sync
kubectl exec -n ${NS} ${MANTLE} -c mantle -- rbd -p ceph-ssd-block-pool snap create ${IMAGE_NAME}@s1

READ_LENGTH=$((100*1024*1024))
for i in $(seq 0 4); do
  kubectl -n ${NS} exec ${MANTLE} -c mantle -- bash -c "rbd -p ceph-ssd-block-pool export-diff --read-offset $((${READ_LENGTH}*${i})) --read-length ${READ_LENGTH} --from-snap s0 ${IMAGE_NAME}@s1 /tmp/diff${i}.img"
  kubectl cp ${NS}/${MANTLE}:/tmp/diff${i}.img diff${i}.img
done
