include ../../versions.mk

SHELL := /bin/bash
BINDIR := $(shell pwd)/bin
CURL := curl -sSfL
GINKGO := $(BINDIR)/ginkgo-$(GINKGO_VERSION)
GINKGO_FLAGS :=
MINIKUBE := $(BINDIR)/minikube-$(MINIKUBE_VERSION)
KUBECTL := $(BINDIR)/kubectl-$(KUBERNETES_VERSION)
HELM := $(BINDIR)/helm-$(HELM_VERSION)
MINIKUBE_HOME := $(BINDIR)
NS := rook-ceph
NODE_NAME := minikube-worker
LOOP_FILE := loop.img
LOOP_DEV := /dev/loop0
POLLING_INTERVAL := 3
TIMEOUT_SECS := 120

export MINIKUBE_HOME

IMG_TAR = controller.tar

.PHONY: test
test:
	$(MAKE) launch-minikube
	$(MAKE) image-build
	$(MAKE) image-tar-load
	$(MAKE) launch-rook-ceph
	$(MAKE) setup-fin-volume
	$(MAKE) setup-components
	$(MAKE) do-test

.PHONY: clean
clean:
	$(MAKE) delete-minikube

$(BINDIR):
	mkdir -p $@

$(GINKGO): | $(BINDIR)
	GOBIN=$(BINDIR) go install github.com/onsi/ginkgo/v2/ginkgo@$(GINKGO_VERSION)
	mv $(BINDIR)/ginkgo $@

$(KUBECTL): | $(BINDIR)
	$(CURL) -o $@ https://dl.k8s.io/release/v$(KUBERNETES_VERSION)/bin/linux/amd64/kubectl
	chmod a+x $@

$(MINIKUBE): | $(BINDIR)
	$(CURL) -o $@ https://github.com/kubernetes/minikube/releases/download/$(MINIKUBE_VERSION)/minikube-linux-amd64
	chmod a+x $@

$(HELM): | $(BINDIR)
	$(CURL) https://get.helm.sh/helm-v$(HELM_VERSION)-linux-amd64.tar.gz \
		| tar xvz -C $(BINDIR) --strip-components 1 linux-amd64/helm
	mv $(BINDIR)/helm $@

.PHONY: launch-minikube
launch-minikube: $(MINIKUBE) $(KUBECTL)
	HOST_STATUS=$$($(MINIKUBE) status -f "{{.Host}}"); \
	if [ "$${HOST_STATUS}" = "Running" ]; then \
		echo "minikube is already running"; \
	else \
		$(MINIKUBE) start \
			--kubernetes-version="v$(KUBERNETES_VERSION)" \
			--driver=kvm2 \
			--memory 6g \
			--cpus=2 \
			--disk-size 40g \
			--extra-config=kubeadm.node-name=$(NODE_NAME) \
			--extra-config=kubelet.hostname-override=$(NODE_NAME); \
	fi
	$(MINIKUBE) kubectl -- get nodes
	$(MINIKUBE) node list

.PHONY: delete-minikube
delete-minikube: $(MINIKUBE)
	$(MINIKUBE) stop || true
	$(MINIKUBE) delete || true

.PHONY: create-loop-dev
create-loop-dev:
	$(MINIKUBE) ssh -- dd if=/dev/zero of=$(LOOP_FILE) bs=1G seek=32 count=0
	$(MINIKUBE) ssh -- sudo losetup $(LOOP_DEV) $(LOOP_FILE) || :
	$(MINIKUBE) ssh -- lsblk

.PHONY: setup-fin-volume
setup-fin-volume: $(MINIKUBE) $(KUBECTL)
	$(MINIKUBE) ssh -- sudo mkdir -p /fin
	$(MINIKUBE) ssh -- sudo chown -R 10000:10000 /fin
	$(MINIKUBE) ssh -- ls -ld /fin
	$(KUBECTL) apply -f testdata/fin-volume.yaml
	$(KUBECTL) -n $(NS) get pv local-osd -o yaml
	$(KUBECTL) get nodes


.PHONY: wait-deploy-ready
wait-deploy-ready: NS=
wait-deploy-ready: DEPLOY=
wait-deploy-ready:
	is_ok="false"; \
	for ((i=0;i<$(TIMEOUT_SECS);i+=$(POLLING_INTERVAL))); do \
		available_replicas=$$($(KUBECTL) -n $(NS) get deploy $(DEPLOY) -o json | jq -r ".status.availableReplicas"); \
		if [ "$$available_replicas" = 1 ]; then \
			is_ok="true"; \
			break; \
		fi; \
		$(KUBECTL) -n $(NS) get pod -o wide; \
		$(KUBECTL) -n $(NS) get deploy -o wide --show-labels; \
		echo "waiting for deploy $(DEPLOY) to be available" > /dev/stderr; \
		sleep $(POLLING_INTERVAL); \
	done; \
	if [ "$$is_ok" = "false" ]; then \
		echo "failed to start deploy $(DEPLOY)" > /dev/stderr; \
		$(KUBECTL) -n $(NS) logs deploy/rook-ceph-operator; \
		exit 1; \
	fi

.PHONY: launch-rook-ceph
launch-rook-ceph: create-loop-dev $(KUBECTL) $(HELM)
	$(HELM) upgrade --install --version $(ROOK_CHART_VERSION) --repo https://charts.rook.io/release \
		--create-namespace --namespace $(NS) rook-ceph rook-ceph -f testdata/values.yaml --wait
	sed \
		-e "s%{LOOP_DEV}%$(LOOP_DEV)%" \
		-e "s%{NODE_NAME}%$(NODE_NAME)%" \
		testdata/persistentvolumes-template.yaml | \
		$(KUBECTL) apply -f -
	$(HELM) upgrade --install --version $(ROOK_CHART_VERSION) --repo https://charts.rook.io/release \
	    --namespace $(NS) rook-ceph-cluster rook-ceph-cluster -f testdata/values-cluster.yaml
	$(MAKE) wait-deploy-ready NS=$(NS) DEPLOY=rook-ceph-operator
	$(MINIKUBE) ssh -- df -h
	$(MAKE) wait-deploy-ready NS=$(NS) DEPLOY=rook-ceph-osd-0
	$(KUBECTL) apply -f testdata/cephblockpool.yaml
	$(KUBECTL) apply -f testdata/storageclass.yaml

.PHONY: image-build
image-build:
	$(MAKE) -C ../.. docker-build

.PHONY: image-tar-load
image-tar-load:
	$(MAKE) -C ../.. docker-save
	$(MINIKUBE) image load ../../${IMG_TAR}
	$(MINIKUBE) ssh -- docker images || true
	rm ../../${IMG_TAR} || true

.PHONY: setup-components
setup-components:
	$(MAKE) -C ../.. install
	$(MAKE) -C ../.. deploy
	$(KUBECTL) patch deploy -n $(NS) fin-controller-manager --type='json' -p='[{"op": "add", "path": "/spec/template/spec/containers/0/args/-", "value":"--raw-img-expansion-unit-size=10485760"}]'


.PHONY: do-test
do-test: $(GINKGO) $(KUBECTL) $(MINIKUBE)
	env \
	PATH=${PATH} \
	E2ETEST=1 \
	KUBECTL=$(KUBECTL) \
	MINIKUBE=$(MINIKUBE) \
	$(GINKGO) --fail-fast -v $(GINKGO_FLAGS) .
