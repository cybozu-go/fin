include ../../versions.mk

SHELL := /bin/bash
BINDIR := $(shell pwd)/bin
CURL := curl -sSfL
GINKGO := $(BINDIR)/ginkgo-$(GINKGO_VERSION)
GINKGO_FLAGS :=
MINIKUBE_PROFILE_NAME := fin-test
MINIKUBE_BIN := $(BINDIR)/minikube-$(MINIKUBE_VERSION)
MINIKUBE := $(MINIKUBE_BIN) --profile=$(MINIKUBE_PROFILE_NAME)
KUBECTL := $(BINDIR)/kubectl-$(KUBERNETES_VERSION)
HELM := $(BINDIR)/helm-$(HELM_VERSION)
MINIKUBE_HOME := $(BINDIR)
NS := rook-ceph
LOOP_FILE := loop.img
LOOP_DEV := /dev/loop0
FIN_VOLUME_LOOP_DEV := /dev/loop1
POLLING_INTERVAL := 2
TIMEOUT_SECS := 600

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

$(MINIKUBE_BIN): | $(BINDIR)
	$(CURL) -o $@ https://github.com/kubernetes/minikube/releases/download/$(MINIKUBE_VERSION)/minikube-linux-amd64
	chmod a+x $@

$(HELM): | $(BINDIR)
	$(CURL) https://get.helm.sh/helm-v$(HELM_VERSION)-linux-amd64.tar.gz \
		| tar xvz -C $(BINDIR) --strip-components 1 linux-amd64/helm
	mv $(BINDIR)/helm $@

.PHONY: launch-minikube
launch-minikube: $(MINIKUBE_BIN) $(KUBECTL)
	HOST_STATUS=$$($(MINIKUBE) status -f "{{.Host}}"); \
	if [ "$${HOST_STATUS}" = "Running" ]; then \
		echo "minikube is already running"; \
	else \
		$(MINIKUBE) start \
			--kubernetes-version="v$(KUBERNETES_VERSION)" \
			--driver=kvm2 \
			--nodes=2 \
			--memory 6g \
			--cpus=2; \
	fi

launch-multinode-minikube:
	LAUNCH_MINIKUBE_EXTRA_ARGS=--nodes=2 $(MAKE) launch-minikube

.PHONY: delete-minikube
delete-minikube: $(MINIKUBE_BIN)
	$(MINIKUBE) stop || true
	$(MINIKUBE) delete || true

.PHONY: create-loop-dev
create-loop-dev:
	$(MINIKUBE) ssh -- dd if=/dev/zero of=$(LOOP_FILE) bs=1G seek=32 count=0
	$(MINIKUBE) ssh -- sudo losetup $(LOOP_DEV) $(LOOP_FILE) || :
	$(MINIKUBE) ssh -- lsblk

.PHONY: setup-fin-volume
setup-fin-volume: $(MINIKUBE_BIN) $(KUBECTL)
	for node in $(MINIKUBE_PROFILE_NAME) $(MINIKUBE_PROFILE_NAME)-m02; do \
		$(MINIKUBE) ssh -n $$node -- sudo dd if=/dev/zero of=fin-volume.img bs=1G seek=1 count=0; \
		$(MINIKUBE) ssh -n $$node -- sudo losetup /dev/loop1 fin-volume.img || :; \
		$(MINIKUBE) ssh -n $$node -- sudo mkfs.xfs /dev/loop1 || :; \
		$(MINIKUBE) ssh -n $$node -- sudo mkdir -p /fin; \
		$(MINIKUBE) ssh -n $$node -- sudo mount /dev/loop1 /fin; \
		$(MINIKUBE) ssh -n $$node -- sudo chown -R 10000:10000 /fin; \
		$(MINIKUBE) ssh -n $$node -- ls -ld /fin; \
	done
	$(KUBECTL) apply -f testdata/fin-volume.yaml

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
		echo "waiting for deploy $(DEPLOY) to be available" > /dev/stderr; \
		sleep $(POLLING_INTERVAL); \
	done; \
	if [ "$$is_ok" = "false" ]; then \
		echo "failed to start deploy $(DEPLOY)" > /dev/stderr; \
		exit 1; \
	fi

.PHONY: launch-rook-ceph
launch-rook-ceph: create-loop-dev $(KUBECTL) $(HELM)
	$(HELM) upgrade --install --version $(ROOK_CHART_VERSION) --repo https://charts.rook.io/release \
		--create-namespace --namespace $(NS) rook-ceph rook-ceph -f testdata/values.yaml --wait
	sed \
		-e "s%{LOOP_DEV}%$(LOOP_DEV)%" \
		-e "s%{NODE_NAME}%$(MINIKUBE_PROFILE_NAME)%" \
		testdata/persistentvolumes-template.yaml | \
		$(KUBECTL) apply -f -
	$(HELM) upgrade --install --version $(ROOK_CHART_VERSION) --repo https://charts.rook.io/release --namespace $(NS) rook-ceph-cluster rook-ceph-cluster -f testdata/values-cluster.yaml --wait
	$(MAKE) wait-deploy-ready NS=$(NS) DEPLOY=rook-ceph-operator
	$(MAKE) wait-deploy-ready NS=$(NS) DEPLOY=rook-ceph-osd-0
	$(KUBECTL) apply -f testdata/cephblockpool.yaml
	$(KUBECTL) apply -f testdata/storageclass.yaml

.PHONY: image-build
image-build:
	$(MAKE) -C ../.. docker-build

.PHONY: image-tar-load
image-tar-load:
	df -h /
	$(MAKE) -C ../.. docker-save
	df -h /
	$(MINIKUBE) image load ../../${IMG_TAR}
	$(MINIKUBE) ssh -- docker images || true
	for node in $$($(MINIKUBE) node list | awk '{print $$1}'); do \
		df -h /
		$(MINIKUBE) ssh -n "$(node)" -- "sudo df -h / /mnt/vda1"; \
		$(MINIKUBE) ssh -n "$(node)" -- "sudo ls -lh /var/lib/minikube/images/${IMG_TAR}"; \
		$(MINIKUBE) ssh -n "$(node)" -- "sudo rm -f  /var/lib/minikube/images/${IMG_TAR}"; \
		$(MINIKUBE) ssh -n "$(node)" -- "sudo ls -lh /var/lib/minikube/images/${IMG_TAR}"; \
		$(MINIKUBE) ssh -n "$(node)" -- "sudo df -h / /mnt/vda1"; \
		df -h /
	done
	rm ../../${IMG_TAR} || true
	df -h /
	

.PHONY: setup-components
setup-components:
	$(MAKE) -C ../.. install
	$(MAKE) -C ../.. deploy
	# Disable webhooks for e2e tests
	$(KUBECTL) -n $(NS) set env deployment/fin-controller-manager ENABLE_WEBHOOKS=false
	$(KUBECTL) patch deploy -n $(NS) fin-controller-manager --type='json' -p='[{"op": "add", "path": "/spec/template/spec/containers/0/args/-", "value":"--raw-img-expansion-unit-size=10485760"}]'

.PHONY: do-test
do-test: $(GINKGO) $(KUBECTL) $(MINIKUBE_BIN)
	env \
	PATH=${PATH} \
	E2ETEST=1 \
	KUBECTL=$(KUBECTL) \
	MINIKUBE=$(MINIKUBE) \
	$(GINKGO) --fail-fast -v $(GINKGO_FLAGS) .
