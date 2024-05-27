include ../../versions.mk

SHELL := /bin/bash
BINDIR := $(shell pwd)/bin
TMPDIR := $(shell pwd)/tmp
CURL := curl -sSfL
GINKGO := $(BINDIR)/ginkgo-$(GINKGO_VERSION)
GINKGO_FLAGS :=
MINIKUBE := $(BINDIR)/minikube-$(MINIKUBE_VERSION)
KUBECTL := $(BINDIR)/kubectl-$(KUBERNETES_VERSION)
HELM := $(BINDIR)/helm-$(HELM_VERSION)
MINIKUBE_HOME := $(BINDIR)
NS := rook-ceph
NODE_NAME := minikube-worker
LOOP_FILE := $(TMPDIR)/loop.img
LOOP_DEV := $(shell loop_dev=$$(losetup | grep $(LOOP_FILE) | cut -d ' ' -f 1); \
		if [ -z "$$loop_dev" ]; then \
			losetup -f; \
		else \
			echo $$loop_dev; \
		fi)
CNI_PLUGIN_INSTALL_DIR := /opt/cni/bin
POLLING_INTERVAL := 1
TIMEOUT_SECS := 180

export MINIKUBE_HOME

define loop-dev
$(shell losetup | grep $(LOOP_FILE) | cut -d ' ' -f 1)
endef

.PHONY: setup
setup: | $(CNI_PLUGIN_INSTALL_DIR)
	sudo apt-get update
	sudo apt-get install -y conntrack socat
# cf. https://github.com/containerd/containerd/blob/main/docs/getting-started.md#option-1-from-the-official-binaries
	$(CURL) https://github.com/containerd/containerd/releases/download/v${CONTAINERD_VERSION}/containerd-${CONTAINERD_VERSION}-linux-amd64.tar.gz \
		| sudo tar xvz -C /usr/local
# Regenerate a config file for containerd as a workaround for minikube.
	sudo sh -c "containerd config default > /etc/containerd/config.toml"
	sudo sed -i 's/SystemdCgroup = false/SystemdCgroup = true/'  /etc/containerd/config.toml
	$(CURL) -O https://raw.githubusercontent.com/containerd/containerd/v${CONTAINERD_VERSION}/containerd.service
	sudo mv containerd.service /lib/systemd/system
	sudo systemctl daemon-reload
	sudo systemctl enable --now containerd
	$(CURL) -O https://github.com/opencontainers/runc/releases/download/${RUNC_VERSION}/runc.amd64
	sudo install -m 755 runc.amd64 /usr/local/sbin/runc
	rm runc.amd64
	$(CURL) https://github.com/containernetworking/plugins/releases/download/$(CNI_PLUGINS_VERSION)/cni-plugins-linux-amd64-$(CNI_PLUGINS_VERSION).tgz \
		| sudo tar xvz -C $(CNI_PLUGIN_INSTALL_DIR)

	$(CURL) https://github.com/kubernetes-sigs/cri-tools/releases/download/$(CRICTL_VERSION)/crictl-$(CRICTL_VERSION)-linux-amd64.tar.gz \
		| sudo tar xvz -C /usr/local/bin
	sudo sysctl fs.protected_regular=0

.PHONY: test
test:
	$(MAKE) launch-minikube
	$(MAKE) launch-rook-ceph
	$(MAKE) setup-components
	$(MAKE) do-test

.PHONY: clean
clean:
	$(MAKE) delete-components
	$(MAKE) delete-rook-ceph
	$(MAKE) delete-minikube

$(BINDIR):
	mkdir -p $@

$(CNI_PLUGIN_INSTALL_DIR):
	sudo mkdir -p $@

$(TMPDIR):
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
		sudo -E $(MINIKUBE) start \
			--kubernetes-version="v$(KUBERNETES_VERSION)" \
			--driver=none \
			--container-runtime=containerd \
			--cni=calico \
			--extra-config=kubeadm.node-name=$(NODE_NAME) \
			--extra-config=kubelet.hostname-override=$(NODE_NAME); \
		sudo chown -R $$USER $$HOME/.kube $(MINIKUBE_HOME)/.minikube; \
		sudo chmod -R a+r $$HOME/.kube $(MINIKUBE_HOME)/.minikube; \
		sudo find $(MINIKUBE_HOME)/.minikube -name id_rsa -exec chmod 600 {} ';'; \
	fi

.PHONY: delete-minikube
delete-minikube: $(MINIKUBE)
	sudo -E $(MINIKUBE) delete || true
	# Is it really OK to remove these files?
	rm -rf /tmp/juju-*

.PHONY: create-loop-dev
create-loop-dev: | $(TMPDIR)
	losetup | grep -q $(LOOP_FILE); \
	RET=$$? ; \
	if [ $$RET -ne 0 ]; then \
		dd if=/dev/zero of=$(LOOP_FILE) bs=1G seek=32 count=0; \
		sudo losetup $(LOOP_DEV) $(LOOP_FILE); \
	fi

.PHONY: delete-loop-dev
delete-loop-dev: | $(TMPDIR)
	sudo losetup -d $(LOOP_DEV)
	sudo rm -f $(LOOP_FILE)

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
		-e "s%{LOOP_DEV}%$(loop-dev)%" \
		-e "s%{NODE_NAME}%$(NODE_NAME)%" \
		testdata/persistentvolumes-template.yaml | \
		$(KUBECTL) apply -f -
	$(HELM) upgrade --install --version $(ROOK_CHART_VERSION) --repo https://charts.rook.io/release --namespace $(NS) rook-ceph-cluster rook-ceph-cluster -f testdata/values-cluster.yaml --wait
	$(MAKE) wait-deploy-ready NS=$(NS) DEPLOY=rook-ceph-operator
	$(MAKE) wait-deploy-ready NS=$(NS) DEPLOY=rook-ceph-osd-0
	$(KUBECTL) apply -f testdata/cephblockpool.yaml
	$(KUBECTL) apply -f testdata/storageclass.yaml

.PHONY: delete-rook-ceph
delete-rook-ceph: $(KUBECTL) $(HELM)
	$(HELM) uninstall --namespace $(NS) rook-ceph-cluster --wait || :
# cf. https://github.com/rook/rook/blob/master/Documentation/Storage-Configuration/ceph-teardown.md#delete-the-data-on-hosts
	sudo rm -rf /var/lib/rook
	$(HELM) uninstall --namespace $(NS) rook-ceph --wait || :
	$(KUBECTL) delete pv $$(yq '.metadata.name' testdata/persistentvolumes-template.yaml) || :
	$(MAKE) delete-loop-dev

.PHONY: setup-components
setup-components:
	$(MAKE) -C ../.. docker-build
	# We can't use `minikube image load` due to a minikube's problem.
	# ref. https://github.com/kubernetes/minikube/issues/17785
	docker save controller:latest | sudo ctr -n k8s.io images import -

	$(MAKE) -C ../.. install
	$(MAKE) -C ../.. deploy

.PHONY: delete-components
delete-components:
	$(MAKE) -C ../.. undeploy || :
	$(MAKE) -C ../.. uninstall || :

.PHONY: do-test
do-test: $(GINKGO) $(KUBECTL)
	sudo -E env \
	PATH=${PATH} \
	E2ETEST=1 \
	KUBECTL=$(KUBECTL) \
	$(GINKGO) --fail-fast -v $(GINKGO_FLAGS) .
