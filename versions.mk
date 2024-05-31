# https://github.com/containerd/containerd/releases
CONTAINERD_VERSION := 1.7.17
# https://github.com/opencontainers/runc/releases
RUNC_VERSION := v1.1.12
# https://github.com/containernetworking/plugins/releases
CNI_PLUGINS_VERSION := v1.5.0
# https://github.com/kubernetes-sigs/cri-tools/releases
CRICTL_VERSION := v1.30.0
# https://github.com/helm/helm/releases
HELM_VERSION := 3.15.1
# kind node image version is related to kind version.
# if you change kind version, also change kind node image version.
# https://github.com/kubernetes-sigs/kind/releases
KIND_VERSION := v0.20.0
# It is set by CI using the environment variable, use conditional assignment.
KUBERNETES_VERSION ?= 1.27.10
# https://github.com/kubernetes/minikube/releases
MINIKUBE_VERSION := v1.33.1
# https://github.com/rook/rook/releases
ROOK_CHART_VERSION := v1.13.8

ENVTEST_KUBERNETES_VERSION := $(shell echo $(KUBERNETES_VERSION) | cut -d "." -f 1-2)

# Tools versions which are defined in go.mod
SELF_DIR := $(dir $(lastword $(MAKEFILE_LIST)))
CONTROLLER_TOOLS_VERSION := $(shell awk '/sigs\.k8s\.io\/controller-tools/ {print substr($$2, 2)}' $(SELF_DIR)/go.mod)
GINKGO_VERSION := $(shell awk '/github.com\/onsi\/ginkgo\/v2/ {print $$2}' $(SELF_DIR)/go.mod)
