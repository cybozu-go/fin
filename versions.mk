# https://github.com/helm/helm/releases
HELM_VERSION := 4.2.0
# It is set by CI using the environment variable, use conditional assignment.
KUBERNETES_VERSION ?= 1.35.4
# https://github.com/kubernetes/minikube/releases
MINIKUBE_VERSION := v1.38.1
# https://github.com/rook/rook/releases
ROOK_CHART_VERSION := v1.19.5
# https://quay.io/repository/ceph/ceph
CEPH_IMAGE_VERSION := v19.2.2
# https://github.com/ceph/ceph-csi/releases
CEPH_CSI_IMAGE_VERSION := v3.16.2
# https://github.com/kubernetes-sigs/kustomize/releases
KUSTOMIZE_VERSION := v5.8.1
# https://github.com/kubernetes-sigs/controller-tools/releases
CONTROLLER_TOOLS_VERSION := v0.20.1
# https://github.com/golangci/golangci-lint/releases
GOLANGCI_LINT_VERSION := v2.11.4

# Tools versions which are defined in go.mod
SELF_DIR := $(dir $(lastword $(MAKEFILE_LIST)))
GINKGO_VERSION := $(shell awk '/github.com\/onsi\/ginkgo\/v2/ {print $$2}' $(SELF_DIR)/go.mod)
MOCKGEN_VERSION := $(shell awk '$$1 == "go.uber.org/mock" {print $$2}' $(SELF_DIR)/go.mod)

ENVTEST_KUBERNETES_VERSION := $(shell echo $(KUBERNETES_VERSION) | cut -d "." -f 1-2).0
