# https://github.com/helm/helm/releases
HELM_VERSION := 4.0.4
# It is set by CI using the environment variable, use conditional assignment.
KUBERNETES_VERSION ?= 1.33.7
# https://github.com/kubernetes/minikube/releases
MINIKUBE_VERSION := v1.37.0
# https://github.com/rook/rook/releases
ROOK_CHART_VERSION := v1.18.6
# https://github.com/kubernetes-sigs/kustomize/releases
KUSTOMIZE_VERSION := v5.7.1
# https://github.com/kubernetes-sigs/controller-tools/releases
CONTROLLER_TOOLS_VERSION := v0.18.0
# https://github.com/golangci/golangci-lint/releases
GOLANGCI_LINT_VERSION := v2.7.2

# Tools versions which are defined in go.mod
SELF_DIR := $(dir $(lastword $(MAKEFILE_LIST)))
GINKGO_VERSION := $(shell awk '/github.com\/onsi\/ginkgo\/v2/ {print $$2}' $(SELF_DIR)/go.mod)
MOCKGEN_VERSION := $(shell awk '$$1 == "go.uber.org/mock" {print $$2}' $(SELF_DIR)/go.mod)

ENVTEST_KUBERNETES_VERSION := $(shell echo $(KUBERNETES_VERSION) | cut -d "." -f 1-2).0
