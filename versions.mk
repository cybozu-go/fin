# https://github.com/helm/helm/releases
HELM_VERSION := 3.17.4
# It is set by CI using the environment variable, use conditional assignment.
KUBERNETES_VERSION ?= 1.32.7
# https://github.com/kubernetes/minikube/releases
#
# NOTE(Sep. 9th, 2025):
# We intentionally do not use the latest minikube v1.36.0.
# It's because Rook takes too long time to create an OSD pod
# with Kubernetes v1.32 created by minikube v1.36.0.
#
# We should revisit this decision when we would like to upgrade
# kubernetes version to v1.33 or later.
MINIKUBE_VERSION := v1.35.0
# https://github.com/rook/rook/releases
ROOK_CHART_VERSION := v1.17.8

ENVTEST_K8S_VERSION := $(shell echo $(KUBERNETES_VERSION) | cut -d "." -f 1-2)

# Tools versions
SELF_DIR := $(dir $(lastword $(MAKEFILE_LIST)))
GINKGO_VERSION := $(shell awk '/github.com\/onsi\/ginkgo\/v2/ {print $$2}' $(SELF_DIR)/go.mod)
KUSTOMIZE_VERSION ?= v5.7.0
CONTROLLER_TOOLS_VERSION ?= v0.18.0
ENVTEST_VERSION ?= release-0.20
GOLANGCI_LINT_VERSION ?= v2.3.1
