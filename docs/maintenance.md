# Maintenance Guide

## How to change the supported Kubernetes minor versions

Fin depends on some Kubernetes repositories like `k8s.io/client-go` and supports two Kubernetes versions at a time.

Issues and PRs related the latest upgrade task also help you understand how to upgrade the supported versions, so checking them together with this guide is recommended when you do this task.

### Upgrade procedure

#### Kubernetes

When upgrading, add the new version and drop the oldest to keep supporting two consecutive minor versions.

Choose the appropriate versions and check the [release note](https://kubernetes.io/docs/setup/release/notes/).

Update the supported Kubernetes minor versions in `README.md`.

We should also update go.mod. According to [the Kubebuilder documentation](https://book.kubebuilder.io/versions_compatibility_supportability), we should use versions compatible with Kubebuilder, so refer to the samples in the testdata directory of the latest Kubebuilder release that supports the target Kubernetes minor (e.g., `https://github.com/kubernetes-sigs/kubebuilder/blob/<kubebuilder-release-tag>/testdata/project-v4/go.mod`) to see which versions should be used.

First, update `k8s.io/*` libraries. Please note that Kubernetes v1 corresponds with v0 for the release tags. For example, v1.17.2 corresponds with the v0.17.2 tag.

```bash
$ VERSION=<upgrading Kubernetes release version>
$ go get k8s.io/api@v${VERSION} k8s.io/apimachinery@v${VERSION} k8s.io/client-go@v${VERSION} k8s.io/component-helpers@v${VERSION}
```

Next, update controller-runtime to the version in the `go.mod` file in that Kubebuilder release's testdata directory. For example, see `https://github.com/kubernetes-sigs/kubebuilder/blob/<kubebuilder-release-tag>/testdata/project-v4/go.mod`. Before updating it, please read the [`controller-runtime`'s release note](https://github.com/kubernetes-sigs/controller-runtime/releases). If there are breaking changes, we should decide how to manage these changes.

```
$ VERSION=<upgrading controller-runtime version>
$ go get sigs.k8s.io/controller-runtime@v${VERSION}
```

Finally, update controller-tools to the version in the `Makefile` in that Kubebuilder release's testdata directory. For example, see `https://github.com/kubernetes-sigs/kubebuilder/blob/<kubebuilder-release-tag>/testdata/project-v4/Makefile`. Before updating it, please read the [`controller-tools`'s release note](https://github.com/kubernetes-sigs/controller-tools/releases). If there are breaking changes, we should decide how to manage these changes.
To change the version, edit `versions.mk`.

#### Go

Choose the version compatible with Kubebuilder (e.g., `https://github.com/kubernetes-sigs/kubebuilder/blob/<kubebuilder-release-tag>/testdata/project-v4/go.mod#L3`).

Edit the following files.

- `go.mod`
- `Dockerfile`
- `README.md`

#### Depending tools

The following tools don't depend on other software, so use the latest versions.
To change their versions, edit `versions.mk`.

- [helm](https://github.com/helm/helm/releases)
- [kustomize](https://github.com/kubernetes-sigs/kustomize/releases)
- [minikube](https://github.com/kubernetes/minikube/releases)
  - After choosing a Minikube version, check the Kubernetes versions it supports in `https://github.com/kubernetes/minikube/blob/<minikube-release-tag>/pkg/minikube/constants/constants_kubernetes_versions.go`. For each target Kubernetes minor, set its newest stable patch version in the `kubernetes-version` matrix in `.github/workflows/e2e.yaml`. Also set `KUBERNETES_VERSION` in `versions.mk`.
- [golangci-lint](https://github.com/golangci/golangci-lint/releases)

Some operators are described in `test/utils/utils.go`. Please check their versions and update them, if necessary. Note that it should use the LTS version for cert-manager.

- [prometheus-operator](https://github.com/prometheus-operator/prometheus-operator/releases)
- [cert-manager](https://github.com/cert-manager/cert-manager/releases)

Update the following version in Dockerfile, if necessary, too:

- custom rbd-export-diff

#### Depending modules

Read Kubernetes's `go.mod` at https://github.com/kubernetes/kubernetes/blob/main/go.mod (replace `main` with the target release branch, such as `release-1.36`) and update the `prometheus/*` modules to the versions listed there. Here is the example to update `prometheus/client_golang`.

```
$ VERSION=<upgrading prometheus-related libraries release version>
$ go get github.com/prometheus/client_golang@v${VERSION}
```

Update `k8s.io/utils` to the version in Kubernetes's `go.mod`.

```
$ VERSION=<k8s.io/utils version in Kubernetes go.mod>
$ go get k8s.io/utils@${VERSION}
```

Then, please tidy up the dependencies.

```bash
$ go mod tidy
```

Regenerate manifests using new controller-tools.

```console
$ sudo rm -rf bin
$ make generate
```

#### Final check

`git grep <the kubernetes version which support will be dropped>`, `git grep image:`, `git grep -i VERSION` and looking `versions.mk` might help to avoid overlooking necessary changes.
