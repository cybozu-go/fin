# Maintenance Guide

## How to change the supported Kubernetes minor versions

Fin depends on some Kubernetes repositories like `k8s.io/client-go` and supports only one Kubernetes version at a time.

Issues and PRs related the latest upgrade task also help you understand how to upgrade the supported versions, so checking them together with this guide is recommended when you do this task.

### Upgrade procedure

#### Kubernetes

Choose the next version and check the [release note](https://kubernetes.io/docs/setup/release/notes/).
e.g. 1.17 -> 1.18

To change the version, edit the following files.

<!--
- `.github/workflows/e2e.yaml`
-->
- `README.md`
- `versions.mk`

We should also update go.mod by the following commands. Please note that Kubernetes v1 corresponds with v0 for the release tags. For example, v1.17.2 corresponds with the v0.17.2 tag.

```bash
$ VERSION=<upgrading Kubernetes release version>
$ go get k8s.io/api@v${VERSION} k8s.io/apimachinery@v${VERSION} k8s.io/client-go@v${VERSION} k8s.io/component-helpers@v${VERSION}
```

Read the [`controller-runtime`'s release note](https://github.com/kubernetes-sigs/controller-runtime/releases), and update to the newest version that is compatible with all supported kubernetes versions. If there are breaking changes, we should decide how to manage these changes.

```
$ VERSION=<upgrading controller-runtime version>
$ go get sigs.k8s.io/controller-runtime@v${VERSION}
```

Read the [`controller-tools`'s release note](https://github.com/kubernetes-sigs/controller-tools/releases), and update to the newest version that is compatible with all supported kubernetes versions. If there are breaking changes, we should decide how to manage these changes.
To change the version, edit `versions.mk`.

#### Go

Choose the same version of Go [used by the latest Kubernetes](https://github.com/kubernetes/kubernetes/blob/master/go.mod) supported by Fin.

Edit the following files.

- `go.mod`
- `Dockerfile`
- `README.md`

#### Depending tools

The following tools do not depend on other software, use the latest versions.
To change their versions, edit `versions.mk`.

- [helm](https://github.com/helm/helm/releases)
- [kustomize](https://github.com/kubernetes-sigs/kustomize/releases)
- [minikube](https://github.com/kubernetes/minikube/releases)
- [Rook](https://github.com/rook/rook/releases)
- [golangci-lint](https://github.com/golangci/golangci-lint/releases)

Some operators are described in `test/utils/utils.go`. Please check their versions and update them, if necessary. Note that it should use the LTS version for cert-manager.

- [prometheus-operator](https://github.com/prometheus-operator/prometheus-operator/releases)
- [cert-manager](https://github.com/cert-manager/cert-manager/releases)

Update the following version in Dockerfile, if necessary, too:

- custom rbd-export-diff

The Ceph version is described in the following file:

- `test/e2e/testdata/values-cluster.yaml`

#### Depending modules

Read Kubernetes's `go.mod`(https://github.com/kubernetes/kubernetes/blob/<upgrading Kubernetes release version\>/go.mod), and update the `prometheus/*` modules. Here is the example to update `prometheus/client_golang`.

```
$ VERSION=<upgrading prometheus-related libraries release version>
$ go get github.com/prometheus/client_golang@v${VERSION}
```

The following modules don't depend on other softwares, so use their latest versions:

```
go get \
    github.com/cespare/xxhash/v2@latest \
    github.com/google/uuid@latest \
    github.com/mattn/go-sqlite3@latest \
    github.com/onsi/ginkgo/v2@latest \
    github.com/onsi/gomega@latest \
    github.com/spf13/cobra@latest \
    github.com/stretchr/testify@latest \
    go.uber.org/mock@latest \
    golang.org/x/sys@latest \
    k8s.io/utils@latest \
    sigs.k8s.io/yaml@latest
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
