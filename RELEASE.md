# Release procedure

This document describes how to release a new version of Fin.

## Versioning

Follow [semantic versioning 2.0.0][semver] to choose the new version number.

## The format of release notes

In the release procedure, the release note is generated automatically,
and then it is edited manually. In this step, PRs should be classified based on [Keep a CHANGELOG](https://keepachangelog.com/en/1.1.0/).

The result should look something like:

```markdown
## What's Changed

### Added

* Add a notable feature for users (#35)

### Changed

* Change a behavior affecting users (#33)

### Removed

* Remove a feature, users action required (#39)

### Fixed

* Fix something not affecting users or a minor change (#40)
```

## Bump version

1. Determine a new version number by [checking the differences](https://github.com/cybozu-go/fin/compare/vX.Y.Z...main) since the last release. Then, define the `VERSION` variable.

    ```sh
    VERSION=1.2.3
    ```

2. Add a new tag and push it.

    ```sh
    git switch main
    git pull
    git tag v${VERSION}
    git push origin v${VERSION}
    ```

3. Once a new tag is pushed, [GitHub Actions][] automatically
   creates a draft release note for the tagged version.

   Visit https://github.com/cybozu-go/fin/releases to check
   the result.

4. Edit the auto-generated release note if necessary and publish it.

[semver]: https://semver.org/spec/v2.0.0.html
[GitHub Actions]: https://github.com/cybozu-go/fin/actions
