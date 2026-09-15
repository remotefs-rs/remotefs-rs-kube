# Changelog

All notable changes to this project are documented in this file.

## 1.0.0

Released on 2026-09-15

### Breaking changes

- migrate to remotefs 1

> requires remotefs 1; constructors, path handling, and transfer methods changed.

### Added

- Breaking: migrate to remotefs 1

> KubeContainerFs and KubeMultiPodFs now implement remotefs::AsyncRemoteFs natively and no longer take a Tokio runtime. Every path must be an absolute POSIX path; pwd and change_dir are gone. open, create, and append return owned streams over the pod exec stream that must be finished explicitly; read_file and write_file replace open_file and create_file, rename replaces mov, set_metadata replaces setstat, and exec returns ExecOutput. Read offsets and lengths are honored natively and capabilities() is advertised. Blocking callers enable the tokio feature and call into_blocking to get a BlockingKubeContainerFs or BlockingKubeMultiPodFs implementing remotefs::RemoteFs.

### Fixed

- test is sync and send

## 0.4.0

Released on 2024-09-30

### Added

- remotefs 0.3

### Fixed

- bump version

## 0.3.0

Released on 2024-09-29

### Added

- first commit
- added container name
- multi pod (#1)

> - feat: multi pod client
> - fix: tests and fixes
> - fix: version
> - fix: changelog

### Fixed

- working on tests
- io
- changelog
- ci
- ci
