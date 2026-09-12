# Changelog

- [Changelog](#changelog)
  - [1.0.0](#100)
  - [0.4.0](#040)
  - [0.3.0](#030)
  - [0.2.0](#020)
  - [0.1.0](#010)

---

## 1.0.0

Released on 12/09/2026

- **BREAKING ‼️** Migrated to remotefs 1. `KubeContainerFs` and
  `KubeMultiPodFs` now implement `remotefs::AsyncRemoteFs` natively and no
  longer take a Tokio runtime in their constructors.
- **BREAKING ‼️** Every path must be an absolute POSIX path; `pwd` and
  `change_dir` are gone. `KubeMultiPodFs` mutating operations on `/`,
  `/pod`, and `/pod/container` fail with `PermissionDenied`, and `copy`,
  `rename`, and `symlink` require both paths in the same container.
- **BREAKING ‼️** `read_file` and `write_file` replace `open_file` and
  `create_file`, `rename` replaces `mov`, `set_metadata` replaces `setstat`,
  `exec` returns `ExecOutput`, and `KubeMultiPodFs::exec` is unsupported.
- Added owned `open`, `create`, and `append` streams over the pod exec
  stream, with native read offsets and lengths and explicit `finish`.
- Added `capabilities()`.
- Added the `tokio` feature with `into_blocking`, returning
  `BlockingKubeContainerFs` / `BlockingKubeMultiPodFs` for
  `remotefs::RemoteFs` callers.
- `WriteOptions::size_hint` is optional; when set it bounds `write_file` and
  is verified on `finish`.
- Dropped the `tar` and `tempfile` dependencies: uploads no longer buffer
  the whole file in memory.

## 0.4.0

Released on 30/09/2024

- remotefs 0.3.0

## 0.3.0

Released on 29/09/2024

- Added `KubeMultiPodFs` to operate on multiple pod and containers at the same time. See docs for details.
- **BREAKING ‼️** Renamed `KubeFs` to `KubeContainerFs`.

## 0.2.0

Released on 17/07/2024

- Added `container` to constructor to specify the container name

## 0.1.0

Released on 16/07/2024

- First release
