# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with
code in this repository.

`AGENTS.md` is a symlink to this file and holds the same agent contract.

## Commands

Every task runs through a [`just`](https://just.systems) recipe. Do not bypass a
recipe with an ad hoc `cargo` or tool command. If a recurring task has no
recipe, add one under `just/` before using it. Run `just` to list all recipes.

```sh
just build                 # cargo build --all-targets
just release               # release build
just test                  # cargo test --lib, then --doc
just integration           # cluster-backed integration tests
just minikube_up           # start the Minikube cluster
just minikube_down         # stop the Minikube cluster
just coverage              # cargo llvm-cov, writes lcov.info
just fmt                   # dprint fmt (Markdown, Rust, TOML, YAML)
just fmt_check             # dprint check
just lint "-- -D warnings" # alias of just clippy
just doc                   # cargo doc --no-deps with RUSTDOCFLAGS="-D warnings"
just deny                  # cargo deny check
just scan_secrets          # trufflehog filesystem
just check                 # the full local quality gate
just setup_githooks        # point core.hooksPath at .githooks
just changelog_preview 0.4.0
just changelog 0.4.0
just publish "--dry-run --allow-dirty"
```

`just check` is the required gate before declaring work done. It chains
`fmt_check`, Clippy with warnings denied, `doc`, `deny`, and `test`.

The tests behind the `integration-tests` feature need a live Kubernetes
cluster reachable via the local kubeconfig and the `MINIKUBE_IP` environment
variable pointing at it. Start one with `just minikube_up`, then run
`just integration` with `MINIKUBE_IP` set to the cluster's IP
(`minikube ip`). Plain unit and doc tests need neither.

If a required tool is missing, say so. Never claim a check passed or silently
swap in a weaker command.

## Architecture

remotefs-kube is a [remotefs](https://github.com/remotefs-rs/remotefs-rs)
client implementation providing access to Kubernetes pods and containers. It
is a library-only crate (`src/lib.rs`, crate name `remotefs_kube`) with no
binaries or examples.

- **Two async clients.** `KubeContainerFs` (`src/kube_container_fs.rs`)
  implements `remotefs::AsyncRemoteFs` over a single container's filesystem.
  `src/kube_container_fs/exec.rs` (`KubeExec`) runs `/bin/sh -c` commands
  through `kube`'s `AttachedProcess`; `src/kube_container_fs/stream.rs`
  wraps a `cat`/`tail`/`head` process into owned `AsyncRemoteRead` /
  `AsyncRemoteWrite` streams whose `finish` waits for the remote exit status.
  `KubeMultiPodFs` (`src/kube_multipod_fs.rs`) exposes a namespace as
  `/pod/container/...` (parsed by `src/kube_multipod_fs/path.rs`) and
  delegates to a throw-away `KubeContainerFs::attached` per call, so it has
  no mutable per-call state. The `tokio` feature adds `into_blocking`
  (`remotefs::adapters::blocking::BlockOn`) for blocking `RemoteFs` callers.
- **No native protocol, no working directory.** Kubernetes has no remote
  filesystem API, so both clients shell out to POSIX utilities inside the
  target container. Every path is validated by
  `utils::path::ensure_posix_absolute` before the connection check and
  quoted with `utils::path::shell_quote`. `src/utils/parser.rs` and
  `src/utils/fmt.rs` parse `ls -la` output back into `remotefs::File`
  entries.
- **Command layer.** `Justfile` is a thin importer. Each recipe group lives in
  its own file under `just/` (`build`, `test`, `code_check`, `changelog`,
  `publish`) and carries a `[group(...)]` attribute so `just --list` stays
  organized. Recipes take an `args=""` passthrough rather than hard-coding
  flags.
- **Formatting is dprint, not cargo fmt.** `dprint.json` owns Markdown, TOML,
  and YAML, and delegates `.rs` files to nightly rustfmt through its exec
  plugin (`--edition 2024`, matching this crate's `package.edition`).
  `rustfmt.toml` uses nightly-only options (`imports_granularity`,
  `group_imports`), which is why nightly is required. Always format with
  `just fmt`.
- **Release path.** Commits follow Conventional Commits and `cliff.toml` turns
  them into `CHANGELOG.md`. Publishing goes through `just publish`
  (`cargo publish --locked`); version bumps live in `Cargo.toml`.
- **Supply-chain policy.** `deny.toml` is strict: license allowlist,
  `yanked = "deny"`, `unmaintained = "all"`, wildcard versions denied, and
  crates.io as the only allowed source. It runs with `all-features = true`.
- **CI only runs the integration-test suite on Linux.**
  `.github/workflows/ci.yml`'s `quality-macos` and `quality-windows` jobs
  build, lint, and run the cluster-free tests; `quality-linux` starts a
  Minikube cluster, runs the `integration-tests` suite, and uploads coverage.

## Conventions

- Toolchain is pinned to Rust 1.98.0 (`rust-toolchain.toml`). `package.edition`
  in `Cargo.toml` is 2024 and `package.rust-version` matches it; do not bump
  either as part of unrelated changes.
- Public library items need canonical rustdoc, including a runnable example.
  `just test` runs doctests, and `just doc` denies warnings.
- Keep `Cargo.toml` dependency and feature entries alphabetically sorted, with
  bare minimal versions.
- Conventional Commits, imperative and lower-case. No agent attribution,
  session links, or agent `Co-Authored-By` lines.
- Do not stage planning state. `docs/superpowers/`, `.superpowers/`, and
  `.claude/plans/` are gitignored and dprint-excluded.
- After editing a Markdown file that contains a table, run
  `fmt-md-tables -i <file>`.
- After any change under `.github/workflows/`, run `zizmor .github/workflows`
  until it exits clean. Pin actions to a full commit SHA with the matching tag
  in a trailing comment, declare least-privilege permissions, and set
  `persist-credentials: false` on checkout.
