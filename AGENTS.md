# Repository Guidelines

This repository contains the `rathole` reverse proxy (Rust). Use these notes to keep contributions focused, consistent, and safe.

## Rule
- 请一直使用简体中文交互

## Project Structure & Module Organization
- Core source lives in `src/` (`server.rs`, `client.rs`, `transport/`, `config*.rs`, `health.rs`). CLI entry is in `src/main.rs`.
- Integration and transport fixtures are in `tests/` (e.g., `tests/for_tcp`, `tests/for_udp`, `integration_test.rs`); benches reside in `benches/`.
- Examples and service templates sit in `examples/` (systemd, sample configs); docs and build notes are under `docs/` (see `docs/build-guide.md`, `docs/transport.md`).
- Packaging/ops assets: `Dockerfile`, `build.rs`, `make.sh`, and release artifacts in `releases/`.

## Build, Test, and Development Commands
- Release build (default server features): `cargo build --release`
- Client build: `cargo build --release --no-default-features --features client,noise`
- Full feature build (server+client+rustls+ws+hot-reload):\
  `cargo build --release --no-default-features --features server,client,rustls,noise,websocket-rustls,hot-reload`
- Format/lint: `cargo fmt --all` and `cargo clippy --all-targets --all-features -D warnings`
- Tests (enable both client/server to exercise integrations): `cargo test --all-features`\
  TLS websocket tests are skipped on macOS due to self-signed cert prompts.

## Coding Style & Naming Conventions
- Rustfmt defaults (4-space indent). Keep modules focused: protocol in `protocol.rs`, config parsing in `config.rs`, platform helpers in `helper.rs`.
- Prefer explicit feature gates for transport variants (`native-tls` vs `rustls`, websocket flags).
- Naming: configs use snake_case keys; binaries and targets use hyphen-free names (`rathole`).

## Testing Guidelines
- Favor async tests with `tokio::test` (existing pattern in `tests/integration_test.rs`); reuse helpers in `tests/common/`.
- Add config fixtures under `tests/for_tcp` or `tests/for_udp` when introducing new transport paths.
- Aim to keep integration coverage runnable with `cargo test --all-features`; note macOS TLS limitations when adding new cases.

## Commit & Pull Request Guidelines
- Commit messages are short and imperative (historically concise, sometimes in Chinese). Include scope and behavior change, e.g., `Add UDP retry backoff`.
- For PRs: describe intent, feature flags touched, and config changes; link issues when relevant. Provide repro steps and test commands executed. Include screenshots/log snippets for user-facing behavior or transport-level changes.

## Security & Configuration Tips
- Tokens are mandatory per service; avoid committing real secrets. Keep sample configs under `examples/` or `tests/`.
- When toggling TLS/Noise/WebSocket, document the expected feature flags and config keys in PRs and update `docs/transport.md` if semantics shift.
