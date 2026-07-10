# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

*Square* (`kubernetes-square` on PyPI) is a stateless CLI + library that reconciles
local Kubernetes manifests against a live cluster in either direction — a Terraform-like
plan/apply workflow for raw manifests. It never mutates manifests it touches, adds no
`managed-by` annotations, and stores no state on the cluster. It coexists with Helm,
Kustomize, and kubectl.

The CLI is a thin wrapper around the library; `square/main.py` parses args and calls into
`square/square.py`. `square/__init__.py` re-exports the primary API (`get`, `plan`,
`apply_plan`, `show_plan`).

## Commands

Requires Python 3.13+. Uses `uv` for everything.

```bash
uv sync                       # install deps (incl. dev group)
uv run pytest                 # run unit + integration tests
uv run pytest tests/test_square.py                    # single file
uv run pytest tests/test_square.py::test_name         # single test
uv run pytest --ignore=tests/test_integration.py      # skip integration tests (as CI does off-Linux)
uv run pytest --cov=square --cov-report=term-missing --cov-fail-under=100  # coverage gate

uv tool run ruff check        # lint
uv tool run ruff format --check   # format check (drop --check to apply)
uv tool run ty check          # static type check (astral `ty`, not mypy)
```

**Coverage must stay at 100%** — CI fails below that (`--cov-fail-under=100`, Linux job).
Every new branch needs a test. Use `# codecov-skip` on lines that genuinely cannot be
covered (see `[tool.coverage.report].exclude_lines`).

Integration tests are gated on a running KinD cluster (skipped via `kind_available()` in
`tests/test_helpers.py` otherwise). To run them:

```bash
cd integration-test-cluster && ./start_cluster.sh   # needs KinD + kubectl installed
# writes kubeconfig to /tmp/kubeconfig-kind.yaml
```

## Architecture

Four CLI verbs (`get`, `plan`, `apply`, plus `init`/`version`) all flow through the same
pipeline. The key modules:

- **`square/square.py`** — orchestration / business logic. `make_plan` → `compile_plan`
  produces a `DeploymentPlan` (create / patch / delete deltas); `apply_plan` executes it;
  `get_resources` downloads and syncs to disk. `make_patch` computes JSON patches.
- **`square/manio.py`** — manifest I/O and the local↔server reconciliation core. `sync`
  merges server state into the local file layout; `compile_square_manifests` flattens files
  into the internal keyed form and rejects duplicate resources; `strip_manifests` /
  `diff` / `save` / `load_manifests` handle the rest.
- **`square/k8s.py`** — all cluster interaction: httpx client + SSL setup, kubeconfig
  loading (standard, in-cluster, minikube, KinD, external authenticator commands),
  API endpoint discovery (`compile_api_endpoints`, `pick_api`), and the async
  `get/post/patch/delete` verbs (retried via `tenacity`).
- **`square/dtypes.py`** — Pydantic models and `NamedTuple`s. `Config` is the central type
  (mirrors `.square.yaml`); `MetaManifest` (apiVersion/kind/namespace/name) is the identity
  key for a resource everywhere internally.
- **`square/main.py`** — argparse CLI, config compilation (CLI args override `.square.yaml`),
  user confirmation for `apply`.
- **`square/cfgfile.py`** / **`square/yaml_io.py`** — load/parse `.square.yaml` and YAML.
- **`square/manio_filters.py`** — apply the `filters` (JSONPath field exclusions).

### Core data representations

Internally manifests live in two shapes (defined in `dtypes.py`):
- `SquareManifests = Dict[MetaManifest, dict]` — flat, keyed by resource identity (server side).
- `LocalManifestLists = Dict[Path, List[Tuple[MetaManifest, dict]]]` — grouped by file, since
  one YAML file may hold many documents (local side).

Square attaches **no meaning to the directory layout** — files/manifests can be freely moved
between files. `--groupby` (e.g. `ns label=kind`) only controls how *new* manifests are placed
on import; everything is compiled to a flat list internally.

### Selectors & filters

- **Selectors** (`kinds`, `labels`, `namespaces`) restrict which resources any command
  operates on. Positional CLI args like `deploy/foo` and `-n`/`-l` refine them.
- **Filters** (`Config.filters`, JSONPath) strip fields (e.g. `.status`, `.metadata.uid`)
  so they never enter the diff or the saved manifests. Defaults live in
  `square/resources/defaultconfig.yaml`.

### Callbacks (library extensibility)

`Config` carries two user-overridable callbacks (`square/callbacks.py`):
- `strip_callback` — shapes each manifest on import / before diffing (what lands on disk).
- `patch_callback` — invoked per resource before computing a patch; return identical
  local+server manifests to exclude a resource from patching. **Only modify the local
  manifest**; altering the server manifest can produce patches K8s rejects.

## Conventions

- **Error handling:** functions return `Tuple[result, bool]` where the trailing `bool` is an
  error flag (`True` == error). Check it; don't rely on exceptions. This pattern is pervasive.
- **Async:** cluster I/O is `async` (httpx + asyncio); `main.py` drives it via `asyncio.run`.
- Everything is fully type-annotated and must pass `ty check`.
- Version bumps use `bump-my-version` (config in `pyproject.toml`, updates `__init__.py`,
  `README.md`, `pyproject.toml`).
- **Code Philosophy**: If it's not tested it's broken.

## Distribution

Ships as a PyPI package (`square` entry point), a PyInstaller single-file binary
(`square.spec`, built in CI for Linux/macOS/Windows on tag push), and a Docker image
(`python -m square` entrypoint). Note `square/__init__.py` forces the multiprocessing
`fork` start method on Linux (Python 3.14 `forkserver` workaround).
