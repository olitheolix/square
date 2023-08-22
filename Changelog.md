# Changelog

## v2.0.0rc1 (2023-08-22)
### Changes
* feat(tenacity): replace `backoff` with `tenacity`.

## v2.0.0rc0 (2023-08-19)
### Highlights
Square is now an async library and interrogates the K8s API concurrently.

This change also makes it easier to manage multiple clusters simultaneously.
Please refer to the new examples for how to use this in your own projects.

There are currently no plans to expose any multi-cluster features via the CLI.

This release also ships extensive internal changes, particularly with regard to
selecting and filtering manifests. These changes will pave the way for more
user defined callback functions in the future.

### Breaking Changes
* The main entry points into Square, ie `get`, `plan`, `apply_plan`,
  `show_plan` are now coroutines (their signature remains the same).

### Changes
* bugfix(log): log correct context name in error message.
* bugfix(logging): `setup_logging` is now idempotent.
* deps(pytest): install pytest-asyncio
* doc: improve documentation of square config file.
* example: add `import_multiple_clusters.py`
* feat(async): make square async.
* feat(async): use concurrent requests in `manio.download`.
* feat(callback): support user defined cleanup callback.
* feat(ruff): add basic config section for ruff linter.

## v1.5.1 (2023-08-02)
### Changes
* bugfix(plan): plan patch even if labels match only local or cluster.
* ci(examples): run examples in github action.
* deps(pyinstaller): omit specific version when building in ci.
* deps(pyinstaller): upgrade pyinstaller from v5.9 to v5.13
* doc(example): clean up `as_library.py`.
* doc(readme): add multi-namespace example to readme.
* doc(readme): remove legacy docu from "examples/readme.md".
* doc(test): improve doc strings.
* feat(k8s): use kubernetes 1.26 in the integration tests.
* test(kind): change configmap label in kind test cluster.
* test(labels): add label based integration tests.
* test(labels): cover more label related use cases in integration tests.
* test(make_plan): additional tests for `manio.make_plan`.
* test(plan): verify that "make_plan" copes with kind/name selection.

## v1.5.0 (2023-07-29)
### Changes
* bugfix(conf): load_eks_config does not expect command arguments anymore.
* deps(pip): install httpx
* deps(pip): remove the now unused `google` libraries.
* deps(pip): replace `requests` with `httpx`.
* deps(poetry): remove the now unused `google` libraries.
* deps(poetry): replace `requests` with `httpx`.
* doc(httpx): remove legacy references to `requests` sessions.
* feat(auth): use default auth path for gke clusters as well.
* feat(httpx): replace requests with httpx

## v1.4.0 (2023-07-22)
### Changes
* ci(docker): add github action to build square in docker.
* ci(pycodestyle): run pycodestyle in github action.
* deps(poetry): routine upgrade.
* deps: add version requirements for pydantic and requests.
* doc(examples): improve docu in examples/*
* doc(readme): remove legacy sections (eg dockerhub) and some tlc.
* feat(config): improve defaults in ".square.yaml" file.
* feat(filters): add dedicated definition for "filters".
* feat(pydantic): switch from dataclasses to pydantic models.
* feat(pydantic): use new "model_validate" method to load `config` model.
* feat(select): `manio.download` now works with kind/name selectors.
* feat(select): `manio.select` can now select based on kind/name.
* feat(select): add "_kinds_names" and "_kinds_only" to `selectors`.
* feat(typing): improve type annotations for `cfgfile.valid`.
* feat(typing): improve type annotations in `dotdict`.
* feat(typing): improve type of `localmanifestlists`.
* feat(yaml): fold multi-line strings in yaml output.
