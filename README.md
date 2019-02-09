[![](https://img.shields.io/badge/license-Apache%202-blue.svg)]()
[![](https://img.shields.io/badge/python-3.7-blue.svg)]()
[![](https://img.shields.io/circleci/project/github/olitheolix/square/master.svg?style=flat)]()
[![](https://img.shields.io/codecov/c/github/olitheolix/square.svg?style=flat)]()
[![](https://img.shields.io/badge/status-dev-orange.svg)]()


Programmatically define and manage the state of a Kubernetes cluster.

# Examples
```bash
# Show help for commands.
square.py -h
square.py diff -h

# Download all deployment and namespace manifests into local YAML files.
square.py get ns deploy         # From all namespaces
square.py get ns deploy -n foo  # Limit operation to "foo" namespace

# Diff all local service manifests against those on the cluster.
square.py diff svc              # All namespaces
square.py diff svc -n foo       # Limit operation to "foo" namespace

# Patch K8s config maps to according to diffs with local YAML files.
square.py patch svc             # All namespaces
square.py patch svc -n foo      # Limit operation to "foo" namespace
```
