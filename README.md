A proof-of-concept for how to (maybe) manage Kubernetes state.

# Intended Usage

The emphasis here is on `intended`.

```bash
# Get help.
square.py -h
square.py diff -h

# Download all deployments and namespaces into a YAML file.
square.py get deploy ns

# Diff the local deployment manifests against those on K8s cluster.
square.py diff deploy

# Patch K8s deployments to match those specified in local YAML files.
square.py patch deploy
```
