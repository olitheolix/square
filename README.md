[![](https://img.shields.io/badge/license-Apache%202-blue.svg)]()
[![](https://img.shields.io/badge/python-3.10,3.11-blue.svg)]()
[![](https://img.shields.io/badge/latest-v2.0.1-blue.svg)]()
[![](https://github.com/olitheolix/square/workflows/build/badge.svg)]()
[![](https://img.shields.io/codecov/c/github/olitheolix/square.svg?style=flat)]()


*Square* is a tool to reconcile local manifests to/from a Kubernetes
cluster.

It facilitates a lightweight and CLI based GitOps workflow, but developers can
also use it as library to drive the Kubernetes state at scale.

It is important to understand that *Square* is neither a version manager, nor a
package manager nor a render engine nor an authoring tool for manifests. Its
sole purpose is to show the difference between the manifests on the local file
system vs those on a cluster, and reconcile the difference in either direction.
The plan/apply workflow will be familiar to Terraform users.

*Square* is safe to use alongside other tools like *Helm*, *Kustomize* or
*kubectl* since it never modifies any manifests it touches. This includes the
`app.kubernetes.io/managed-by` annotation. Similarly, it does not create any
`Secrets` or other resource to track its own state because it has no state.


# Installation
Grab a [binary release](https://github.com/olitheolix/square/releases) or
use `pip` to install it into a Python 3.10+ environment:

```console
foo@bar:~$ pip install kubernetes-square --upgrade
foo@bar:~$ square version
2.0.1
```

# Quickstart
Run `square config` to create a `.square.yaml` and open it in an editor. It
comes with sensible defaults but you probably want to double check the values
for `kubeconfig` and `kubecontext` near the top of the file. You can also tweak
the `selectors.kinds`, `selectors.labels` and `selectors.namespaces` to target
specific resources, labels and namespaces.

A typical *Square* workflow is this:

```console
# Create a `.square.yaml` configuration file and adjust at least the `kubeconfig` value.
square config

# Download all resources that match the selectors in the `.square.yaml` file.
square get

# Show the differences and exit.
square plan

# Show the differences and give user the option to reconcile it.
square apply
```

Command line arguments take precedence over the values defined in `.square.yaml`:

```console
# Plan the resources based on `.square.yaml`.
square plan

# Limit the plan to the `default` and `foo` namespaces.
square plan -n default foo

# Limit the plan to ConfigMaps and Deployments only.
square plan configmap deployment

# Limit the plan to resources that have the `app=foo` label.
square plan -l app=foo

# Limit plan to all deployments called `foo`.
square plan deploy/foo

# Mix and match several options.
square plan configmap deploy/foo -n default -l app=foo
```

These commands will also work with the `square apply` command.


# Example Workflow
This section shows how to use *Square* to import resources from a cluster as
well as plan and apply changes.

## Setup
Assuming you have a valid Kubeconfig you can setup the demo as follows:

```console
# Deploy the demo resources with `kubectl`. We are not using Square here
# to demonstrate how to import existing resources with it.
foo@bar:~$ kubectl apply -f integration-test-cluster/test-resources.yaml
...

# Create a new folder for the experiment.
foo@bar:~$ mkdir try_square
foo@bar:~$ cd try_square

# Creata a vanilla Square configuration file `.square.yaml`.
foo@bar:~$ square config
Created configuration file <.square.yaml>.
Please open the file in an editor and adjust the values, most notably `kubeconfig` and `selectors.[kinds | namespaces | labels]`.

# Open `.square.yaml` in your editor and change the values for `kubeconfig` and
# (optionally) `kubecontext` to point to your Kubeconfig for your cluster.
foo@bar:~$ emacs .square.yaml
...

```

## Get Current Cluster State
Download all `Namespace`- and `Deployment` manifests from the cluster and save
them to `./manifests`:

```console
# Import all the resources specified in `.square.yaml`.
foo@bar:~$ square get --groupby ns kind
foo@bar:~$ tree
.
└── manifests
    ├── default
    │   ├── configmap.yaml
    │   ├── namespace.yaml
    │   └── service.yaml
    ├── _global_
    │   ├── clusterrolebinding.yaml
    │   ├── clusterrole.yaml
    │   └── customresourcedefinition.yaml
    ├── kube-node-lease
    │   ├── configmap.yaml
    │   └── namespace.yaml
    ├── kube-public
    │   ├── configmap.yaml
    │   ├── namespace.yaml
    │   ├── rolebinding.yaml
    │   └── role.yaml
    ├── kube-system
    │   ├── configmap.yaml
    │   ├── daemonset.yaml
    │   ├── deployment.yaml
    │   ├── namespace.yaml
    │   ├── rolebinding.yaml
    │   ├── role.yaml
    │   ├── secret.yaml
    │   ├── serviceaccount.yaml
    │   └── service.yaml
    ├── local-path-storage
    │   ├── configmap.yaml
    │   ├── deployment.yaml
    │   ├── namespace.yaml
    │   └── serviceaccount.yaml
    ├── square-tests-1
    │   ├── configmap.yaml
    │   ├── cronjob.yaml
    │   ├── daemonset.yaml
    │   ├── deployment.yaml
    │   ├── horizontalpodautoscaler.yaml
    │   ├── ingress.yaml
    │   ├── namespace.yaml
    │   ├── persistentvolumeclaim.yaml
    │   ├── poddisruptionbudget.yaml
    │   ├── rolebinding.yaml
    │   ├── role.yaml
    │   ├── secret.yaml
    │   ├── serviceaccount.yaml
    │   ├── service.yaml
    │   └── statefulset.yaml
    └── square-tests-2
        ├── configmap.yaml
        ├── cronjob.yaml
        ├── daemonset.yaml
        ├── deployment.yaml
        ├── horizontalpodautoscaler.yaml
        ├── ingress.yaml
        ├── namespace.yaml
        ├── persistentvolumeclaim.yaml
        ├── rolebinding.yaml
        ├── role.yaml
        ├── secret.yaml
        ├── serviceaccount.yaml
        ├── service.yaml
        └── statefulset.yaml

9 directories, 54 files
```

The `--groupby` determines the layout of the `manifests/` folder. In
this case, Square created one folder for each namespace and grouped the
resources type within those folders. Non-namespaced resources, like
`ClusterRole` will be in the `_global_` folder.

It is important to note that Square does not attach any meaning to the
directory layout. Internally, it will compile all manifests into a flat list.
You can therefore rename and move files as you see fit, and even move
individual manifests between files. Furthermore, `square get` is smart
enough to update the right manifests in the right files.

### Group By Label
*Square* can also group manifests based on a __single_ label. For instance,
here are the [integration test cluster](integration-test-cluster) manifests
grouped by `Namespace` and the `app` label:

```console
foo@bar:~$ rm -rf manifests
foo@bar:~$ square get --groupby ns label=app kind
foo@bar:~$ tree
.
└── manifests
    ├── default
    │   └── _other
    │       ├── configmap.yaml
    │       ├── namespace.yaml
    │       └── service.yaml
    ├── _global_
    │   ├── demoapp-1
    │   │   ├── clusterrolebinding.yaml
    │   │   ├── clusterrole.yaml
    │   │   └── customresourcedefinition.yaml
    │   └── _other
    │       ├── clusterrolebinding.yaml
    │       └── clusterrole.yaml
    ├── kube-node-lease
    │   └── _other
    │       ├── configmap.yaml
    │       └── namespace.yaml
    ├── kube-public
    │   └── _other
    │       ├── configmap.yaml
    │       ├── namespace.yaml
    │       ├── rolebinding.yaml
    │       └── role.yaml
    ├── kube-system
    │   ├── kindnet
    │   │   └── daemonset.yaml
    │   ├── kube-proxy
    │   │   └── configmap.yaml
    │   └── _other
    │       ├── configmap.yaml
    │       ├── daemonset.yaml
    │       ├── deployment.yaml
    │       ├── namespace.yaml
    │       ├── rolebinding.yaml
    │       ├── role.yaml
    │       ├── secret.yaml
    │       ├── serviceaccount.yaml
    │       └── service.yaml
    ├── local-path-storage
    │   └── _other
    │       ├── configmap.yaml
    │       ├── deployment.yaml
    │       ├── namespace.yaml
    │       └── serviceaccount.yaml
    ├── square-tests-1
    │   ├── demoapp-1
    │   │   ├── configmap.yaml
    │   │   ├── cronjob.yaml
    │   │   ├── daemonset.yaml
    │   │   ├── deployment.yaml
    │   │   ├── horizontalpodautoscaler.yaml
    │   │   ├── ingress.yaml
    │   │   ├── namespace.yaml
    │   │   ├── persistentvolumeclaim.yaml
    │   │   ├── rolebinding.yaml
    │   │   ├── role.yaml
    │   │   ├── secret.yaml
    │   │   ├── serviceaccount.yaml
    │   │   ├── service.yaml
    │   │   └── statefulset.yaml
    │   └── _other
    │       ├── configmap.yaml
    │       ├── poddisruptionbudget.yaml
    │       └── secret.yaml
    └── square-tests-2
        ├── demoapp-1
        │   ├── configmap.yaml
        │   ├── cronjob.yaml
        │   ├── daemonset.yaml
        │   ├── deployment.yaml
        │   ├── horizontalpodautoscaler.yaml
        │   ├── ingress.yaml
        │   ├── namespace.yaml
        │   ├── persistentvolumeclaim.yaml
        │   ├── rolebinding.yaml
        │   ├── role.yaml
        │   ├── secret.yaml
        │   ├── serviceaccount.yaml
        │   ├── service.yaml
        │   └── statefulset.yaml
        └── _other
            ├── configmap.yaml
            └── secret.yaml

22 directories, 62 files
```

Note that resources without an `app` label are in the catch-all folder `_other`.

## Create A Plan
The plan is currently clean because we just imported the manifests from the
cluster. To very:

```console
foo@bar:~$ square plan
--------------------------------------------------------------------------------
Plan: 0 to add, 0 to change, 0 to destroy.
```

To make this more interesting we will add a `foo` label to the `Namespace` manifest in
`manifests/square-tests-1/demoapp-1/namespace.yaml`. It should look something like this:
```yaml
apiVersion: v1
kind: Namespace
metadata:
  labels:
    app: demoapp-1
    foo: bar
    kubernetes.io/metadata.name: square-tests-1
  name: square-tests-1
spec:
  finalizers:
  - kubernetes
```

Now the plane will show a difference between the local and server manifests:
```console
foo@bar:~$ square plan ns
Patch NAMESPACE None/square-tests-1 (v1)
    ---
    +++
    @@ -3,6 +3,7 @@
     metadata:
       labels:
         app: demoapp-1
    +    foo: bar
         kubernetes.io/metadata.name: square-tests-1
       name: square-tests-1
     spec:

--------------------------------------------------------------------------------
Plan: 0 to add, 1 to change, 0 to destroy.
```

We could now use `square get ns` to make the local manifest match the cluster
state or `square apply ns` to make the cluster state match the local manifests.
We will do the latter:

```console
foo@bar:~$ square apply ns
Patch NAMESPACE None/square-tests-1 (v1)
    ---
    +++
    @@ -3,6 +3,7 @@
     metadata:
       labels:
         app: demoapp-1
    +    foo: bar
         kubernetes.io/metadata.name: square-tests-1
       name: square-tests-1
     spec:

--------------------------------------------------------------------------------
Plan: 0 to add, 1 to change, 0 to destroy.

Type yes to apply the plan.
  Your answer: yes

Patching NAMESPACE None/square-tests-1
```

We can use *kubectl* to verify that the `square-tests-1` Namespace now has the `foo=bar` label:

```console
foo@bar:~$ kubectl describe ns square-tests-1
Name:         default
Labels:       foo=bar
Annotations:  <none>
Status:       Active

No resource quota.

No resource limits.

```

The plan is now clean again because the local files and the
cluster are in sync again:

```console
foo@bar:~$ square plan ns
--------------------------------------------------------------------------------
Plan: 0 to add, 0 to change, 0 to destroy.
```

## Create and Destroy Resources
So far we have only imported and modified existing resource but `square apply`
will also create and remove resources as necessary. For instance, to add a new
resource we add its manifest to the `manifests/` folder, either in a new file
or added to an existing one.

```console
foo@bar:~$ cat > manifests/additional_manifest.yaml <<EOF
apiVersion: v1
kind: Namespace
metadata:
  name: dummy
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: random-user
  namespace: dummy
EOF

foo@bar:~$ square apply
Create NAMESPACE None/dummy (v1)
    apiVersion: v1
    kind: Namespace
    metadata:
      name: dummy

Create SERVICEACCOUNT dummy/random-user (v1)
    apiVersion: v1
    kind: ServiceAccount
    metadata:
      name: random-user
      namespace: dummy

--------------------------------------------------------------------------------
Plan: 2 to add, 0 to change, 0 to destroy.

Type yes to apply the plan.
  Your answer: yes

Creating NAMESPACE None/dummy
Creating SERVICEACCOUNT dummy/random-user
```

Similarly, to remove those resources again we merely delete the manifest file
and run `square apply` again:

```console
foo@bar:~$ rm manifests/additional_manifest.yaml
foo@bar:~$ square apply
Delete CONFIGMAP dummy/kube-root-ca.crt (v1)
Delete NAMESPACE None/dummy (v1)
Delete SERVICEACCOUNT dummy/random-user (v1)
--------------------------------------------------------------------------------
Plan: 0 to add, 0 to change, 3 to destroy.

Type yes to apply the plan.
  Your answer: yes

Deleting SERVICEACCOUNT dummy/random-user
Deleting CONFIGMAP dummy/kube-root-ca.crt
Deleting NAMESPACE None/dummy
```

# Use It As A Library
You can also use *Square* as a library. In fact, the CLI commands explained
here are just thin wrappers around that library. See
[here](examples/basic_workflow.py) for how to build a basic version of the CLI with
only a few lines of code.

# Automated Tests
*Square* ships with a comprehensive set of unit tests:

    pipenv run pytest

To run the integration tests as well you need to have
[KinD](https://github.com/kubernetes-sigs/kind/releases) and start it with:

    cd integration-test-cluster
    ./start_cluster.sh
