[![](https://img.shields.io/badge/license-Apache%202-blue.svg)]()
[![](https://img.shields.io/badge/python-3.7+-blue.svg)]()
[![](https://img.shields.io/badge/latest-v0.21.0-blue.svg)]()
[![](https://img.shields.io/circleci/project/github/olitheolix/square/master.svg?style=flat)]()
[![](https://img.shields.io/codecov/c/github/olitheolix/square.svg?style=flat)]()
[![](https://img.shields.io/badge/status-prod-green.svg)]()


Declarative state management of a Kubernetes cluster.

*Square* is to Kubernetes management what Terraform is to Cloud management.

# Installation
You can install *Square* in any Python 3.7+ environment with:

```console
foo@bar:~$ pip install kubernetes-square
foo@bar:~$ square version
0.21.0
```

Some pre-built binaries are available on the [Release
page](https://github.com/olitheolix/square/releases), or you can
[build your own](Building-A-Binary) for any version.

## Supported Clusters And Versions
*Square* supports standard configurations for Minikube, EKS and GKE. All tests
assume a `v1.13` cluster but we have successfully used it with all cluster
versions from `v1.9` to `v1.14`.

# Examples
*Square* will use the `KUBECONFIG` environment variable to locate the
Kubernetes credentials. Alternatively, you can specify the credentials with the
`--kubeconfig` and `--context` arguments.

## Get Current Cluster State
Download all _Namespace_- and _Deployment_ manifests from the cluster and save
them to `./manifests`:

```console
foo@bar:~$ kubectl apply -f integration-test-cluster/test-resources.yaml
...
foo@bar:~$ square get ns deployment --groupby ns kind --folder manifests/
foo@bar:~$ tree manifests
manifests/
├── default
│   └── namespace.yaml
├── _global_
│   └── clusterrole.yaml
├── kube-public
│   └── namespace.yaml
├── kube-system
│   ├── deployment.yaml
│   └── namespace.yaml
└── square-tests
    ├── deployment.yaml
    └── namespace.yaml
```

These are the YAML files from the [integration test
cluster](integration-test-cluster) (a Minikube). The `--groupby` argument
determine the layout of `manifests/`. In this case, each namespace becomes a
folder and the manifests are grouped by resource type. The only folder that
does not correspond to a Kubernetes namespace is `_global_` because it harbours
all non-namespaced resources like `ClusterRole` or `ClusterRoleBinding`.

The file names, as well as the manifest order inside those files are
irrelevant. *Square* will always compile them into a flat list internally. As
such, you are free to rename the files, or distribute their content across
multiple files. You can still use `square get ...` afterwards and *Square* will
update the right resources in the right files. If it finds a resource on the
server that is not yet defined in any of the files it will create the
corresponding file.

### Group By Label
*Square* can also use _one_ resource label and make it part of the manifests
foldershierarchy. Here is an example for the [integration test
cluster](integration-test-cluster):

```console
foo@bar:~$ kubectl apply -f integration-test-cluster/test-resources.yaml
...
foo@bar:~$ square get all --groupby ns label=app kind --folder manifests/
foo@bar:~$ tree manifests
manifests/
├── default
│   └── _other
│       ├── namespace.yaml
│       ├── secret.yaml
│       ├── serviceaccount.yaml
│       └── service.yaml
├── _global_
│   ├── demoapp
│   │   ├── clusterrolebinding.yaml
│   │   └── clusterrole.yaml
│   └── _other
│       ├── clusterrolebinding.yaml
│       └── clusterrole.yaml
├── kube-public
│   └── _other
│       ├── configmap.yaml
│       ├── namespace.yaml
│       ├── rolebinding.yaml
│       ├── role.yaml
│       └── serviceaccount.yaml
├── kube-system
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
└── square-tests
    ├── demoapp
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
        ├── secret.yaml
        └── serviceaccount.yaml
```

In this case, *Square* co-located all those resources that exist inside the
same namespace *and* have the same value for their `app` label. It put resources
without an `app` label into the catch-all folder `_other` and non-namespaced
resources into the `_global_` folder.

## Create The Update Plan
Following on with the example, the local files and the cluster state
are now in sync:

```console
foo@bar:~$ square plan ns
--------------------------------------------------------------------------------
Plan: 0 to add, 0 to change, 0 to destroy.
```

To make this more interesting, add a label to the _Namespace_ manifest in
`square-tests/demoapp/namespace.yaml`. It should something like this:
```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: default
  labels:
    foo: bar
spec:
  finalizers:
  - kubernetes
```

Save the file and create a plan:
```console
foo@bar:~$ square plan ns
Patch NAMESPACE default/default
    ---
    +++
    @@ -1,6 +1,8 @@
     apiVersion: v1
     kind: Namespace
     metadata:
    +  labels:
    +    foo: bar
       name: default
     spec:
       finalizers:
--------------------------------------------------------------------------------
Plan: 0 to add, 1 to change, 0 to destroy.
```

This will show the difference in standard `diff` format. In words: *Square*
would patch the `default` namespace to bring the K8s cluster back into sync
with the local files. Let's apply the plan to do just that:

```console
foo@bar:~$ square apply ns
Patch NAMESPACE default/default
    ---
    +++
    @@ -1,6 +1,8 @@
     apiVersion: v1
     kind: Namespace
     metadata:
    +  labels:
    +    foo: bar
       name: default
     spec:
       finalizers:

Compiled 1 patches.
Patch(url='https://192.168.0.177:8443/api/v1/namespaces/default', ops=[{'op': 'add', 'path': '/metadata/labels', 'value': {'foo': 'bar'}}])

foo@bar:~$ square plan ns
--------------------------------------------------------------------------------
Plan: 0 to add, 0 to change, 0 to destroy.
```

*Square* will first print the same diff we saw earlier already, followed by the
JSON patch it sent to K8s to update the _Namespace_ resource.

Use *kubectl* to ensure the patch worked and the name space now has a `foo:bar` label.

```console
foo@bar:~$ kubectl describe ns default
Name:         default
Labels:       foo=bar
Annotations:  <none>
Status:       Active

No resource quota.

No resource limits.

```

## Apply The Plan To Create and Destroy Resources
The `apply` operation we just saw is also the tool to create and delete
resources. To add a new one, simply add its manifest to `manifests/` (create a
new file or add it to an existing one). Then use *Square* to patch it.

For instance, to deploy the latest *Square* image from
[Dockerhub](https://hub.docker.com/r/olitheolix/square), download the [example
manifests](examples/square.yaml) into the `manifests/` folder and use *Square*
to deploy it:

```console
foo@bar:~$ wget https://github.com/olitheolix/square/raw/master/examples/square.yaml -O manifests/square.yaml
foo@bar:~$ square apply all
Create NAMESPACE square/square
    apiVersion: v1
    kind: Namespace
    metadata:
      name: square

Create SERVICEACCOUNT square/square
    apiVersion: v1
    kind: ServiceAccount
    metadata:
      name: square
      namespace: square

Create CLUSTERROLE None/square
    apiVersion: rbac.authorization.k8s.io/v1
    kind: ClusterRole
    metadata:
      name: square
    rules:
    - apiGroups:
      - ''
      - apps
      - rbac.authorization.k8s.io
      - extensions
      resources:
      - clusterrolebindings
      - clusterroles
      - configmaps
      - daemonsets
      - deployments
      - ingresses
      - namespaces
      - persistentvolumeclaims
      - rolebindings
      - roles
      - secrets
      - services
      - statefulsets
      verbs:
      - get
      - list
      - update
      - patch

Create CLUSTERROLEBINDING None/square
    apiVersion: rbac.authorization.k8s.io/v1
    kind: ClusterRoleBinding
    metadata:
      name: square
    roleRef:
      apiGroup: rbac.authorization.k8s.io
      kind: ClusterRole
      name: square
    subjects:
    - kind: ServiceAccount
      name: square
      namespace: square

Create DEPLOYMENT square/square
    apiVersion: extensions/v1beta1
    kind: Deployment
    metadata:
      name: square
      namespace: square
    spec:
      replicas: 1
      selector:
        matchLabels:
          app: square
      template:
        metadata:
          labels:
            app: square
        spec:
          containers:
          - command:
            - sleep
            - 10000d
            image: olitheolix/square:latest
            imagePullPolicy: Always
            name: square
          serviceAccountName: square
          terminationGracePeriodSeconds: 1

Creating NAMESPACE square/square
Creating SERVICEACCOUNT square/square
Creating CLUSTERROLE None/square
Creating CLUSTERROLEBINDING None/square
Creating DEPLOYMENT square/square
Compiled 0 patches.

foo@bar:~$ kubectl -n square get po
NAME                     READY   STATUS    RESTARTS   AGE
square-b6bc65f6d-2xmzm   1/1     Running   0          37s
```

# Building A Binary
Build a shared library version of Python version with `pyenv`:

```console
# Build a new Python distribution with shared libraries.
foo@bar:~$ pipenv --rm
foo@bar:~$ env PYTHON_CONFIGURE_OPTS="--enable-shared" pyenv install 3.7.4
foo@bar:~$ pyenv local 3.7.4
foo@bar:~$ pip install pipenv

# Install pyinstaller and run it.
foo@bar:~$ pipenv install pyinstaller==3.6
foo@bar:~$ pipenv run pyinstaller -n square square/__main__.py
foo@bar:~$ pipenv run pyinstaller square.spec --onefile
```

This should produce the `./dist/square` executable.


# Deploy On A Cluster
*Square* does not require anything installed on your cluster to work. However,
it will require the appropriate RBACs if you want to run it in a Pod. The
[examples folder](examples) contains an example on how to deploy the
[official Docker image](https://hub.docker.com/r/olitheolix/square).

This can be useful for automation tasks. For instance, you may want to
track configuration drift of your cluster over time.

# Use It As A Library
You can also use *Square* as a library in your own projects. See
[here](examples/as_library.py) for an example.

# Automated Tests
*Square* ships with a comprehensive set of unit test as well as a small set of
integration tests.

    pipenv run pytest

This will automatically run the integration tests as well if you have started
the [KIND](https://github.com/bsycorp/kind) cluster with these commands first:

    cd integration-test-cluster
    ./start_cluster.sh

NOTE: currently, CI runs the unit tests only.

# Development Status
We have been using *Square* for our production workloads for some time now and
deem it stable.
