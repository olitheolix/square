[![](https://img.shields.io/badge/license-Apache%202-blue.svg)]()
[![](https://img.shields.io/badge/python-3.7-blue.svg)]()
[![](https://img.shields.io/badge/latest-v0.14.0-blue.svg)]()
[![](https://img.shields.io/circleci/project/github/olitheolix/square/master.svg?style=flat)]()
[![](https://img.shields.io/codecov/c/github/olitheolix/square.svg?style=flat)]()
[![](https://img.shields.io/badge/status-dev-orange.svg)]()


Declarative state management of a Kubernetes cluster. It is somewhat akin to
Terraform, just for Kubernetes. It can also download all the cluster manifests
to get a snapshot of the current state.

# Installation
You can install *Square* in any Python 3.7 environment with:

```console
foo@bar:~$ pip install kubernetes-square
foo@bar:~$ square version
v0.14.0
```

You may also [build your own binary](Building-A-Binary), if you prefer.
Some pre-built one are also available on the [Release
page](https://github.com/olitheolix/square/releases).


## Supported Clusters
*Square* supports standard configurations for Minikube, EKS and GKE.

# Examples
*Square* will use the `KUBECONFIG` environment variable to locate the
Kubernetes credentials. Use `--kubeconfig` and `--context` arguments to
override this behaviour.

## Get Current Cluster State
Download all _Namespace_- and _Deployment_ manifests from the cluster and save
them to `./manifests`:

```console
foo@bar:~$ square get ns deployment --groupby ns kind --folder manifests/
foo@bar:~$ tree manifests
manifests
├── clusterrolebinding.yaml
├── clusterrole.yaml
├── default
│   └── namespace.yaml
├── kube-public
│   └── namespace.yaml
├── kube-system
│   ├── deployment.yaml
│   └── namespace.yaml
└── tests
    ├── deployment.yaml
    └── namespace.yaml
```

These are the Yaml files for an (almost) vanilla Minikube cluster. The layout
should be self explanatory, and you can change it with the `--groupby` argument.

The file names are arbitrary. Feel free to rename them as you please. The same
applies to their content: slice it as you see fit because. This works because
*Square* always flattens and concatenates all manifest files internally. The
`square get ...` command is also smart enough to sync changes back to the
correct file, or use `_other.yaml` for new resources.

### Group By Label
*Square* can also organise the manifests by _one_ label. For instance, if your
Kubernetes resources have an `app` label, then you organise your manifests like
in the next example:

```console
foo@bar:~$ square get ns deployment --groupby ns label=app kind --folder manifests/
foo@bar:~$ tree manifests
manifests
├── kube-system
│   ├── cluster-autoscaler
│   │   ├── rolebinding.yaml
│   │   └── role.yaml
│   ├── helm
│   │   ├── deployment.yaml
│   │   └── service.yaml
│   ├── metrics-server
│   │   └── rolebinding.yaml
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
├── nginx-ingress
│   ├── clusterrolebinding.yaml
│   └── clusterrole.yaml
├── _other
│   ├── clusterrolebinding.yaml
│   └── clusterrole.yaml
└── prometheus
   ├── clusterrolebinding.yaml
   └── clusterrole.yaml
```

All resources that live in the same namespace *and* have the same value for
their `app` label are now co-located in one folder. Resources without an `app`
label end up in the catch-all folder `_other`.

## Diff Cluster State
Following on with the example, the local files and the cluster state
should now be in sync, which means an empty plan:

```console
foo@bar:~$ square plan ns
--------------------------------------------------------------------------------
Plan: 0 to add, 0 to change, 0 to destroy.
```

To make this more interesting, add a label to the _Namespace_ manifest in
`_default.yaml` to make it look like this:
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

Save it and compute the difference:
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
would patch the `default` name space to bring the K8s cluster back into sync
with the local files. Let's do just that:

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

Use *kubectl* to verify that the patch worked and the name space now has a
`foo:bar` label.

```console
foo@bar:~$ kubectl describe ns default
Name:         default
Labels:       foo=bar
Annotations:  <none>
Status:       Active

No resource quota.

No resource limits.

```

## Create and Destroy Resources
The `apply` operation we just saw is also the tool to create and delete
resources. To add a new one, simply add its manifest to `manifests/` (create a
new file or add it to an existing one). Then use _square_ to patch it.

For instance, to deploy the latest *Square* image from
[Dockerhub](https://hub.docker.com/r/olitheolix/square), download the [example
manifests](examples/square.yaml) into the `manifests/` folder and use _square_
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
foo@bar:~$ pipenv install pyinstaller
foo@bar:~$ pipenv run pyinstaller square/square.py --onefile
```

This should produce the `./dist/square` executable.


# Deploy On A Cluster
*Square* does not require anything installed on your cluster to work. However,
it will require the appropriate RBACs if you want to run it from inside your
cluster. The [examples folder](examples) contains demo manifests to deploy the
official [Docker image](https://hub.docker.com/r/olitheolix/square).

This can be useful for automation tasks. For instance, you may want to
track configuration drift of your cluster over time.

# Tests
*Square* ships with a comprehensive unit test suit and a still nascent
integration tests suite.

    pipenv run pytest

This will automatically run the integration tests as well if you have started
the [KIND](https://github.com/bsycorp/kind) cluster:

    cd deployment
    ./start_minikube.sh
    cd ..

NOTE: currently, CI runs the unit tests only.

# Development Status
*Square* is still under development. Several rough edges remain but the core
has become stable enough for serious work.
