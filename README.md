[![](https://img.shields.io/badge/license-Apache%202-blue.svg)]()
[![](https://img.shields.io/badge/python-3.7-blue.svg)]()
[![](https://img.shields.io/circleci/project/github/olitheolix/square/master.svg?style=flat)]()
[![](https://img.shields.io/codecov/c/github/olitheolix/square.svg?style=flat)]()
[![](https://img.shields.io/badge/status-dev-orange.svg)]()


Declarative state management of a Kubernetes cluster. It is somewhat akin to
Terraform, just for Kubernetes. Unlike Terraform, it can also seamlessly
download all the cluster manifests to get a snapshot of the current state.

## Binaries
Linux and Windows binaries are available on the
[Release page](https://github.com/olitheolix/square/releases).

## Docker
The [examples](examples) explain how to deploy the official [Docker
image](https://hub.docker.com/r/olitheolix/square) directly to your cluster.

## Supported Clusters
*Square* supports standard configurations for Minikube, EKS and GKE. If
*kubectl* can access those then so should *square*.

# Examples
By default, *square* will use the default context and user from your
`~/.kube/conf` file. Use `--kubeconfig` and `--context` arguments to override
it.


## Get Current Cluster State
Download all _Namespace_- and _Deployment_ manifests from the cluster and save
them to `./manifests`:

```console
foo@bar:~$ square get ns deployment
foo@bar:~$ ls manifests/
_default.yaml  _kube-public.yaml  _kube-system.yaml  _None.yaml
```

These are the Yaml files for a vanilla Minikube cluster. Each file contains the
manifests for the respective Kubernetes _Namespace_ except `_None.yaml`, which
contains the manifests that exist independent of name spaces like `ClusterRole`
and `ClusterRoleBinding`.

The file names are arbitrary. Feel free to rename them as you please. The same
applies to their content: you may chop up the manifests in those files
as you see fit because *square* will internally concatenate all the files
anyway. It is also smart enough to sync changes back to the correct file when
you use `square get ...` again.

## Diff Cluster State
Following on from the previous example, the local files and the cluster state
should now be in sync, which means an empty diff:

```console
foo@bar:~$ square diff ns
foo@bar:~$
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

Save it and compute the diff:
```console
foo@bar:~$ square diff ns
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
```

This will show the difference in standard `diff` format. In words: *square*
would patch the `default` name space to bring the K8s cluster back into sync
with the local files. Let's do just that:

```console
foo@bar:~$ square patch ns
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

foo@bar:~$ square diff ns
foo@bar:~$
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

# Create and Destroy Resources
The `patch` operation we just saw is also the tool to create and delete
resources. To add a new one, simply add its manifest to `manifests/` (create a
new file or add it to an existing one). Then use _square_ to patch it.

For instance, to deploy the latest *square* image from
[Dockerhub](https://hub.docker.com/r/olitheolix/square), download the [example
manifests](examples/square.yaml) into the `manifests/` folder and use _square_
to deploy it:

```console
foo@bar:~$ wget https://github.com/olitheolix/square/raw/master/examples/square.yaml -O manifests/square.yaml
foo@bar:~$ square patch all
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
foo@bar:~$ 
```

# Development Status
*Square* is still under development. Several rough edges remain but the core
has become stable enough for more serious work.
