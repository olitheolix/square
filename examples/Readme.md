# Deploy Square
This example is only useful if you want to run Square inside your cluster. You
do not need to deploy anything to use it from your own terminal.

These examples were tested with Minikube v1.10.0.

The [manifest](square-single-namespace.yaml) deploys `square` into a new
namespace (also called `square`). Use `kubectl` or `square` to deploy it.

IMPORTANT: the RBAC configuration in this example is probably too liberal for
production clusters. For instance, you should only grant read access to
deployments if you only want to track the drift of _Deployment_
manifests.


```console
# Download the manifest.
foo@bar:~$ mkdir manifests
foo@bar:~$ wget https://github.com/olitheolix/square/raw/master/examples/square.yaml -O manifests/square.yaml

# Deploy with "square".
foo@bar:~$ square patch all -n square

# Deploy with "kubectl".
foo@bar:~$ kubectl apply -f manifests/square.yaml
```

Once deployed, you can log into the container and try it out:

```console
# Deploy with "kubectl".
foo@bar:~$ kubectl exec -ti kubectl -n square exec -ti square-<hash>-<hash> /bin/ash
root@square:~$ square get all
root@square:~$
```
