# The `sh` library does not exist for Windows.
try:
    import sh
except ImportError:
    sh = None

from collections import defaultdict
from typing import Any, Dict, List

from square.dtypes import K8sConfig, K8sResource


def kind_available():
    """Return `True` if we have an integration test cluster available."""
    if not sh:
        # We probably run on Windows - no integration test cluster.
        return False

    # Query the version of the integration test cluster. If that works we have
    # a cluster that the tests can use, otherwise not.
    try:
        sh.kubectl("--kubeconfig", "/tmp/kubeconfig-kind.yaml", "version")  # type: ignore
    except (ImportError, sh.CommandNotFound, sh.ErrorReturnCode_1):         # type: ignore
        return False
    return True


def k8s_apis(config: K8sConfig) -> Dict[str, List[K8sResource]]:
    res = [
        K8sResource(
            apiVersion="rbac.authorization.k8s.io/v1",
            kind="ClusterRole",
            name="clusterroles",
            namespaced=False,
            url=f"{config.url}/apis/rbac.authorization.k8s.io/v1",
            aliases=("clusterrole", "clusterroles"),
            preferred=True,
        ),
        K8sResource(
            apiVersion="rbac.authorization.k8s.io/v1",
            kind="ClusterRoleBinding",
            name="clusterrolebindings",
            namespaced=False,
            url=f"{config.url}/apis/rbac.authorization.k8s.io/v1",
            aliases=("clusterrolebinding", "clusterrolebindins"),
            preferred=True,
        ),
        K8sResource(
            apiVersion="v1",
            kind="ConfigMap",
            name="configmaps",
            namespaced=False,
            url=f"{config.url}/api/v1",
            aliases=("cm", "configmap", "configmaps"),
            preferred=True,
        ),
        K8sResource(
            apiVersion="apiextensions.k8s.io/v1",
            kind="CustomResourceDefinition",
            name="customresourcedefinitions",
            namespaced=False,
            url=f"{config.url}/apis/apiextensions.k8s.io/v1",
            aliases=("crd", "customresourcedefinition"),
            preferred=True,
        ),
        K8sResource(
            apiVersion="mycrd.com/v1",
            kind="DemoCRD",
            name="democrds",
            namespaced=True,
            url=f"{config.url}/apis/mycrd.com/v1",
            aliases=("democrd", "democrds"),
            preferred=True,
        ),
        K8sResource(
            apiVersion="apps/v1",
            kind="Deployment",
            name="deployments",
            namespaced=True,
            url=f"{config.url}/apis/apps/v1",
            aliases=("deploy", "deployment", "deployments"),
            preferred=True,
        ),
        K8sResource(
            apiVersion="autoscaling/v2",
            kind="HorizontalPodAutoscaler",
            name="horizontalpodautoscalers",
            namespaced=True,
            url=f"{config.url}/apis/autoscaling/v2",
            aliases=("horizontalpodautoscaler", "horizontalpodautoscalers", "hpa"),
            preferred=True,
        ),
        K8sResource(
            apiVersion="autoscaling/v1",
            kind="HorizontalPodAutoscaler",
            name="horizontalpodautoscalers",
            namespaced=True,
            url=f"{config.url}/apis/autoscaling/v1",
            aliases=("horizontalpodautoscaler", "horizontalpodautoscalers", "hpa"),
        ),
        K8sResource(
            apiVersion="autoscaling/v2beta1",
            kind="HorizontalPodAutoscaler",
            name="horizontalpodautoscalers",
            namespaced=True,
            url=f"{config.url}/apis/autoscaling/v2beta1",
            aliases=("horizontalpodautoscaler", "horizontalpodautoscalers", "hpa"),
        ),
        K8sResource(
            apiVersion="autoscaling/v2beta2",
            kind="HorizontalPodAutoscaler",
            name="horizontalpodautoscalers",
            namespaced=True,
            url=f"{config.url}/apis/autoscaling/v2beta2",
            aliases=("horizontalpodautoscaler", "horizontalpodautoscalers", "hpa"),
        ),
        K8sResource(
            apiVersion="networking.k8s.io/v1",
            kind="Ingress",
            name="ingresses",
            namespaced=False,
            url=f"{config.url}/apis/networking.k8s.io/v1",
            aliases=("ingress", "ing"),
            preferred=True,
        ),
        K8sResource(
            apiVersion="v1",
            kind="Namespace",
            name="namespaces",
            namespaced=False,
            url=f"{config.url}/api/v1",
            aliases=("namespace", "namespaces", "ns"),
            preferred=True,
        ),
        K8sResource(
            apiVersion="v1",
            kind="Pod",
            name="pods",
            namespaced=True,
            url=f"{config.url}/api/v1",
            aliases=("po", "pod", "pods"),
            preferred=True,
        ),
        K8sResource(
            apiVersion="rbac.authorization.k8s.io/v1",
            kind="Role",
            name="roles",
            namespaced=False,
            url=f"{config.url}/apis/rbac.authorization.k8s.io/v1",
            aliases=("role", "roles"),
            preferred=True,
        ),
        K8sResource(
            apiVersion="rbac.authorization.k8s.io/v1",
            kind="RoleBinding",
            name="rolebindings",
            namespaced=False,
            url=f"{config.url}/apis/rbac.authorization.k8s.io/v1",
            aliases=("rolebinding", "rolebindings"),
            preferred=True,
        ),
        K8sResource(
            apiVersion="v1",
            kind="Secret",
            name="secrets",
            namespaced=False,
            url=f"{config.url}/api/v1",
            aliases=("secret", "secrets"),
            preferred=True,
        ),
        K8sResource(
            apiVersion="v1",
            kind="Service",
            name="services",
            namespaced=True,
            url=f"{config.url}/api/v1",
            aliases=("service", "services", "svc"),
            preferred=True,
        ),
        K8sResource(
            apiVersion="v1",
            kind="ServiceAccount",
            name="serviceaccounts",
            namespaced=True,
            url=f"{config.url}/api/v1",
            aliases=("serviceaccount", "serviceaccounts", "sa"),
            preferred=True,
        ),
    ]
    assert len(res) == len(set(res))

    # Expand the resources into all their various forms, eg {"po", "pod",
    # "po.v1", "pod.v1", "deploy", ..., "deployment.apps", ...}
    ans: Dict[str, List[K8sResource]] = defaultdict(list)
    for r in res:
        for name in r.aliases:
            ans[name].append(r)
            if r.apiVersion == "v1":
                ans[f"{name}.v1"].append(r)
            else:
                group = r.apiVersion.partition("/")[0]
                ans[f"{name}.{group}"].append(r)
                ans[f"{name}.{r.apiVersion}"].append(r)
    return ans


def make_manifest(kind: str, namespace: str | None, name: str | None,
                  labels: Dict[str, str] = {}) -> dict:
    # Try to find the resource `kind` and lift its associated `apiVersion`.
    apis = k8s_apis(K8sConfig(version="1.26"))
    apiVersion = apis[kind.lower()][0].apiVersion

    # Compile a manifest.
    manifest: Dict[str, Any]
    manifest = {
        'apiVersion': apiVersion,
        'kind': kind,
        'metadata': {
            'name': name,
            'labels': labels,
        },
        'spec': {
            'finalizers': ['kubernetes']
        },
        'garbage': 'more garbage',
    }

    # Do not include an empty label dict.
    if not labels:
        del manifest["metadata"]["labels"]

    # Only create namespace entry if one was specified.
    if namespace is not None:
        manifest['metadata']['namespace'] = namespace

    return manifest


def mk_deploy(name: str, ns: str = "namespace") -> dict:
    return make_manifest("Deployment", ns, name)
