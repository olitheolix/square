# The `sh` library does not exist for Windows.
try:
    import sh
except ImportError:
    sh = None

from typing import Any, Dict, Tuple

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


def k8s_apis(config: K8sConfig) -> Dict[Tuple[str, str], K8sResource]:
    return {
        ("ClusterRole", ""): K8sResource(
            apiVersion="rbac.authorization.k8s.io/v1",
            kind="ClusterRole",
            name="clusterroles",
            namespaced=False,
            url=f"{config.url}/apis/rbac.authorization.k8s.io/v1",
            all_names=("clusterrole", "clusterroles"),
        ),
        ("ClusterRole", "rbac.authorization.k8s.io/v1"): K8sResource(
            apiVersion="rbac.authorization.k8s.io/v1",
            kind="ClusterRole",
            name="clusterroles",
            namespaced=False,
            url=f"{config.url}/apis/rbac.authorization.k8s.io/v1",
            all_names=("clusterrole", "clusterroles"),
        ),
        ("ConfigMap", ""): K8sResource(
            apiVersion="v1",
            kind="ConfigMap",
            name="configmaps",
            namespaced=True,
            url=f"{config.url}/api/v1",
            all_names=("configmap", "configmaps", "cm"),
        ),
        ("ConfigMap", "v1"): K8sResource(
            apiVersion="v1",
            kind="ConfigMap",
            name="configmaps",
            namespaced=True,
            url=f"{config.url}/api/v1",
            all_names=("configmap", "configmaps", "cm"),
        ),
        ("DemoCRD", ""): K8sResource(
            apiVersion="mycrd.com/v1",
            kind="DemoCRD",
            name="democrds",
            namespaced=True,
            url=f"{config.url}/apis/mycrd.com/v1",
            all_names=("democrd", "democrds"),
        ),
        ("DemoCRD", "mycrd.com/v1"): K8sResource(
            apiVersion="mycrd.com/v1",
            kind="DemoCRD",
            name="democrds",
            namespaced=True,
            url=f"{config.url}/apis/mycrd.com/v1",
            all_names=("democrd", "democrds"),
        ),
        ("Deployment", ""): K8sResource(
            apiVersion="apps/v1",
            kind="Deployment",
            name="deployments",
            namespaced=True,
            url=f"{config.url}/apis/apps/v1",
            all_names=("deploy", "deployment", "deployments"),
        ),
        ("Deployment", "apps/v1"): K8sResource(
            apiVersion="apps/v1",
            kind="Deployment",
            name="deployments",
            namespaced=True,
            url=f"{config.url}/apis/apps/v1",
            all_names=("deploy", "deployment", "deployments"),
        ),
        ("HorizontalPodAutoscaler", ""): K8sResource(
            apiVersion="autoscaling/v2",
            kind="HorizontalPodAutoscaler",
            name="horizontalpodautoscalers",
            namespaced=True,
            url=f"{config.url}/apis/autoscaling/v2",
            all_names=("horizontalpodautoscaler", "horizontalpodautoscalers", "hpa")
        ),
        ("HorizontalPodAutoscaler", "autoscaling/v1"): K8sResource(
            apiVersion="autoscaling/v1",
            kind="HorizontalPodAutoscaler",
            name="horizontalpodautoscalers",
            namespaced=True,
            url=f"{config.url}/apis/autoscaling/v1",
            all_names=("horizontalpodautoscaler", "horizontalpodautoscalers", "hpa")
        ),
        ("HorizontalPodAutoscaler", "autoscaling/v2"): K8sResource(
            apiVersion="autoscaling/v2",
            kind="HorizontalPodAutoscaler",
            name="horizontalpodautoscalers",
            namespaced=True,
            url=f"{config.url}/apis/autoscaling/v2",
            all_names=("horizontalpodautoscaler", "horizontalpodautoscalers", "hpa")
        ),
        ("HorizontalPodAutoscaler", "autoscaling/v2beta1"): K8sResource(
            apiVersion="autoscaling/v2beta1",
            kind="HorizontalPodAutoscaler",
            name="horizontalpodautoscalers",
            namespaced=True,
            url=f"{config.url}/apis/autoscaling/v2beta1",
            all_names=("horizontalpodautoscaler", "horizontalpodautoscalers", "hpa")
        ),
        ("HorizontalPodAutoscaler", "autoscaling/v2beta2"): K8sResource(
            apiVersion="autoscaling/v2beta2",
            kind="HorizontalPodAutoscaler",
            name="horizontalpodautoscalers",
            namespaced=True,
            url=f"{config.url}/apis/autoscaling/v2beta2",
            all_names=("horizontalpodautoscaler", "horizontalpodautoscalers", "hpa")
        ),
        ("Ingress", ""): K8sResource(
            apiVersion="networking.k8s.io/v1",
            kind="Ingress",
            name="ingresses",
            namespaced=True,
            url=f"{config.url}/apis/networking.k8s.io/v1",
            all_names=("ing", "ingress", "ingresses"),
        ),
        ("Ingress", "networking.k8s.io/v1"): K8sResource(
            apiVersion="networking.k8s.io/v1",
            kind="Ingress",
            name="ingresses",
            namespaced=True,
            url=f"{config.url}/apis/networking.k8s.io/v1",
            all_names=("ing", "ingress", "ingresses"),
        ),
        ("Namespace", ""): K8sResource(
            apiVersion="v1",
            kind="Namespace",
            name="namespaces",
            namespaced=False,
            url=f"{config.url}/api/v1",
            all_names=("namespace", "namespaces", "ns"),
        ),
        ("Namespace", "v1"): K8sResource(
            apiVersion="v1",
            kind="Namespace",
            name="namespaces",
            namespaced=False,
            url=f"{config.url}/api/v1",
            all_names=("namespace", "namespaces", "ns"),
        ),
        ("Pod", ""): K8sResource(
            apiVersion="v1",
            kind="Pod",
            name="pods",
            namespaced=True,
            url=f"{config.url}/api/v1",
            all_names=("po", "pod", "pods"),
        ),
        ("Pod", "v1"): K8sResource(
            apiVersion="v1",
            kind="Pod",
            name="pods",
            namespaced=True,
            url=f"{config.url}/api/v1",
            all_names=("po", "pod", "pods"),
        ),
        ("Service", ""): K8sResource(
            apiVersion="v1",
            kind="Service",
            name="services",
            namespaced=True,
            url=f"{config.url}/api/v1",
            all_names=("service", "services", "svc"),
        ),
        ("Service", "v1"): K8sResource(
            apiVersion="v1",
            kind="Service",
            name="services",
            namespaced=True,
            url=f"{config.url}/api/v1",
            all_names=("service", "services", "svc"),
        ),
    }


def make_manifest(kind: str, namespace: str | None, name: str | None,
                  labels: Dict[str, str] = {}) -> dict:
    # Try to find the resource `kind` and lift its associated `apiVersion`.
    apis = k8s_apis(K8sConfig(version="1.26"))
    try:
        apiVersion = apis[(kind, "")].apiVersion  # type: ignore
    except KeyError:
        apiVersion = "v1"

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
