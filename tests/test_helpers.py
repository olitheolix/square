# The `sh` library does not exist for Windows.
try:
    import sh
except ImportError:
    sh = None

from typing import Any, Dict

from square.dtypes import K8sConfig, K8sResource


def kind_available():
    """Return `True` if we have an integration test cluster available."""
    if not sh:
        # We probably run on Windows - no integration test cluster.
        return False

    # Query the version of the integration test cluster. If that works we have
    # a cluster that the tests can use, otherwise not.
    try:
        sh.kubectl("--kubeconfig", "/tmp/kubeconfig-kind.yaml", "version")
    except (ImportError, sh.CommandNotFound, sh.ErrorReturnCode_1):
        return False
    return True


def k8s_apis(config: K8sConfig):
    return {
        ("ClusterRole", ""): K8sResource(
            apiVersion="rbac.authorization.k8s.io/v1",
            kind="ClusterRole",
            name="clusterroles",
            namespaced=False,
            url=f"{config.url}/apis/rbac.authorization.k8s.io/v1",
        ),
        ("ClusterRole", "rbac.authorization.k8s.io/v1"): K8sResource(
            apiVersion="rbac.authorization.k8s.io/v1",
            kind="ClusterRole",
            name="clusterroles",
            namespaced=False,
            url=f"{config.url}/apis/rbac.authorization.k8s.io/v1",
        ),
        ("ClusterRoleBinding", ""): K8sResource(
            apiVersion="rbac.authorization.k8s.io/v1",
            kind="ClusterRoleBinding",
            name="clusterrolebindings",
            namespaced=False,
            url=f"{config.url}/apis/rbac.authorization.k8s.io/v1",
        ),
        ("ClusterRoleBinding", "rbac.authorization.k8s.io/v1"): K8sResource(
            apiVersion="rbac.authorization.k8s.io/v1",
            kind="ClusterRoleBinding",
            name="clusterrolebindings",
            namespaced=False,
            url=f"{config.url}/apis/rbac.authorization.k8s.io/v1",
        ),
        ("ConfigMap", ""): K8sResource(
            apiVersion="v1",
            kind="ConfigMap",
            name="configmaps",
            namespaced=True,
            url=f"{config.url}/api/v1",
        ),
        ("ConfigMap", "v1"): K8sResource(
            apiVersion="v1",
            kind="ConfigMap",
            name="configmaps",
            namespaced=True,
            url=f"{config.url}/api/v1",
        ),
        ("CronJob", ""): K8sResource(
            apiVersion="batch/v1",
            kind="CronJob",
            name="cronjobs",
            namespaced=True,
            url=f"{config.url}/apis/batch/v1",
        ),
        ("CronJob", "batch/v1"): K8sResource(
            apiVersion="batch/v1",
            kind="CronJob",
            name="cronjobs",
            namespaced=True,
            url=f"{config.url}/apis/batch/v1",
        ),
        ("CustomResourceDefinition", ""): K8sResource(
            apiVersion="apiextensions.k8s.io/v1",
            kind="CustomResourceDefinition",
            name="customresourcedefinitions",
            namespaced=False,
            url=f"{config.url}/apis/apiextensions.k8s.io/v1",
        ),
        ("CustomResourceDefinition", "apiextensions.k8s.io/v1"): K8sResource(
            apiVersion="apiextensions.k8s.io/v1",
            kind="CustomResourceDefinition",
            name="customresourcedefinitions",
            namespaced=False,
            url=f"{config.url}/apis/apiextensions.k8s.io/v1",
        ),
        ("DaemonSet", ""): K8sResource(
            apiVersion="apps/v1",
            kind="DaemonSet",
            name="daemonsets",
            namespaced=True,
            url=f"{config.url}/apis/apps/v1",
        ),
        ("DaemonSet", "apps/v1"): K8sResource(
            apiVersion="apps/v1",
            kind="DaemonSet",
            name="daemonsets",
            namespaced=True,
            url=f"{config.url}/apis/apps/v1",
        ),
        ("DemoCRD", ""): K8sResource(
            apiVersion="mycrd.com/v1",
            kind="DemoCRD",
            name="democrds",
            namespaced=True,
            url=f"{config.url}/apis/mycrd.com/v1",
        ),
        ("DemoCRD", "mycrd.com/v1"): K8sResource(
            apiVersion="mycrd.com/v1",
            kind="DemoCRD",
            name="democrds",
            namespaced=True,
            url=f"{config.url}/apis/mycrd.com/v1",
        ),
        ("Deployment", ""): K8sResource(
            apiVersion="apps/v1",
            kind="Deployment",
            name="deployments",
            namespaced=True,
            url=f"{config.url}/apis/apps/v1",
        ),
        ("Deployment", "apps/v1"): K8sResource(
            apiVersion="apps/v1",
            kind="Deployment",
            name="deployments",
            namespaced=True,
            url=f"{config.url}/apis/apps/v1",
        ),
        ("HorizontalPodAutoscaler", ""): K8sResource(
            apiVersion="autoscaling/v2",
            kind="HorizontalPodAutoscaler",
            name="horizontalpodautoscalers",
            namespaced=True,
            url=f"{config.url}/apis/autoscaling/v2",
        ),
        ("HorizontalPodAutoscaler", "autoscaling/v1"): K8sResource(
            apiVersion="autoscaling/v1",
            kind="HorizontalPodAutoscaler",
            name="horizontalpodautoscalers",
            namespaced=True,
            url=f"{config.url}/apis/autoscaling/v1",
        ),
        ("HorizontalPodAutoscaler", "autoscaling/v2"): K8sResource(
            apiVersion="autoscaling/v2",
            kind="HorizontalPodAutoscaler",
            name="horizontalpodautoscalers",
            namespaced=True,
            url=f"{config.url}/apis/autoscaling/v2",
        ),
        ("HorizontalPodAutoscaler", "autoscaling/v2beta1"): K8sResource(
            apiVersion="autoscaling/v2beta1",
            kind="HorizontalPodAutoscaler",
            name="horizontalpodautoscalers",
            namespaced=True,
            url=f"{config.url}/apis/autoscaling/v2beta1",
        ),
        ("HorizontalPodAutoscaler", "autoscaling/v2beta2"): K8sResource(
            apiVersion="autoscaling/v2beta2",
            kind="HorizontalPodAutoscaler",
            name="horizontalpodautoscalers",
            namespaced=True,
            url=f"{config.url}/apis/autoscaling/v2beta2",
        ),
        ("Ingress", ""): K8sResource(
            apiVersion="networking.k8s.io/v1",
            kind="Ingress",
            name="ingresses",
            namespaced=True,
            url=f"{config.url}/apis/networking.k8s.io/v1",
        ),
        ("Ingress", "networking.k8s.io/v1"): K8sResource(
            apiVersion="networking.k8s.io/v1",
            kind="Ingress",
            name="ingresses",
            namespaced=True,
            url=f"{config.url}/apis/networking.k8s.io/v1",
        ),
        ("Job", ""): K8sResource(
            apiVersion="batch/v1",
            kind="Job",
            name="jobs",
            namespaced=True,
            url=f"{config.url}/apis/batch/v1",
        ),
        ("Job", "batch/v1"): K8sResource(
            apiVersion="batch/v1",
            kind="Job",
            name="jobs",
            namespaced=True,
            url=f"{config.url}/apis/batch/v1",
        ),
        ("Namespace", ""): K8sResource(
            apiVersion="v1",
            kind="Namespace",
            name="namespaces",
            namespaced=False,
            url=f"{config.url}/api/v1",
        ),
        ("Namespace", "v1"): K8sResource(
            apiVersion="v1",
            kind="Namespace",
            name="namespaces",
            namespaced=False,
            url=f"{config.url}/api/v1",
        ),
        ("PersistentVolume", ""): K8sResource(
            apiVersion="v1",
            kind="PersistentVolume",
            name="persistentvolumes",
            namespaced=False,
            url=f"{config.url}/api/v1",
        ),
        ("PersistentVolume", "v1"): K8sResource(
            apiVersion="v1",
            kind="PersistentVolume",
            name="persistentvolumes",
            namespaced=False,
            url=f"{config.url}/api/v1",
        ),
        ("PersistentVolumeClaim", ""): K8sResource(
            apiVersion="v1",
            kind="PersistentVolumeClaim",
            name="persistentvolumeclaims",
            namespaced=True,
            url=f"{config.url}/api/v1",
        ),
        ("PersistentVolumeClaim", "v1"): K8sResource(
            apiVersion="v1",
            kind="PersistentVolumeClaim",
            name="persistentvolumeclaims",
            namespaced=True,
            url=f"{config.url}/api/v1",
        ),
        ("Pod", ""): K8sResource(
            apiVersion="v1",
            kind="Pod",
            name="pods",
            namespaced=True,
            url=f"{config.url}/api/v1",
        ),
        ("Pod", "v1"): K8sResource(
            apiVersion="v1",
            kind="Pod",
            name="pods",
            namespaced=True,
            url=f"{config.url}/api/v1",
        ),
        ("PodDisruptionBudget", ""): K8sResource(
            apiVersion="policy/v1",
            kind="PodDisruptionBudget",
            name="poddisruptionbudgets",
            namespaced=True,
            url=f"{config.url}/apis/policy/v1",
        ),
        ("PodDisruptionBudget", "policy/v1"): K8sResource(
            apiVersion="policy/v1",
            kind="PodDisruptionBudget",
            name="poddisruptionbudgets",
            namespaced=True,
            url=f"{config.url}/apis/policy/v1",
        ),
        ("Role", ""): K8sResource(
            apiVersion="rbac.authorization.k8s.io/v1",
            kind="Role",
            name="roles",
            namespaced=True,
            url=f"{config.url}/apis/rbac.authorization.k8s.io/v1",
        ),
        ("Role", "rbac.authorization.k8s.io/v1"): K8sResource(
            apiVersion="rbac.authorization.k8s.io/v1",
            kind="Role",
            name="roles",
            namespaced=True,
            url=f"{config.url}/apis/rbac.authorization.k8s.io/v1",
        ),
        ("RoleBinding", ""): K8sResource(
            apiVersion="rbac.authorization.k8s.io/v1",
            kind="RoleBinding",
            name="rolebindings",
            namespaced=True,
            url=f"{config.url}/apis/rbac.authorization.k8s.io/v1",
        ),
        ("RoleBinding", "rbac.authorization.k8s.io/v1"): K8sResource(
            apiVersion="rbac.authorization.k8s.io/v1",
            kind="RoleBinding",
            name="rolebindings",
            namespaced=True,
            url=f"{config.url}/apis/rbac.authorization.k8s.io/v1",
        ),
        ("Secret", ""): K8sResource(
            apiVersion="v1",
            kind="Secret",
            name="secrets",
            namespaced=True,
            url=f"{config.url}/api/v1",
        ),
        ("Secret", "v1"): K8sResource(
            apiVersion="v1",
            kind="Secret",
            name="secrets",
            namespaced=True,
            url=f"{config.url}/api/v1",
        ),
        ("Service", ""): K8sResource(
            apiVersion="v1",
            kind="Service",
            name="services",
            namespaced=True,
            url=f"{config.url}/api/v1",
        ),
        ("Service", "v1"): K8sResource(
            apiVersion="v1",
            kind="Service",
            name="services",
            namespaced=True,
            url=f"{config.url}/api/v1",
        ),
        ("ServiceAccount", ""): K8sResource(
            apiVersion="v1",
            kind="ServiceAccount",
            name="serviceaccounts",
            namespaced=True,
            url=f"{config.url}/api/v1",
        ),
        ("ServiceAccount", "v1"): K8sResource(
            apiVersion="v1",
            kind="ServiceAccount",
            name="serviceaccounts",
            namespaced=True,
            url=f"{config.url}/api/v1",
        ),
        ("StatefulSet", ""): K8sResource(
            apiVersion="apps/v1",
            kind="StatefulSet",
            name="statefulsets",
            namespaced=True,
            url=f"{config.url}/apis/apps/v1",
        ),
        ("StatefulSet", "apps/v1"): K8sResource(
            apiVersion="apps/v1",
            kind="StatefulSet",
            name="statefulsets",
            namespaced=True,
            url=f"{config.url}/apis/apps/v1",
        ),
    }


def make_manifest(kind: str, namespace: str, name: str,
                  labels: Dict[str, str] = {}) -> dict:
    # Try to find the resource `kind` and lift its associated `apiVersion`.
    apis = k8s_apis(K8sConfig(version="1.15"))
    try:
        apiVersion = apis[(kind, "")].apiVersion
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
