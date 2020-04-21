from typing import Any, Dict

from square.dtypes import K8sConfig, K8sResource


def k8s_apis(config: K8sConfig):
    return {
        ("ClusterRole", "rbac.authorization.k8s.io/v1"): K8sResource(
            apiVersion="rbac.authorization.k8s.io/v1",
            kind="ClusterRole",
            name="clusterroles",
            namespaced=False,
            url=f"{config.url}/apis/rbac.authorization.k8s.io/v1",
        ),
        ("ClusterRole", "rbac.authorization.k8s.io/v1beta1"): K8sResource(
            apiVersion="rbac.authorization.k8s.io/v1beta1",
            kind="ClusterRole",
            name="clusterroles",
            namespaced=False,
            url=f"{config.url}/apis/rbac.authorization.k8s.io/v1beta1",
        ),
        ("ClusterRoleBinding", "rbac.authorization.k8s.io/v1"): K8sResource(
            apiVersion="rbac.authorization.k8s.io/v1",
            kind="ClusterRoleBinding",
            name="clusterrolebindings",
            namespaced=False,
            url=f"{config.url}/apis/rbac.authorization.k8s.io/v1",
        ),
        ("ClusterRoleBinding", "rbac.authorization.k8s.io/v1beta1"): K8sResource(
            apiVersion="rbac.authorization.k8s.io/v1beta1",
            kind="ClusterRoleBinding",
            name="clusterrolebindings",
            namespaced=False,
            url=f"{config.url}/apis/rbac.authorization.k8s.io/v1beta1",
        ),
        ("ConfigMap", "v1"): K8sResource(
            apiVersion="v1",
            kind="ConfigMap",
            name="configmaps",
            namespaced=True,
            url=f"{config.url}/api/v1",
        ),
        ("CronJob", "batch/v1beta1"): K8sResource(
            apiVersion="batch/v1beta1",
            kind="CronJob",
            name="cronjobs",
            namespaced=True,
            url=f"{config.url}/apis/batch/v1beta1",
        ),
        ("CustomResourceDefinition", "apiextensions.k8s.io/v1beta1"): K8sResource(
            apiVersion="apiextensions.k8s.io/v1beta1",
            kind="CustomResourceDefinition",
            name="customresourcedefinitions",
            namespaced=False,
            url=f"{config.url}/apis/apiextensions.k8s.io/v1beta1",
        ),
        ("DaemonSet", "apps/v1"): K8sResource(
            apiVersion="apps/v1",
            kind="DaemonSet",
            name="daemonsets",
            namespaced=True,
            url=f"{config.url}/apis/apps/v1",
        ),
        ("DaemonSet", "apps/v1beta2"): K8sResource(
            apiVersion="apps/v1beta2",
            kind="DaemonSet",
            name="daemonsets",
            namespaced=True,
            url=f"{config.url}/apis/apps/v1beta2",
        ),
        ("DaemonSet", "extensions/v1beta1"): K8sResource(
            apiVersion="extensions/v1beta1",
            kind="DaemonSet",
            name="daemonsets",
            namespaced=True,
            url=f"{config.url}/apis/extensions/v1beta1",
        ),
        ("DemoCRD", "mycrd.com/v1"): K8sResource(
            apiVersion="mycrd.com/v1",
            kind="DemoCRD",
            name="democrds",
            namespaced=True,
            url=f"{config.url}/apis/mycrd.com/v1",
        ),
        ("Deployment", "apps/v1"): K8sResource(
            apiVersion="apps/v1",
            kind="Deployment",
            name="deployments",
            namespaced=True,
            url=f"{config.url}/apis/apps/v1",
        ),
        ("Deployment", "apps/v1beta1"): K8sResource(
            apiVersion="apps/v1beta1",
            kind="Deployment",
            name="deployments",
            namespaced=True,
            url=f"{config.url}/apis/apps/v1beta1",
        ),
        ("Deployment", "apps/v1beta2"): K8sResource(
            apiVersion="apps/v1beta2",
            kind="Deployment",
            name="deployments",
            namespaced=True,
            url=f"{config.url}/apis/apps/v1beta2",
        ),
        ("Deployment", "extensions/v1beta1"): K8sResource(
            apiVersion="extensions/v1beta1",
            kind="Deployment",
            name="deployments",
            namespaced=True,
            url=f"{config.url}/apis/extensions/v1beta1",
        ),
        ("HorizontalPodAutoscaler", "autoscaling/v1"): K8sResource(
            apiVersion="autoscaling/v1",
            kind="HorizontalPodAutoscaler",
            name="horizontalpodautoscalers",
            namespaced=True,
            url=f"{config.url}/apis/autoscaling/v1",
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
        ("Ingress", "extensions/v1beta1"): K8sResource(
            apiVersion="extensions/v1beta1",
            kind="Ingress",
            name="ingresses",
            namespaced=True,
            url=f"{config.url}/apis/extensions/v1beta1",
        ),
        ("Ingress", "networking.k8s.io/v1beta1"): K8sResource(
            apiVersion="networking.k8s.io/v1beta1",
            kind="Ingress",
            name="ingresses",
            namespaced=True,
            url=f"{config.url}/apis/networking.k8s.io/v1beta1",
        ),
        ("Job", "batch/v1"): K8sResource(
            apiVersion="batch/v1",
            kind="Job",
            name="jobs",
            namespaced=True,
            url=f"{config.url}/apis/batch/v1",
        ),
        ("Namespace", "v1"): K8sResource(
            apiVersion="v1",
            kind="Namespace",
            name="namespaces",
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
        ("PersistentVolumeClaim", "v1"): K8sResource(
            apiVersion="v1",
            kind="PersistentVolumeClaim",
            name="persistentvolumeclaims",
            namespaced=True,
            url=f"{config.url}/api/v1",
        ),
        ("PodDisruptionBudget", "policy/v1beta1"): K8sResource(
            apiVersion="policy/v1beta1",
            kind="PodDisruptionBudget",
            name="poddisruptionbudgets",
            namespaced=True,
            url=f"{config.url}/apis/policy/v1beta1",
        ),
        ("RoleBinding", "rbac.authorization.k8s.io/v1"): K8sResource(
            apiVersion="rbac.authorization.k8s.io/v1",
            kind="RoleBinding",
            name="rolebindings",
            namespaced=True,
            url=f"{config.url}/apis/rbac.authorization.k8s.io/v1",
        ),
        ("RoleBinding", "rbac.authorization.k8s.io/v1beta1"): K8sResource(
            apiVersion="rbac.authorization.k8s.io/v1beta1",
            kind="RoleBinding",
            name="rolebindings",
            namespaced=True,
            url=f"{config.url}/apis/rbac.authorization.k8s.io/v1beta1",
        ),
        ("Secret", "v1"): K8sResource(
            apiVersion="v1",
            kind="Secret",
            name="secrets",
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
        ("ServiceAccount", "v1"): K8sResource(
            apiVersion="v1",
            kind="ServiceAccount",
            name="serviceaccounts",
            namespaced=True,
            url=f"{config.url}/api/v1",
        ),
        ("StatefulSet", "apps/v1"): K8sResource(
            apiVersion="apps/v1",
            kind="StatefulSet",
            name="statefulsets",
            namespaced=True,
            url=f"{config.url}/apis/apps/v1",
        ),
        ("StatefulSet", "apps/v1beta1"): K8sResource(
            apiVersion="apps/v1beta1",
            kind="StatefulSet",
            name="statefulsets",
            namespaced=True,
            url=f"{config.url}/apis/apps/v1beta1",
        ),
        ("StatefulSet", "apps/v1beta2"): K8sResource(
            apiVersion="apps/v1beta2",
            kind="StatefulSet",
            name="statefulsets",
            namespaced=True,
            url=f"{config.url}/apis/apps/v1beta2",
        ),
    }


def make_manifest(kind: str, namespace: str, name: str,
                  labels: Dict[str, str] = {}) -> dict:
    # Try to find the resource `kind` and lift its associated `apiVersion`.
    apis = k8s_apis(K8sConfig(version="1.15"))
    candidates = [(_kind, _ver) for _kind, _ver in apis if _kind == kind]
    if len(candidates) == 0:
        apiVersion = "v1"
    else:
        candidates.sort()
        resource = apis[candidates.pop(0)]
        apiVersion = resource.apiVersion

    # Compile a manifest.
    manifest: Dict[str, Any]
    manifest = {
        'apiVersion': apiVersion,
        'kind': kind,
        'metadata': {
            'name': name,
            'labels': labels,
            'foo': 'bar',
        },
        'spec': {
            'finalizers': ['kubernetes']
        },
        'status': {
            'some': 'status',
        },
        'garbage': 'more garbage',
    }

    # Only create namespace entry if one was specified.
    if namespace is not None:
        manifest['metadata']['namespace'] = namespace

    return manifest


def mk_deploy(name: str, ns: str = "namespace") -> dict:
    return make_manifest("Deployment", ns, name)
