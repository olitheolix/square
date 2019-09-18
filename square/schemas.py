"""Define the structure of the stripped manifest schema.

A stripped manifest is a sub-set of a normal manifest. The remaining keys
capture the salient information about the resource.

For instance, almost all manifest can have a "status" field. Albeit useful for
diagnostics, it makes no sense to compute diffs of "status" fields and submit
them in patches.

# Schema Conventions
Schemas are normal dictionaries without a depth limit.  All keys correspond to
a K8s manifest key. All value must be either dicts themselves, a bool to
specify whether the fields must be included in the stripped manifest, or None
if the field is not mandatory but should be included.

* True: field will be included. It is an error if the input manifest lacks it.
* False: field will not be included. It is an error if the input manifest has it.
* None: field will be included if the input manifest has it, and ignored otherwise.

"""
schema_1_9 = {
    "ClusterRole": {
        "metadata": {
            "labels": None,
            "name": True,
            "namespace": False,
        },
        "rules": None,
    },
    "ClusterRoleBinding": {
        "metadata": {
            "labels": None,
            "name": True,
            "namespace": False,
        },
        "roleRef": None,
        "subjects": None
    },
    "ConfigMap": {
        "metadata": {
            "labels": None,
            "name": True,
            "namespace": True,
        },
        "data": None,
        "binaryData": None,
    },
    "DaemonSet": {
        "metadata": {
            "labels": None,
            "name": True,
            "namespace": True,
        },
        "spec": True,
    },
    "Deployment": {
        "metadata": {
            "labels": None,
            "name": True,
            "namespace": True,
        },
        "creationTimestamp": False,
        "spec": True,
    },
    "HorizontalPodAutoscaler": {
        "metadata": {
            "labels": None,
            "name": True,
            "namespace": True,
        },
        "spec": True,
    },
    "Ingress": {
        "metadata": {
            "annotations": {
                "ingress.kubernetes.io/configuration-snippet": None,
                "kubernetes.io/ingress.class": None,
            },
            "labels": None,
            "name": True,
            "namespace": True,
        },
        "spec": True,
    },
    "Namespace": {
        "metadata": {
            "labels": None,
            "name": True,
            "namespace": False
        },
        "spec": None,
    },
    "PersistentVolumeClaim": {
        "metadata": {
            "labels": None,
            "name": True,
            "namespace": True,
        },
        "spec": True,
    },
    "Role": {
        "metadata": {
            "labels": None,
            "name": True,
            "namespace": True,
        },
        "rules": None,
    },
    "RoleBinding": {
        "metadata": {
            "labels": None,
            "name": True,
            "namespace": True,
        },
        "roleRef": None,
        "subjects": None
    },
    "Secret": {
        "metadata": {
            "labels": None,
            "name": True,
            "namespace": True,
            "annotations": {
                "kubernetes.io/service-account.name": None,
            }
        },
        "data": None,
        "stringData": None,
        "type": None,
    },
    "Service": {
        "metadata": {
            "labels": None,
            "name": True,
            "namespace": True,
        },
        "spec": {
            "ports": None,
            "selector": None,
            "type": True,
        },
    },
    "ServiceAccount": {
        "kind": None,
        "imagePullSecrets": None,
        "metadata": {
            "labels": None,
            "name": True,
            "namespace": True,
        },
        "secrets": None,
    },
    "StatefulSet": {
        "metadata": {
            "labels": None,
            "name": True,
            "namespace": True,
        },
        "spec": True,
    },
}


RESOURCE_SCHEMA = {
    "1.9": schema_1_9,
    "1.10": schema_1_9,
    "1.11": schema_1_9,
    "1.13": schema_1_9,
    "1.14": schema_1_9,
}
