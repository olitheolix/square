import copy
import itertools
import random
import sys
import unittest.mock as mock
from pathlib import Path
from typing import Dict, List, cast

import pytest
import yaml

import square.k8s as k8s
import square.manio as manio
from square.dtypes import (
    Config, Filters, GroupBy, LocalManifestLists, MetaManifest, Selectors,
    SquareManifests,
)
from square.k8s import resource

from .test_helpers import make_manifest, mk_deploy


class TestHelpers:
    def test_make_meta(self):
        """Basic test for helper class, especially Namespaces.

        Namespace, as usual, are a special because their K8s manifests must not
        have a "metadata.namespace" field. For those manifests, `make_meta`
        must populate the "MetaManifest.namespace" attribute with the content
        of "metadata.name" form the input manifest. Albeit invalid for K8s,
        this reduces special case coding for Namespaces in `square`.

        """
        # DEPLOYMENT manifest. All fields must be copied verbatim.
        manifest = make_manifest("Deployment", "namespace", "name")
        expected = MetaManifest(
            apiVersion=manifest["apiVersion"],
            kind=manifest["kind"],
            namespace="namespace",
            name=manifest["metadata"]["name"]
        )
        assert manio.make_meta(manifest) == expected

        # NAMESPACE manifest. The "namespace" field must be populated with the
        # "metadata.name" field.
        manifest = make_manifest("Namespace", None, "name")
        expected = MetaManifest(
            apiVersion=manifest["apiVersion"],
            kind=manifest["kind"],
            namespace=None,
            name=manifest["metadata"]["name"]
        )
        assert manio.make_meta(manifest) == expected

        # Some resource kinds apply globally and have no namespace, eg
        # ClusterRole and ClusterRoleBinding. For these, the namespace field
        # must be empty.
        manifest = make_manifest("ClusterRole", None, "name")
        expected = MetaManifest(
            apiVersion=manifest["apiVersion"],
            kind=manifest["kind"],
            namespace=None,
            name=manifest["metadata"]["name"]
        )
        assert manio.make_meta(manifest) == expected

    def test_is_valid_manifest(self, k8sconfig):
        """Check various (in)valid and manifests."""
        # CLUSTEROLE resources may declare a namespace even though Square and
        # K8s will both silently ignore it.
        manifest = make_manifest("ClusterRole", None, "name")
        assert manio.is_valid_manifest(manifest, k8sconfig)
        manifest = make_manifest("ClusterRole", "must-not-have-ns", "name")
        assert manio.is_valid_manifest(manifest, k8sconfig)

        # SERVICE resources must declare a namespace.
        manifest = make_manifest("Service", "must-have-ns", "name")
        assert manio.is_valid_manifest(manifest, k8sconfig)
        manifest = make_manifest("Service", None, "name")
        assert not manio.is_valid_manifest(manifest, k8sconfig)

        # NAMESPACE resources must not declare a namespace.
        manifest = make_manifest("Namespace", None, "name")
        assert manio.is_valid_manifest(manifest, k8sconfig)

        # UNKNOWN resources must be silently ignored.
        manifest = make_manifest("Unknown", None, "name")
        assert manio.is_valid_manifest(manifest, k8sconfig)
        manifest = make_manifest("Unknown", "ns", "name")
        assert manio.is_valid_manifest(manifest, k8sconfig)

        # Every resource must have several mandatory fields.
        mandatory_fields = ("apiVersion", "kind", "metadata")
        for field in mandatory_fields:
            manifest = make_manifest("ClusterRole", None, "name")
            del manifest[field]
            assert not manio.is_valid_manifest(manifest, k8sconfig)

            manifest = make_manifest("Service", "must-have-ns", "name")
            del manifest[field]
            assert not manio.is_valid_manifest(manifest, k8sconfig)

            manifest = make_manifest("Namespace", None, "name")
            del manifest[field]
            assert not manio.is_valid_manifest(manifest, k8sconfig)


class TestSelect:
    def test_select(self):
        """Select manifest based on `Selector`."""
        # Convenience.
        select = manio.select
        Sel = Selectors

        labels = [[], ["app=a"], ["env=e"], ["app=a", "env=e"]]

        # ---------------------------------------------------------------------
        #                      Namespace Manifest
        # ---------------------------------------------------------------------
        # Iterate over (almost) all valid selectors.
        manifest = make_manifest("Namespace", None, "name", {"app": "a", "env": "e"})
        namespaces = (["name"], ["name", "ns5"])
        kinds = ({"Namespace"}, {"Deployment", "Namespace"})
        for kind, ns, lab in itertools.product(kinds, namespaces, labels):
            sel = Sel(kinds=kind, namespaces=ns, labels=cast(List[str], lab))
            assert select(manifest, sel, True) is True

        # Function must reject these because the selectors match only partially.
        selectors = [
            Sel(kinds={"blah"}),
            Sel(kinds={"Namespace"}, namespaces=["foo-ns"]),
            Sel(kinds={"Namespace"}, namespaces=["ns0"], labels=["app=b"]),
            Sel(kinds={"Namespace"}, namespaces=["ns0"], labels=["app=a", "env=x"]),
        ]
        for sel in selectors:
            assert select(manifest, sel, True) is False

        # Reject the manifest if the selector does not specify at least one "kind".
        assert select(manifest, Sel(kinds={"Namespace"}), True) is True
        assert select(manifest, Sel(), True) is False

        # ---------------------------------------------------------------------
        #                      Deployment Manifest
        # ---------------------------------------------------------------------
        # Iterate over (almost) all valid selectors.
        manifest = make_manifest("Deployment", "my-ns", "name", {"app": "a", "env": "e"})
        kinds = ({"Deployment"}, {"Deployment", "Namespace"})
        namespaces = (["my-ns"], ["my-ns", "other-ns"])
        for kind, ns, lab in itertools.product(kinds, namespaces, labels):
            sel = Sel(kinds=kind, namespaces=ns, labels=cast(List[str], lab))
            assert select(manifest, sel, True) is True

        # Function must reject these because the selectors match only partially.
        selectors = [
            Sel(kinds={"blah"}),
            Sel(kinds={"Deployment"}, namespaces=["foo-ns"]),
            Sel(kinds={"Deployment"}, namespaces=["ns0"], labels=["app=b"]),
            Sel(kinds={"Deployment"}, namespaces=["ns0"], labels=["app=a", "env=x"]),
        ]
        for sel in selectors:
            assert select(manifest, sel, True) is False

        # Reject the manifest if the selector does not specify at least one "kind".
        assert select(manifest, Sel(kinds={"Deployment"}), True) is True
        assert select(manifest, Sel(), True) is False

        # ---------------------------------------------------------------------
        #                      ClusterRole Manifest
        # ---------------------------------------------------------------------
        # The namespace is irrelevant for a non-namespaced resource like ClusterRole.
        manifest = make_manifest("ClusterRole", None, "name", {"app": "a", "env": "e"})
        kinds = ({"ClusterRole"}, {"ClusterRole", "Namespace"})
        namespaces = (["my-ns"], ["my-ns", "other-ns"])
        for kind, ns, lab in itertools.product(kinds, namespaces, labels):
            sel = Sel(kinds=kind, namespaces=ns, labels=cast(List[str], lab))
            assert select(manifest, sel, True) is True

        # Function must reject these because the selectors match only partially.
        selectors = [
            Sel(kinds={"blah"}),
            Sel(kinds={"Clusterrole"}, namespaces=["ns0"], labels=["app=b"]),
            Sel(kinds={"Clusterrole"}, namespaces=["ns0"], labels=["app=a", "env=x"]),
        ]
        for sel in selectors:
            assert select(manifest, sel, True) is False

        # Reject the manifest if the selector does not specify at least one "kind".
        assert select(manifest, Sel(), True) is False

        # ---------------------------------------------------------------------
        #                    Default Service Account
        # ---------------------------------------------------------------------
        # Must always ignore "default" service account.
        kind, ns = "ServiceAccount", "ns1"
        manifest = make_manifest(kind, ns, "default")
        assert select(manifest, Sel(kinds={kind}, namespaces=[ns]), True) is False

        # Must select all other Secret that match the selector.
        manifest = make_manifest(kind, ns, "some-service-account")
        assert select(manifest, Sel(kinds={kind}, namespaces=[ns]), True) is True

        # ---------------------------------------------------------------------
        #                      Default Token Secret
        # ---------------------------------------------------------------------
        # Must always ignore "default-token-*" Secrets.
        kind, ns = "Secret", "ns1"
        manifest = make_manifest(kind, ns, "default-token-12345")
        assert select(manifest, Sel(kinds={kind}, namespaces=[ns]), True) is False

        # Must select all other Secret that match the selector.
        manifest = make_manifest(kind, ns, "some-secret")
        assert select(manifest, Sel(kinds={kind}, namespaces=[ns]), True) is True

    def test_select_ignore_labels(self):
        """The `select` function must ignore `labels` when asked."""
        # Convenience.
        select = manio.select

        # Create a Pod manifest.
        manifest = make_manifest("Pod", "ns", "mypod", labels={"foo": "bar"})

        # Selector matches the pod labels. It must therefore not matter if
        # `match_labels` is set or not.
        sel = Selectors(kinds={"Pod"}, labels=["foo=bar"])
        assert select(manifest, sel, match_labels=True) is True
        assert select(manifest, sel, match_labels=False) is True

        # Selector does not match the pod labels. The function must only report
        # a match when we disable label matching.
        sel = Selectors(kinds={"Pod"}, labels=["wrong=label"])
        assert select(manifest, sel, match_labels=True) is False
        assert select(manifest, sel, match_labels=False) is True

    def test_select_kind_name(self):
        """Verify `select` if not only KIND but also NAME was specified."""
        # Convenience.
        select = manio.select
        manifest = make_manifest("Pod", "ns", "app")

        # Must be selected because it is a Pod.
        for namespaces in ([], ["ns"]):
            selector = Selectors(kinds={"Pod"}, namespaces=namespaces)
            assert select(manifest, selector, True) is True

        # Must be selected because it is uniquely specified Pod.
        for namespaces in ([], ["ns"]):
            selector = Selectors(kinds={"Pod/app"}, namespaces=namespaces)
            assert select(manifest, selector, True) is True

        # Must not select it because it is not the uniquely specified Pod.
        for namespaces in ([], ["ns"]):
            selector = Selectors(kinds={"Pod/foo"}, namespaces=namespaces)
            assert select(manifest, selector, True) is False

        # Must be selected because it is covered by "Pod".
        for namespaces in ([], ["ns"]):
            selector = Selectors(kinds={"Pod/foo", "Pod"}, namespaces=namespaces)
            assert select(manifest, selector, True) is True

        # Must never select the manifest if the namespace is wrong.
        selectors = [
            Selectors(kinds={"Pod"}, namespaces=["wrong"]),
            Selectors(kinds={"Pod/app"}, namespaces=["wrong"]),
            Selectors(kinds={"Pod/foo"}, namespaces=["wrong"]),
            Selectors(kinds={"Pod/foo", "Pod"}, namespaces=["wrong"]),
        ]
        for selector in selectors:
            assert select(manifest, selector, True) is False


class TestUnpackParse:
    def test_unpack_k8s_resource_list_ok(self):
        """Convert eg a `DeploymentList` into a Python dict of `Deployments`."""
        # Demo manifests for this test.
        manifests_withkind = [
            make_manifest('Deployment', f'ns_{_}', f'name_{_}')
            for _ in range(3)
        ]
        # K8s does not include the "kind" key when it provides a resource in a
        # list. Here we mimic this behaviour and manually delete it. The test
        # function must be able to cope with that.
        manifests_nokind = copy.deepcopy(manifests_withkind)
        for manifest in manifests_nokind:
            del manifest["kind"]

        # The actual DeploymentList returned from K8s.
        manifest_list = {
            'apiVersion': 'apps/v1',
            'kind': 'DeploymentList',
            'items': manifests_nokind,
        }

        # Parse the DeploymentList into a dict. The keys are ManifestTuples and
        # the values are the Deployments (*not* DeploymentList) manifests.
        data, err = manio.unpack_k8s_resource_list(manifest_list)
        assert err is False

        # The test function must not have modified our original dictionaries,
        # which means they must still miss the "kind" field. However, it must
        # have added the correct "kind" value the manifests it returned.
        assert all(["kind" not in _ for _ in manifests_nokind])
        assert all(["kind" in v for _, v in data.items()])

        # Verify the Python dict.
        MM = MetaManifest
        assert data == {
            MM('apps/v1', 'Deployment', 'ns_0', 'name_0'): manifests_withkind[0],
            MM('apps/v1', 'Deployment', 'ns_1', 'name_1'): manifests_withkind[1],
            MM('apps/v1', 'Deployment', 'ns_2', 'name_2'): manifests_withkind[2],
        }

        # Function must return deep copies of the manifests to avoid difficult
        # to debug reference bugs.
        for src, out_key in zip(manifests_withkind, data):
            assert src == data[out_key]
            assert src is not data[out_key]

    def test_unpack_k8s_resource_list_invalid_list_manifest(self):
        """The input manifest must have `apiVersion`, `kind` and `items`.

        Furthermore, the `kind` *must* be capitalised and end in `List`, eg
        `DeploymentList`.

        """
        # Valid input.
        src = {'apiVersion': 'v1', 'kind': 'DeploymentList', 'items': []}
        ret = manio.unpack_k8s_resource_list(src)
        assert ret == ({}, False)

        # Missing `apiVersion`.
        src = {'kind': 'DeploymentList', 'items': []}
        assert manio.unpack_k8s_resource_list(src) == ({}, True)

        # Missing `kind`.
        src = {'apiVersion': 'v1', 'items': []}
        assert manio.unpack_k8s_resource_list(src) == ({}, True)

        # Missing `items`.
        src = {'apiVersion': 'v1', 'kind': 'DeploymentList'}
        assert manio.unpack_k8s_resource_list(src) == ({}, True)

        # All fields present but `kind` does not end in List (case sensitive).
        for invalid_kind in ('Deploymentlist', 'Deployment'):
            src_invalid = {'apiVersion': 'v1', 'kind': invalid_kind, 'items': []}
            assert manio.unpack_k8s_resource_list(src_invalid) == ({}, True)


class TestYamlManifestIO:
    def yamlfy(self, data):
        return {
            k: yaml.safe_dump_all(v, default_flow_style=False)
            for k, v in data.items()
        }

    def test_sort_manifests(self):
        """Verify the sorted output for three files with randomly ordered manifests."""
        # Convenience.
        priority = ["Namespace", "Service", "Deployment"]

        def mm(*args):
            man = make_manifest(*args)
            meta = manio.make_meta(man)
            return (meta, man)

        # Create valid MetaManifests.
        meta_ns_a = mm("Namespace", "a", "a")
        meta_ns_b = mm("Namespace", "b", "b")
        meta_svc_a = [mm("Service", "a", f"d_{_}") for _ in range(10)]
        meta_dply_a = [mm("Deployment", "a", f"d_{_}") for _ in range(10)]
        meta_svc_b = [mm("Service", "b", f"d_{_}") for _ in range(10)]
        meta_dply_b = [mm("Deployment", "b", f"d_{_}") for _ in range(10)]

        # Define manifests in the correctly grouped and sorted order for three
        # YAML files.
        sorted_manifests_1 = [
            meta_ns_a,
            meta_ns_b,
            meta_svc_a[0],
            meta_svc_a[1],
            meta_svc_b[0],
            meta_svc_b[1],
            meta_dply_a[0],
            meta_dply_a[1],
            meta_dply_b[0],
            meta_dply_b[1],
        ]
        sorted_manifests_2 = [
            meta_svc_a[0],
            meta_svc_a[1],
            meta_dply_b[0],
            meta_dply_b[1],
        ]
        sorted_manifests_3 = [
            meta_ns_a,
            meta_svc_b[0],
            meta_dply_a[1],
        ]

        # Compile input and expected output for test function.
        file_manifests: LocalManifestLists = {
            Path("m0.yaml"): sorted_manifests_1.copy(),
            Path("m1.yaml"): sorted_manifests_2.copy(),
            Path("m2.yaml"): sorted_manifests_3.copy(),
        }
        expected = {k: [man for _, man in v] for k, v in file_manifests.items()}

        # Shuffle the manifests in each file and verify that the test function
        # always produces the correct order, ie NS, SVC, DEPLOY, and all
        # manifests in each group sorted by namespace and name.
        random.seed(1)
        for _ in range(10):
            for fname in file_manifests:
                random.shuffle(cast(list, file_manifests[fname]))
            assert manio.sort_manifests(file_manifests, priority) == expected

    def test_sort_manifests_priority(self):
        """Verify the sorted output for three files with randomly ordered manifests."""
        # Convenience.
        fun = manio.sort_manifests
        fname = Path("manifests.yaml")
        random.seed(1)

        def mm(*args):
            man = make_manifest(*args)
            meta = manio.make_meta(man)
            return (meta, man)

        # Create valid MetaManifests.
        meta_ns_a = mm("Namespace", "a", "a")
        meta_ns_b = mm("Namespace", "b", "b")
        meta_svc_a = mm("Service", "a", "svc-a")
        meta_dpl_a = mm("Deployment", "a", "dpl-a")
        meta_dpl_b = mm("Deployment", "b", "dpl-b")

        # --- Define manifests in the correctly prioritised order.
        priority = ["Namespace", "Service", "Deployment"]
        sorted_manifests = [
            meta_ns_a,
            meta_ns_b,
            meta_svc_a,
            meta_svc_a,
            meta_dpl_a,
            meta_dpl_b,
        ]

        # Compile input and expected output for test function.
        file_manifests: LocalManifestLists = {fname: sorted_manifests.copy()}
        expected = {k: [man for _, man in v] for k, v in file_manifests.items()}

        # Shuffle the manifests in each file and verify the sorted output.
        for _ in range(10):
            random.shuffle(cast(list, file_manifests[fname]))
            assert fun(file_manifests, priority) == expected

        # --- Define manifests in the correctly prioritised order.
        priority = ["Service", "Namespace", "Deployment"]
        sorted_manifests = [
            meta_svc_a,
            meta_svc_a,
            meta_ns_a,
            meta_ns_b,
            meta_dpl_a,
            meta_dpl_b,
        ]

        # Compile input and expected output for test function.
        file_manifests = {fname: sorted_manifests.copy()}
        expected = {k: [man for _, man in v] for k, v in file_manifests.items()}

        # Shuffle the manifests in each file and verify the sorted output.
        for _ in range(10):
            random.shuffle(file_manifests[fname])
            assert fun(file_manifests, priority) == expected

        # --- Define manifests in the correctly prioritised order.
        priority = ["Service", "Deployment"]
        sorted_manifests = [
            meta_svc_a,
            meta_svc_a,
            meta_dpl_a,
            meta_dpl_b,
            meta_ns_a,
            meta_ns_b,
        ]

        # Compile input and expected output for test function.
        file_manifests = {fname: sorted_manifests.copy()}
        expected = {k: [man for _, man in v] for k, v in file_manifests.items()}

        # Shuffle the manifests in each file and verify the sorted output.
        for _ in range(10):
            random.shuffle(file_manifests[fname])
            assert fun(file_manifests, priority) == expected

        # --- Define manifests in the correctly prioritised order.
        sorted_manifests = [
            meta_dpl_a,
            meta_dpl_b,
            meta_ns_a,
            meta_ns_b,
            meta_svc_a,
            meta_svc_a,
        ]

        # Compile input and expected output for test function.
        file_manifests = {fname: sorted_manifests.copy()}
        expected = {k: [man for _, man in v] for k, v in file_manifests.items()}

        # Shuffle the manifests in each file and verify the sorted output.
        for _ in range(10):
            random.shuffle(file_manifests[fname])
            assert fun(file_manifests, priority=[]) == expected

    def test_parse_noselector_ok(self):
        """Test function must be able to parse the YAML string and compile a dict."""
        # Convenience.
        m1_yaml = Path("m0.yaml")
        m2_yaml = Path("m2.yaml")
        m3_yaml = Path("m3.yaml")

        # Generic selector that matches all manifests in this test.
        selectors = Selectors(kinds={"Deployment"})

        # Construct manifests like `load_files` would return them.
        kind, ns, labels = "Deployment", "namespace", {"app": "name"}
        dply = [make_manifest(kind, ns, f"d_{_}", labels) for _ in range(10)]
        meta = [manio.make_meta(_) for _ in dply]
        fdata_test_in = {
            m1_yaml: [dply[0], dply[1]],
            m2_yaml: [dply[2]],
            m3_yaml: [],
        }
        fdata_test_in = cast(Dict[Path, str], self.yamlfy(fdata_test_in))

        # We expect a dict with the same keys as the input. The dict values
        # must be a list of tuples, each of which contains the MetaManifest and
        # actual manifest as a Python dict.
        expected = {
            m1_yaml: [(meta[0], dply[0]), (meta[1], dply[1])],
            m2_yaml: [(meta[2], dply[2])],
        }
        assert manio.parse(fdata_test_in, selectors) == (expected, False)

        # Add a superfluous "---" at the beginning or end of the document. The
        # function must silently remove the empty document the YAML parser
        # would produce.
        fdata_test_blank_pre = copy.deepcopy(fdata_test_in)
        fdata_test_blank_post = copy.deepcopy(fdata_test_in)
        fdata_test_blank_post[m2_yaml] = fdata_test_blank_pre[m2_yaml] + "\n---\n"
        fdata_test_blank_pre[m2_yaml] = "\n---\n" + fdata_test_blank_pre[m2_yaml]
        out, err = manio.parse(fdata_test_blank_post, selectors)
        assert not err and len(out[m2_yaml]) == len(expected[m2_yaml])
        out, err = manio.parse(fdata_test_blank_pre, selectors)
        assert not err and len(out[m2_yaml]) == len(expected[m2_yaml])

    def test_parse_with_selector_ok(self):
        """Verify the function correctly apply the selectors."""
        # Construct manifests like `load_files` would return them.
        kind, ns = "Deployment", "namespace"
        dply = [
            make_manifest(kind, ns, f"d_{_}", {"app": f"d_{_}", "cluster": "test"})
            for _ in range(10)
        ]
        meta = [manio.make_meta(_) for _ in dply]
        fdata_test_in = {
            "m0.yaml": [dply[0], dply[1]],
            "m2.yaml": [dply[2]],
            "m3.yaml": [],
        }
        fdata_test_in = cast(Dict[Path, str], self.yamlfy(fdata_test_in))

        # We expect a dict with the same keys as the input. The dict values
        # must be a list of tuples, each of which contains the MetaManifest and
        # actual manifest as a Python dict.
        expected = {
            "m0.yaml": [(meta[0], dply[0]), (meta[1], dply[1])],
            "m2.yaml": [(meta[2], dply[2])],
        }
        # Generic selector that matches all manifests in this test. Function
        # must ignore label selectors.
        for labels in ([], ["cluster=test"]):
            selectors = Selectors(kinds={"Deployment"}, labels=labels)
            assert manio.parse(fdata_test_in, selectors) == (expected, False)

        # Same as before, but this time with an explicit namespace. Function
        # must ignore label selectors.
        for labels in ([], ["cluster=test"]):
            selectors = Selectors(kinds={"Deployment"},
                                  namespaces=["namespace"],
                                  labels=labels)
            assert manio.parse(fdata_test_in, selectors) == (expected, False)

        # Must match the same resources because the function must not apply
        # label matching, only KIND and namespace.
        selectors = Selectors(kinds={"Deployment"}, labels=["cluster=foo"])
        assert manio.parse(fdata_test_in, selectors) == (expected, False)

        # Must match nothing because we do not have a namespace "blah".
        selectors = Selectors(kinds={"Deployment"}, namespaces=["blah"],
                              labels=["cluster=test"])
        assert manio.parse(fdata_test_in, selectors) == ({}, False)

        # Must match nothing because we do not have a resource kind "blah".
        selectors = Selectors(kinds={"blah"}, namespaces=["namespace"],
                              labels=["cluster=test"])
        assert manio.parse(fdata_test_in, selectors) == ({}, False)

    def test_parse_err(self):
        """Intercept YAML decoding errors."""
        # Generic selector that matches all manifests in this test.
        selectors = Selectors(kinds={"Deployment"})

        # Construct manifests like `load_files` would return them.
        for data in ["scanner error :: - yaml", ": parser error -"]:
            assert manio.parse({Path("m0.yaml"): data}, selectors) == ({}, True)

        # Corrupt manifests (can happen when files are read from local YAML
        # files that are not actually K8s manifests). This one looks valid
        # except it misses the `metadata.name` field.
        not_a_k8s_manifest = {
            'apiVersion': 'v1',
            'kind': 'Deployment',
            'metadata': {'namespace': 'namespace'},
            'items': [{"invalid": "manifest"}]
        }
        fdata_test_in = {"m0.yaml": yaml.safe_dump(not_a_k8s_manifest)}
        fdata_test_in = cast(Dict[Path, str], fdata_test_in)
        assert manio.parse(fdata_test_in, selectors) == ({}, True)

    def test_parse_worker(self):
        """Load valid and invalid YAML strings with `_parse_worker` helper."""
        fun = manio._parse_worker
        m1_yaml = Path("m0.yaml")

        # Construct manifests like `load_files` would return them.
        for data in ["scanner error :: - yaml", ": parser error -"]:
            assert fun(m1_yaml, data) == ([], True)

        # Must return the manifest if the source string was valid.
        valid_yaml = {
            'apiVersion': 'v1',
            'kind': 'Deployment',
        }
        assert fun(m1_yaml, yaml.safe_dump(valid_yaml)) == ([valid_yaml], False)

    def test_compile_square_manifests_ok(self):
        """Test function must remove the filename dimension.

        All meta manifests are unique in this test. See
        `test_compile_square_manifests_err` for what happens if not.

        """
        meta0 = MetaManifest('apps/v1', 'Deployment', 'ns_0', 'name_0')
        meta1 = MetaManifest('apps/v1', 'Deployment', 'ns_0', 'name_1')
        meta2 = MetaManifest('apps/v1', 'Deployment', 'ns_0', 'name_2')

        src: LocalManifestLists = {
            Path("file0.txt"): [
                (meta0, {"manifest": "0"}),
                (meta1, {"manifest": "1"}),
            ],
            Path("file1.txt"): [(meta2, {"manifest": "2"})],
        }
        ret, err = manio.compile_square_manifests(src)
        assert err is False
        assert ret == {
            meta0: {"manifest": "0"},
            meta1: {"manifest": "1"},
            meta2: {"manifest": "2"},
        }

    def test_compile_square_manifests_err(self):
        """The MetaManifests must be unique across all source files."""
        meta = MetaManifest('apps/v1', 'Deployment', 'ns_0', 'name_0')

        # Two resources with same meta information in same file.
        src: LocalManifestLists = {
            Path("file0.txt"): [(meta, {"manifest": "0"}),
                                (meta, {"manifest": "0"})],
        }
        assert manio.compile_square_manifests(src) == ({}, True)

        # Two resources with same meta information in different files.
        src: LocalManifestLists = {
            Path("file0.txt"): [(meta, {"manifest": "0"})],
            Path("file1.txt"): [(meta, {"manifest": "0"})],
        }
        assert manio.compile_square_manifests(src) == ({}, True)

    def test_manifest_lifecycle(self, tmp_path):
        """Load, sync and save manifests the hard way.

        This test does not cover error scenarios. Instead, it shows how the
        individual functions in `manio` play together.

        This test does not load or save any files.

        """
        # Generic selector that matches all manifests in this test.
        selectors = Selectors(kinds={"Deployment"})
        groupby = GroupBy(order=[], label="")

        # Construct demo manifests in the same way as `load_files` would.
        dply = [mk_deploy(f"d_{_}", "nsfoo") for _ in range(10)]
        meta = [manio.make_meta(_) for _ in dply]
        fdata_test_in = {
            Path("m0.yaml"): [dply[0], dply[1], dply[2]],
            Path("m1.yaml"): [dply[3], dply[4]],
            Path("m2.yaml"): [dply[5]],
        }
        fdata_test_in = self.yamlfy(fdata_test_in)
        expected_manifests = {meta[_]: dply[_] for _ in range(6)}

        # ---------- PARSE YAML FILES ----------
        # Parse Yaml string, extract MetaManifest and compile new dict from it.
        # :: Dict[Filename:YamlStr] -> Dict[Filename:List[(MetaManifest, YamlDict)]]
        fdata_meta, err = manio.parse(fdata_test_in, selectors)
        assert err is False

        # Drop the filenames and create a dict that uses MetaManifests as keys.
        # :: Dict[Filename:List[(MetaManifest, YamlDict)]] -> Dict[MetaManifest:YamlDict]
        local_manifests, err = manio.compile_square_manifests(fdata_meta)
        assert err is False

        # Verify that the loaded manifests are correct.
        assert local_manifests == expected_manifests

        # ---------- CREATE FAKE SERVER MANIFESTS ----------
        # Create a fake set of server manifests based on `expected_manifests`.
        # In particular, pretend that K8s supplied two additional manifests,
        # lacks two others and features one with different content.
        server_manifests = expected_manifests
        del expected_manifests

        server_manifests[meta[6]] = dply[6]  # add new one
        server_manifests[meta[7]] = dply[7]  # add new one
        del server_manifests[meta[3]], server_manifests[meta[5]]  # delete two
        server_manifests[meta[1]]["metadata"] = {"new": "label"}  # modify one

        # ---------- SYNC SERVER MANIFESTS BACK TO LOCAL YAML FILES ----------
        # Sync the local manifests to match those in the server.
        # * Upsert local with server values.
        # * Delete the manifests that do not exist on the server.
        # :: Dict[MetaManifests:YamlDict] -> Dict[Filename:List[(MetaManifest, YamlDict)]]
        updated_manifests, err = manio.sync(
            fdata_meta, server_manifests,
            Selectors(kinds={"Deployment"}),
            groupby,
        )
        assert err is False

        # Convert the data to YAML. The output would normally be passed to
        # `save_files` but here we will verify it directly (see below).
        # :: Dict[Filename:List[(MetaManifest, YamlDict)]] -> Dict[Filename:YamlStr]
        assert not manio.save(tmp_path, updated_manifests, ["Deployment"])

        # Expected output after we merged back the changes (reminder: `dply[1]`
        # is different, `dply[{3,5}]` were deleted and `dply[{6,7}]` are new).
        # The new manifests must all end up in "_other.yaml" file because they
        # specify resources in the `nsfoo` namespace.
        expected = {
            Path("m0.yaml"): [dply[0], server_manifests[meta[1]], dply[2]],
            Path("m1.yaml"): [dply[4]],
            Path("_other.yaml"): [dply[6], dply[7]],
        }
        expected = self.yamlfy(expected)
        for fname, ref in expected.items():
            out = yaml.safe_load_all((tmp_path / fname).read_text())
            ref = yaml.safe_load_all(ref)
            out, ref = list(out), list(ref)
            assert ref == out


class TestCleanupCallback:
    def test_run_cleanup_callback_deployment(self, config, k8sconfig):
        """Filter DEPLOYMENT manifests."""
        # A valid DEPLOYMENT manifest with a few optional and irrelevant keys.
        manifest = {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {
                "annotations": {
                    "deployment.kubernetes.io/revision": "remove",
                    "kubectl.kubernetes.io/last-applied-configuration": "remove",
                    "keep": "this",
                },
                "name": "name",
                "namespace": "ns",
                "labels": {
                    "label1": "value1",
                },
                "keep": "this",
                "creationTimestamp": "remove",
                "resourceVersion": "remove",
                "selfLink": "remove",
                "uid": "remove",
            },
            "spec": "some spec",
            "status": "remove",
        }
        expected = {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {
                "annotations": {
                    "keep": "this",
                },
                "name": "name",
                "namespace": "ns",
                "labels": {
                    "label1": "value1",
                },
                "keep": "this",
            },
            "spec": "some spec",
        }
        out, err = manio.run_cleanup_callback(config, manifest)
        assert not err
        assert out == expected

    def test_cleanup_manifests(self, config, k8sconfig):
        """Run some basic tests."""
        # Convenience.
        fun = manio.cleanup_manifests

        man_loc = make_manifest("ClusterRole", None, "name")
        meta_loc = manio.make_meta(man_loc)
        man_srv = make_manifest("Service", "ns", "name")
        meta_srv = manio.make_meta(man_srv)
        local: SquareManifests = {meta_loc: man_loc}
        server: SquareManifests = {meta_srv: man_srv}

        # Must do nothing with empty inputs.
        assert fun(config, k8sconfig, {}, {}) == ({}, {}, False)

        # Must return input verbatim because the default filters have no effect
        # on our dummy manifest.
        assert fun(config, k8sconfig, local, server) == (local, server, False)

        # Add an annotation to our manifest and try again. This must once again
        # do nothing because the default filters do not touch the annotations.
        man_loc["metadata"]["annotations"] = {"foo": "bar"}
        assert fun(config, k8sconfig, local, server) == (local, server, False)
        assert man_loc["metadata"]["annotations"] == {"foo": "bar"}

        # ----------------------------------------------------------------------
        # Change the filter to remove all annotations. This time, the returned
        # manifest must not have its annotations anymore but they must still be
        # present in the original.
        # ----------------------------------------------------------------------
        man_loc["metadata"]["annotations"] = {"foo": "bar"}
        config.filters = {"ClusterRole": [{"metadata": ["annotations"]}]}
        ret_loc, ret_srv, err = fun(config, k8sconfig, local, server)
        assert not err

        # Annotations must have been removed in the output.
        assert "annotations" not in ret_loc[meta_loc]["metadata"]

        # Annotations must still be present in the input dictionary.
        assert "annotations" in man_loc["metadata"]

        # Must not have touched the `server` since it did not have any
        # annotations.
        assert ret_srv == server

    def test_cleanup_manifests_err(self, config, k8sconfig):
        """Force an error during a manifest cleanup."""
        # Convenience.
        fun = manio.cleanup_manifests

        # Create valid test input.
        man = make_manifest("ClusterRole", None, "name")
        meta = manio.make_meta(man)
        server: SquareManifests = {meta: man}

        # Valid input.
        assert fun(config, k8sconfig, {}, server) == ({}, server, False)

    def test_cleanup_manifests_runtime_error(self, config: Config, k8sconfig):
        """Gracefully abort if the callback function is ill behaved."""
        # Convenience.
        fun = manio.cleanup_manifests

        # Create valid test input.
        man = make_manifest("ClusterRole", None, "name")
        meta = manio.make_meta(man)
        server: SquareManifests = {meta: man}

        # Must succeed.
        assert fun(config, k8sconfig, {}, server) == ({}, server, False)

        # Callback raises an exception.
        def cb_exception(square_config, manifest):
            raise RuntimeError()

        config.clean_callback = cb_exception
        assert fun(config, k8sconfig, {}, server) == ({}, {}, True)

        # Callback provides too many return values.
        def cb_invalid_return_values(square_config, manifest):
            return (None, {}, "foo")

        config.clean_callback = cb_invalid_return_values
        assert fun(config, k8sconfig, {}, server) == ({}, {}, True)


class TestDiff:
    def test_diff_ok(self):
        """Diff two valid manifests and (roughly) verify the output."""
        # Two valid manifests.
        srv = make_manifest("Deployment", "namespace", "name1")
        loc = make_manifest("Deployment", "namespace", "name2")

        # Diff the manifests. Must not return an error.
        diff_str, err = manio.diff(loc, srv)
        assert err is False

        # Since it is difficult to compare the correct diff string due to
        # formatting characters, we will only verify that the string contains
        # a line that removes the old "names1" and one line to add "name2".
        assert "-  name: name1" in diff_str
        assert "+  name: name2" in diff_str


class TestYamlManifestIOIntegration:
    """These integration tests all write files to temporary folders."""

    def test_load_save_files(self, tmp_path):
        """Basic file loading/saving tests."""
        # Demo file names and content.
        fnames = ("m0.yaml", "m1.yaml", "foo/m2.yaml", "foo/bar/m3.yaml")
        fnames = [Path(_) for _ in fnames]
        file_data = {fname: f"Data in {fname}" for fname in fnames}

        # Asking for non-existing files must return an error.
        assert manio.load_files(tmp_path, fnames) == ({}, True)

        # Saving files to the temporary folder and loading them afterwards
        # again must work.
        assert manio.save_files(tmp_path, file_data) is False
        assert manio.load_files(tmp_path, fnames) == (file_data, False)

        # Manually verify the files.
        for fname in fnames:
            fp = tmp_path / fname
            assert fp.exists()
            assert fp.read_text() == file_data[fname]

    def test_load_save_files_empty(self, tmp_path):
        """Only non-empty files must be written."""
        # Add an empty file.
        file_data = {
            Path("empty.yaml"): "",
            Path("nonempty.yaml"): "some content",
        }

        # Saving the files. Verify that the empty one was not created.
        assert manio.save_files(tmp_path, file_data) is False
        assert (tmp_path / "nonempty.yaml").exists()
        assert not (tmp_path / "empty.yaml").exists()

    @pytest.mark.skipif(sys.platform.startswith("win"), reason="Windows")
    def test_save_err_permissions(self, tmp_path):
        """Make temp folder readonly and try to save the manifests."""
        file_data = {Path("m0.yaml"): "Some data"}
        tmp_path.chmod(0o555)
        assert manio.save_files(tmp_path, file_data) is True
        assert not (tmp_path / "m0.yaml").exists()

    @pytest.mark.skipif(sys.platform.startswith("win"), reason="Windows")
    def test_save_remove_stale_err_permissions(self, tmp_path):
        """Place a read-only YAML file into the output folder.

        The test function must abort because it will try to delete that file
        before it writes anything else.

        """
        # Create a dummy file and save an empty set of files. This must delete
        # the dummy file because the test function will first clean out all
        # stale manifests.
        tmp_file = (tmp_path / "foo.yaml")
        tmp_file.touch()
        assert manio.save_files(tmp_path, {}) is False
        assert not tmp_file.exists()

        # Create the dummy file again, then make its folder read-only. This
        # time, the function must abort with an error because it cannot delete
        # files inside a read-only folder.
        tmp_file.touch()
        tmp_path.chmod(0o555)
        assert manio.save_files(tmp_path, {}) is True
        assert tmp_file.exists()

    def test_load_manifests_save_ok(self, tmp_path):
        """Basic test that uses the {load,save} convenience functions."""
        # Generic selector that matches all manifests in this test.
        priority = ["Deployment"]
        selectors = Selectors(kinds=set(priority))

        # Create two YAML files, each with multiple manifests.
        dply = [mk_deploy(f"d_{_}") for _ in range(10)]
        meta = [manio.make_meta(mk_deploy(f"d_{_}")) for _ in range(10)]
        man_files: LocalManifestLists = {
            Path("m0.yaml"): [(meta[0], dply[0]), (meta[1], dply[1])],
            Path("foo/m1.yaml"): [(meta[2], dply[2])],
        }
        expected = (manio.compile_square_manifests(man_files)[0], man_files)
        del dply, meta

        # Save the test data, then load it back and verify.
        assert manio.save(tmp_path, man_files, priority) is False
        assert manio.load_manifests(tmp_path, selectors) == (*expected, False)

        # Glob the folder and ensure it contains exactly the files specified in
        # the `fdata_test_in` dict.
        fnames_abs = {str(tmp_path / fname) for fname in man_files.keys()}
        assert set(str(_) for _ in tmp_path.rglob("*.yaml")) == fnames_abs

        # Create non-YAML files. The `load_files` function must skip those.
        (tmp_path / "delme.txt").touch()
        (tmp_path / "foo" / "delme.txt").touch()
        assert manio.load_manifests(tmp_path, selectors) == (*expected, False)

    def test_load_manifests_save_hidden_ok(self, tmp_path):
        """Basic test that uses the {load,save} convenience functions."""
        # Generic selector that matches all manifests in this test.
        priority = ["Deployment"]
        selectors = Selectors(kinds=set(priority))

        # Create two YAML files, each with multiple manifests.
        dply = [mk_deploy(f"d_{_}") for _ in range(3)]
        meta = [manio.make_meta(mk_deploy(f"d_{_}")) for _ in range(3)]
        visible_files: LocalManifestLists = {
            Path("m0.yaml"): [(meta[0], dply[0])],
        }
        hidden_files: LocalManifestLists = {
            Path(".hidden-0.yaml"): [(meta[1], dply[1])],
            Path("foo/.hidden-1.yaml"): [(meta[2], dply[2])],
        }
        del dply, meta

        # Save the hidden and non-hidden files. The function must accept the
        # hidden files but silently ignore and not save them.
        assert manio.save(tmp_path, visible_files, priority) is False
        assert set(str(_) for _ in tmp_path.rglob("*.yaml")) == {str(tmp_path / "m0.yaml")}  # noqa
        assert manio.save(tmp_path, hidden_files, priority) is False
        assert set(str(_) for _ in tmp_path.rglob("*.yaml")) == set()

        # Write the hidden files with dummy content.
        for fname, _ in hidden_files.items():
            fname = tmp_path / fname
            fname.parent.mkdir(parents=True, exist_ok=True)
            fname.write_text("")
        assert manio.load_manifests(tmp_path, selectors) == ({}, {}, False)

    def test_save_delete_stale_yaml(self, tmp_path):
        """`save_file` must remove all excess YAML files."""
        # Generic selector that matches all manifests in this test.
        priority = ["Deployment"]
        selectors = Selectors(kinds=set(priority))

        # Create two YAML files, each with multiple manifests.
        dply = [mk_deploy(f"d_{_}") for _ in range(10)]
        meta = [manio.make_meta(mk_deploy(f"d_{_}")) for _ in range(10)]
        man_files: LocalManifestLists = {
            Path("m0.yaml"): [(meta[0], dply[0])],
            Path("m1.yaml"): [(meta[1], dply[1])],
            Path("foo/m2.yaml"): [(meta[2], dply[2])],
            Path("foo/m3.yaml"): [(meta[3], dply[3])],
            Path("bar/m4.yaml"): [(meta[4], dply[4])],
            Path("bar/m5.yaml"): [(meta[5], dply[5])],
        }
        expected = (manio.compile_square_manifests(man_files)[0], man_files)
        del dply, meta

        # Save and load the test data.
        assert manio.save(tmp_path, man_files, priority) is False
        assert manio.load_manifests(tmp_path, selectors) == (*expected, False)

        # Save a reduced set of files. Compared to `fdata_full`, it is two
        # files short and a third one ("bar/m4.yaml") is empty.
        fdata_reduced = man_files.copy()
        del fdata_reduced[Path("m0.yaml")]
        del fdata_reduced[Path("foo/m3.yaml")]
        fdata_reduced[Path("bar/m4.yaml")] = []
        expected = (manio.compile_square_manifests(fdata_reduced)[0], fdata_reduced)

        # Verify that the files still exist from the last call to `save`.
        assert (tmp_path / "m0.yaml").exists()
        assert (tmp_path / "foo/m3.yaml").exists()
        assert (tmp_path / "bar/m4.yaml").exists()

        # Save the reduced set of files.
        assert manio.save(tmp_path, fdata_reduced, priority) is False

        # Load the data. It must neither contain the files we removed from the
        # dict above, nor "bar/m4.yaml" which contained an empty manifest list.
        del fdata_reduced[Path("bar/m4.yaml")]
        assert manio.load_manifests(tmp_path, selectors) == (*expected, False)

        # Verify that the files physically do not exist anymore.
        assert not (tmp_path / "m0.yaml").exists()
        assert not (tmp_path / "foo/m3.yaml").exists()
        assert not (tmp_path / "bar/m4.yaml").exists()

    @mock.patch.object(manio, "load_files")
    def test_load_err(self, m_load, tmp_path):
        """Simulate an error in `load_files` function."""
        # Generic selector that matches all manifests in this test.
        selectors = Selectors(kinds={"Deployment"})
        m_load.return_value = ({}, True)
        assert manio.load_manifests(tmp_path, selectors) == ({}, {}, True)

    def test_save_invalid_manifest(self, tmp_path):
        """Must handle YAML errors gracefully."""
        # Create valid MetaManifests.
        meta = [manio.make_meta(mk_deploy(f"d_{_}")) for _ in range(10)]

        # Input to test function where one "manifest" is garbage that cannot be
        # converted to a YAML string, eg a Python frozenset. This assumption
        # also violates the definition of `LocalManifestLists` which is why we
        # need to tell MyPy to ignore it.
        file_manifests: LocalManifestLists = {
            Path("m0.yaml"): [
                (meta[0], {"0": "0"}),
                (meta[1], frozenset(("invalid", "input")))  # type: ignore
            ],
            Path("m1.yaml"): [(meta[2], {"2": "2"})],
        }

        # Test function must return with an error.
        assert manio.save(tmp_path, file_manifests, ["Deployment"]) is True


class TestSync:
    def test_filename_for_manifest_ok(self):
        """Verify a few valid file name conventions."""
        # Convenience.
        fun = manio.filename_for_manifest

        # Valid manifest with an "app" LABEL.
        man = make_manifest("Deployment", "ns", "name", {"app": "app"})
        meta = manio.make_meta(man)

        # No grouping - all manifests must end up in `_other.yaml`.
        groupby = GroupBy(order=[], label="")
        assert fun(meta, man, groupby) == (Path("_other.yaml"), False)

        # Group by NAMESPACE and kind.
        groupby = GroupBy(order=["ns", "kind"], label="")
        assert fun(meta, man, groupby) == (Path("ns/deployment.yaml"), False)

        # Group by KIND and NAMESPACE (inverse of previous test).
        groupby = GroupBy(order=["kind", "ns"], label="")
        assert fun(meta, man, groupby) == (Path("deployment/ns.yaml"), False)

        # Group by the existing LABEL "app".
        groupby = GroupBy(order=["label"], label="app")
        assert fun(meta, man, groupby) == (Path("app.yaml"), False)

    def test_filename_for_manifest_namespace(self):
        """Namespace related tests.

        Namespaces are special since they are not themselves namespaced.

        """
        # Convenience.
        fun = manio.filename_for_manifest

        # Valid manifest with an "app" LABEL.
        man = make_manifest("Namespace", None, "nsname", {"app": "app"})
        meta = manio.make_meta(man)

        # No grouping - all namespaces must end up in `_other.yaml`.
        groupby = GroupBy(order=[], label="")
        assert fun(meta, man, groupby) == (Path("_other.yaml"), False)

        # Group by NAMESPACE and kind.
        groupby = GroupBy(order=["ns", "kind"], label="")
        assert fun(meta, man, groupby) == (Path("nsname/namespace.yaml"), False)

        # Group by KIND and NAMESPACE (inverse of previous test).
        groupby = GroupBy(order=["kind", "ns"], label="")
        assert fun(meta, man, groupby) == (Path("namespace/nsname.yaml"), False)

        # Group by the existing LABEL "app".
        groupby = GroupBy(order=["label"], label="app")
        assert fun(meta, man, groupby) == (Path("app.yaml"), False)

    def test_filename_for_manifest_not_namespaced(self):
        """Resources that exist outside namespaces, like ClusterRole."""
        # Convenience.
        FP = Path
        fun = manio.filename_for_manifest

        # Valid manifest with an "app" LABEL.
        for kind in ("ClusterRole", "ClusterRoleBinding"):
            man = make_manifest(kind, None, "name", {"app": "app"})
            meta = manio.make_meta(man)

            # No hierarchy - all resources must end up in `_other.yaml`.
            groupby = GroupBy(order=[], label="")
            assert fun(meta, man, groupby) == (Path("_other.yaml"), False)

            # Group by NAMESPACE and kind: must use "_global_" as folder name.
            groupby = GroupBy(order=["ns", "kind"], label="")
            assert fun(meta, man, groupby) == (FP(f"_global_/{kind.lower()}.yaml"), False)

            # Group by KIND and NAMESPACE (inverse of previous test).
            groupby = GroupBy(order=["kind", "ns"], label="")
            assert fun(meta, man, groupby) == (FP(f"{kind.lower()}/_global_.yaml"), False)

            # Group by the existing LABEL "app".
            groupby = GroupBy(order=["label"], label="app")
            assert fun(meta, man, groupby) == (FP("app.yaml"), False)

    def test_filename_for_manifest_valid_but_no_label(self):
        """Consider corner cases when sorting by a non-existing label."""
        # Convenience.
        fun = manio.filename_for_manifest

        # This manifest has no label, most notably not a "app" LABEL.
        man = make_manifest("Deployment", "ns", "name", {})
        meta = manio.make_meta(man)

        # Dump everything into "_other.yaml" if LABEL "app" does not exist.
        groupby = GroupBy(order=["label"], label="app")
        assert fun(meta, man, groupby) == (Path("_other.yaml"), False)

        # Use `_all` as the name if LABEL "app" does not exist.
        groupby = GroupBy(order=["ns", "label"], label="app")
        assert fun(meta, man, groupby) == (Path("ns/_other.yaml"), False)

        groupby = GroupBy(order=["label", "ns"], label="app")
        assert fun(meta, man, groupby) == (Path("_other/ns.yaml"), False)

    def test_filename_for_manifest_err(self):
        """Verify a few invalid file name conventions."""
        # Convenience.
        fun = manio.filename_for_manifest

        # A valid manifest - content is irrelevant for this test.
        man = make_manifest("Deployment", "ns", "name", {})
        meta = manio.make_meta(man)

        # The "label" must not be a non-empty string if it is in the group.
        groupby = GroupBy(order=["label"], label="")
        assert fun(meta, man, groupby) == (Path(), True)

        # Gracefully abort for unknown types.
        groupby = GroupBy(order=["ns", "blah"], label="")
        assert fun(meta, man, groupby) == (Path(), True)

    def test_sync_modify_select_kind_and_namespace_ok(self, k8sconfig):
        """Add, modify and delete a few manifests.

        Create fake inputs for the test function, namely local- and remote
        manifests. Their structure is slightly different in that the local
        manifests still carry the file name in their data structure whereas the
        server ones do not.

        """
        def modify(manifest):
            # Return modified version of `manifest` that Square must identify
            # as different from the original.
            out = copy.deepcopy(manifest)
            out["spec"]["finalizers"].append("foo")
            return out

        # Convenience shorthand.
        fun = manio.sync
        groupby = GroupBy(order=[], label="")
        kinds = k8sconfig.kinds

        # Various MetaManifests to use in the tests.
        ns0_man = make_manifest("Namespace", None, "ns0")
        ns1_man = make_manifest("Namespace", None, "ns1")
        ns0 = manio.make_meta(ns0_man)
        ns1 = manio.make_meta(ns1_man)
        dpl_ns0_man = make_manifest("Deployment", "ns0", "d-ns0")
        dpl_ns0 = manio.make_meta(dpl_ns0_man)

        dpl_ns1_man = make_manifest("Deployment", "ns1", "d-ns1")
        dpl_ns1 = manio.make_meta(dpl_ns1_man)

        svc_ns0_man = make_manifest("Service", "ns0", "s-ns0")
        svc_ns0 = manio.make_meta(svc_ns0_man)

        svc_ns1_man = make_manifest("Service", "ns1", "s-ns0")
        svc_ns1 = manio.make_meta(svc_ns1_man)

        # The local and server manifests will remain fixed for all tests. We
        # will only vary the namespaces and resource kinds (arguments to `sync`).
        loc_man: LocalManifestLists = {
            Path("m0.yaml"): [
                (ns0, ns0_man),
                (dpl_ns0, dpl_ns0_man),
                (svc_ns0, svc_ns0_man),
                (ns1, ns1_man),
                (dpl_ns1, dpl_ns1_man),
                (svc_ns1, svc_ns1_man),
            ],
        }
        srv_man = {
            ns0: modify(ns0_man),
            dpl_ns0: modify(dpl_ns0_man),
            svc_ns0: modify(svc_ns0_man),
            ns1: modify(ns1_man),
            dpl_ns1: modify(dpl_ns1_man),
            svc_ns1: modify(svc_ns1_man),
        }

        # ----------------------------------------------------------------------
        # Special cases: empty list of resources. Must do nothing.
        # ----------------------------------------------------------------------
        expected = loc_man
        selectors = Selectors()
        assert fun(loc_man, srv_man, selectors, groupby) == (expected, False)

        selectors = Selectors()
        assert fun(loc_man, srv_man, selectors, groupby) == (expected, False)

        # NOTE: this must *not* sync the Namespace manifest from "ns1" because
        # it was not an explicitly specified resource.
        selectors = Selectors(namespaces=["ns1"])
        assert fun(loc_man, srv_man, selectors, groupby) == (expected, False)

        # ----------------------------------------------------------------------
        # Sync all namespaces implicitly (Namespaces, Deployments, Services).
        # ----------------------------------------------------------------------
        expected = {
            Path("m0.yaml"): [
                (ns0, modify(ns0_man)),
                (dpl_ns0, modify(dpl_ns0_man)),
                (svc_ns0, modify(svc_ns0_man)),
                (ns1, modify(ns1_man)),
                (dpl_ns1, modify(dpl_ns1_man)),
                (svc_ns1, modify(svc_ns1_man)),
            ],
        }, False

        # Sync the manifests. The order of `kinds` and `namespaces` must not matter.
        for kinds in itertools.permutations(["Namespace", "Deployment", "Service"]):
            kinds = set(kinds)

            # Implicitly use all namespaces.
            selectors = Selectors(kinds=kinds)
            assert fun(loc_man, srv_man, selectors, groupby) == expected

            # Specify all namespaces explicitly.
            for ns in itertools.permutations(["ns0", "ns1"]):
                selectors = Selectors(kinds=kinds, namespaces=list(ns))
                assert fun(loc_man, srv_man, selectors, groupby) == expected

        # ----------------------------------------------------------------------
        # Sync the server manifests in namespace "ns0".
        # ----------------------------------------------------------------------
        expected = {
            Path("m0.yaml"): [
                (ns0, ns0_man),
                (dpl_ns0, modify(dpl_ns0_man)),
                (svc_ns0, modify(svc_ns0_man)),
                (ns1, ns1_man),
                (dpl_ns1, dpl_ns1_man),
                (svc_ns1, svc_ns1_man),
            ],
        }, False
        for kinds in itertools.permutations(["Deployment", "Service"]):
            selectors = Selectors(kinds=set(kinds), namespaces=["ns0"])
            assert fun(loc_man, srv_man, selectors, groupby) == expected

        # ----------------------------------------------------------------------
        # Sync only Deployments (all namespaces).
        # ----------------------------------------------------------------------
        expected = {
            Path("m0.yaml"): [
                (ns0, ns0_man),
                (dpl_ns0, modify(dpl_ns0_man)),
                (svc_ns0, svc_ns0_man),
                (ns1, ns1_man),
                (dpl_ns1, modify(dpl_ns1_man)),
                (svc_ns1, svc_ns1_man),
            ],
        }, False
        selectors = Selectors(kinds={"Deployment"})
        assert fun(loc_man, srv_man, selectors, groupby) == expected

        # ----------------------------------------------------------------------
        # Sync only Deployments in namespace "ns0".
        # ----------------------------------------------------------------------
        expected = {
            Path("m0.yaml"): [
                (ns0, ns0_man),
                (dpl_ns0, modify(dpl_ns0_man)),
                (svc_ns0, svc_ns0_man),
                (ns1, ns1_man),
                (dpl_ns1, dpl_ns1_man),
                (svc_ns1, svc_ns1_man),
            ],
        }, False
        selectors = Selectors(kinds={"Deployment"}, namespaces=["ns0"])
        assert fun(loc_man, srv_man, selectors, groupby) == expected

        # ----------------------------------------------------------------------
        # Sync only Services in namespace "ns1".
        # ----------------------------------------------------------------------
        expected = {
            Path("m0.yaml"): [
                (ns0, ns0_man),
                (dpl_ns0, dpl_ns0_man),
                (svc_ns0, svc_ns0_man),
                (ns1, ns1_man),
                (dpl_ns1, dpl_ns1_man),
                (svc_ns1, modify(svc_ns1_man)),
            ],
        }, False
        selectors = Selectors(kinds={"Service"}, namespaces=["ns1"])
        assert fun(loc_man, srv_man, selectors, groupby) == expected

    @mock.patch.object(manio, "filename_for_manifest")
    def test_sync_modify_delete_ok(self, m_fname):
        """Add, modify and delete a few manifests.

        Create fake inputs for the test function, namely local- and remote
        manifests. Their structure is slightly different in that the local
        manifests still carry the file name in their data structure whereas the
        server ones do not.

        """
        def modify(manifest):
            # Return modified version of `manifest` that Square must identify
            # as different from the original.
            out = copy.deepcopy(manifest)
            out["spec"]["finalizers"].append("foo")
            return out

        groupby = GroupBy(order=[], label="")
        m_fname.return_value = (Path("catchall.yaml"), False)

        # Args for `sync`. In this test, we only have Deployments and want to
        # sync them across all namespaces.
        kinds = {"Deployment"}

        # First, create the local manifests as `load_files` would return it.
        man_1 = [mk_deploy(f"d_{_}", "ns1") for _ in range(10)]
        man_2 = [mk_deploy(f"d_{_}", "ns2") for _ in range(10)]
        meta_1 = [manio.make_meta(_) for _ in man_1]
        meta_2 = [manio.make_meta(_) for _ in man_2]
        loc_man: LocalManifestLists = {
            Path("m0.yaml"): [(meta_1[0], man_1[0]), (meta_1[1], man_1[1]),
                              (meta_2[2], man_2[2])],
            Path("m1.yaml"): [(meta_2[3], man_2[3]), (meta_1[4], man_1[4])],
            Path("m2.yaml"): [(meta_1[5], man_1[5])],
        }

        # Create server manifests as `download_manifests` would return it. Only
        # the MetaManifests (ie dict keys) are relevant whereas the dict values
        # are not but serve to improve code readability here.
        srv_man = {
            meta_1[0]: man_1[0],            # same
            meta_1[1]: modify(man_1[1]),    # modified
            meta_2[2]: man_2[2],            # same
            meta_1[4]: man_1[4],            # same
                                            # delete [5]
            meta_1[6]: man_1[6],            # new
            meta_2[7]: man_2[7],            # new
            meta_1[8]: man_1[8],            # new
        }

        # The expected outcome is that the local manifests were updated,
        # overwritten (modified), deleted or put into a default manifest.
        expected = {
            Path("m0.yaml"): [
                (meta_1[0], man_1[0]),
                (meta_1[1], modify(man_1[1])),
                (meta_2[2], man_2[2])
            ],
            Path("m1.yaml"): [(meta_1[4], man_1[4])],
            Path("m2.yaml"): [],
            Path("catchall.yaml"): [
                (meta_1[6], man_1[6]),
                (meta_2[7], man_2[7]),
                (meta_1[8], man_1[8])
            ],
        }, False
        selectors = Selectors(kinds=kinds)
        assert manio.sync(loc_man, srv_man, selectors, groupby) == expected

    def test_sync_catch_all_files(self):
        """Verify that syncing the catch-all files works as expected.

        This requires a special test to ensure these auto generated catch-all
        files behave like their "normal" user created counterparts.

        """
        def modify(manifest):
            # Return modified version of `manifest` that Square must identify
            # as different from the original.
            out = copy.deepcopy(manifest)
            out["spec"]["finalizers"].append("foo")
            return out

        # Args for `sync`. In this test, we only have Deployments and want to
        # sync them across all namespaces.
        kinds = {"Deployment"}
        groupby = GroupBy(order=[], label="")

        # First, create the local manifests as `load_files` would return it.
        # The `{0: "blah"}` like dicts are necessary because the
        # `filename_for_manifest` function requires a dict to operate on. The
        # "0" part of the dict is otherwise meaningless.
        man_1 = [mk_deploy(f"d_{_}", "ns1") for _ in range(10)]
        man_2 = [mk_deploy(f"d_{_}", "ns2") for _ in range(10)]
        meta_1 = [manio.make_meta(_) for _ in man_1]
        meta_2 = [manio.make_meta(_) for _ in man_2]
        loc_man: LocalManifestLists = {
            Path("_ns1.yaml"): [
                (meta_1[1], man_1[1]),
                (meta_1[2], man_1[2]),
                (meta_1[3], man_1[3]),
                (meta_1[5], man_1[5]),
            ],
            Path("_ns2.yaml"): [
                (meta_2[2], man_2[2]),
                (meta_2[6], man_2[6]),
            ]
        }

        # Create server manifests as `download_manifests` would return it. Only
        # the MetaManifests (ie dict keys) are relevant whereas the dict values
        # are not but serve to improve code readability here.
        srv_man = {
            # --- _ns1.yaml ---
            meta_1[0]: man_1[0],          # new
            meta_1[1]: modify(man_1[1]),  # modified
            meta_1[2]: man_1[2],          # same
                                          # delete [3]
                                          # [4] never existed
                                          # delete [5]

            # --- _ns2.yaml ---
            meta_2[0]: man_2[0],          # new
            meta_2[9]: man_2[9],          # new
            meta_2[7]: man_2[7],          # new
            meta_2[6]: modify(man_2[6]),  # modified
                                          # delete [2]
            meta_2[5]: man_2[5],          # new
            meta_2[3]: man_2[3],          # new
        }

        # The expected outcome is that the local manifests were updated,
        # either overwritten (modified), deleted or put into a default
        # manifest.
        # NOTE: this test _assumes_ that the `srv_man` dict iterates over its
        # keys in insertion order, which is guaranteed for Python 3.7.
        expected = {
            Path("_ns1.yaml"): [
                (meta_1[1], modify(man_1[1])),
                (meta_1[2], man_1[2]),
            ],
            Path("_ns2.yaml"): [
                (meta_2[6], modify(man_2[6])),
            ],
            Path("_other.yaml"): [
                (meta_1[0], man_1[0]),
                (meta_2[0], man_2[0]),
                (meta_2[9], man_2[9]),
                (meta_2[7], man_2[7]),
                (meta_2[5], man_2[5]),
                (meta_2[3], man_2[3]),
            ],
        }, False
        selectors = Selectors(kinds=kinds)
        assert manio.sync(loc_man, srv_man, selectors, groupby) == expected

    def test_sync_filename_err(self):
        """Must gracefully handle errors in `filename_for_manifest`.

        This test will use an invalid grouping specification to force an error.
        As a consequence, the function must abort cleanly and return an error.

        """
        # Valid selector for Deployment manifests.
        selectors = Selectors(kinds={"Deployment"})

        # Simulate the scenario where the server has a Deployment we lack
        # locally. This will ensure that `sync` will try to create a new file
        # for it, which is what we want to test.
        loc_man: LocalManifestLists = {}
        srv_man = {manio.make_meta(mk_deploy("d_1", "ns1")): mk_deploy("d_1", "ns1")}

        # Define an invalid grouping specification. As a consequence,
        # `filename_for_manifest` will return an error and we can verify if
        # `sync` gracefully handles it and returns an error.
        groupby = GroupBy(order=["blah"], label="")
        assert manio.sync(loc_man, srv_man, selectors, groupby) == ({}, True)

    def test_service_account_support_file(self):
        """Ensure the ServiceAccount support file has the correct setup.

        Some other tests, most notably those for the `align` function, rely on
        this.

        """
        # Fixtures.
        selectors = Selectors(kinds={"ServiceAccount"})
        name = 'demoapp'

        # Load the test support file and ensure it contains exactly one manifest.
        local_meta, _, err = manio.load_manifests(Path("tests/support/"), selectors)
        assert not err
        assert len(local_meta) == 1

        # The manifest must be a ServiceAccount with the correct name.
        meta, manifest = local_meta.copy().popitem()
        assert meta == MetaManifest(apiVersion='v1',
                                    kind='ServiceAccount',
                                    namespace='default',
                                    name=name)

        # The manifest must contain exactly one secret that starts with "demoapp-token-".
        assert "secrets" in manifest
        token = [_ for _ in manifest["secrets"] if _["name"].startswith(f"{name}-token-")]
        assert len(token) == 1 and token[0] == {"name": "demoapp-token-abcde"}

    def test_align_serviceaccount(self):
        """Align the ServiceAccount token secrets among local- and cluster
        manifest.

        This test assumes the support file with the ServiceAccount is properly
        setup (see `test_service_account_support_file`).

        """
        # Fixtures.
        selectors = Selectors(kinds={"ServiceAccount"})

        # Load and unpack the ServiceAccount manifest. Make two copies so we
        # can create local/cluster manifest as inputs for the `align` function.
        local_meta, _, _ = manio.load_manifests(Path("tests/support/"), selectors)
        local_meta = copy.deepcopy(local_meta)
        server_meta = copy.deepcopy(local_meta)
        meta, _ = local_meta.copy().popitem()

        # ----------------------------------------------------------------------
        # Server and local manifests are identical - `align` must do nothing.
        # ----------------------------------------------------------------------
        local_meta_out, err = manio.align_serviceaccount(local_meta, server_meta)
        assert not err
        assert local_meta_out == local_meta

        # ----------------------------------------------------------------------
        # The server manifest contains a token but the local one does not. This
        # is the expected use case. The `align` function must copy the server
        # token into the local manifest.
        # ----------------------------------------------------------------------
        local_meta[meta]["secrets"] = [{"name": "loc-foo"}]
        server_meta[meta]["secrets"] = [
            {"name": "srv-foo"}, {"name": "srv-bar"},
            {"name": "demoapp-token-srvfoo"}
        ]
        local_meta_out, err = manio.align_serviceaccount(local_meta, server_meta)
        assert not err
        assert local_meta_out[meta]["secrets"] == [
            {"name": "loc-foo"},
            {"name": "demoapp-token-srvfoo"}
        ]

        # ----------------------------------------------------------------------
        # The server manifest contains a token, the local one contains a
        # different one. The local token must prevail.
        # ----------------------------------------------------------------------
        local_meta[meta]["secrets"] = [
            {"name": "loc-foo"},
            {"name": "demoapp-token-locfoo"}
        ]
        server_meta[meta]["secrets"] = [
            {"name": "srv-foo"},
            {"name": "srv-bar"},
            {"name": "demoapp-token-srvfoo"}
        ]
        local_meta_out, err = manio.align_serviceaccount(local_meta, server_meta)
        assert not err
        assert local_meta_out == local_meta

        # ----------------------------------------------------------------------
        # The server manifest contains a secret token, the local does not have
        # secrets whatsoever. test is purely to ensure the function does not
        # break if the service account has no "secrets" key.
        # ----------------------------------------------------------------------
        del local_meta[meta]["secrets"]
        server_meta[meta]["secrets"] = [
            {"name": "srv-foo"},
            {"name": "srv-bar"},
            {"name": "demoapp-token-srvfoo"}
        ]
        local_meta_out, err = manio.align_serviceaccount(local_meta, server_meta)
        assert not err
        assert local_meta_out[meta]["secrets"] == [
            {"name": "demoapp-token-srvfoo"}
        ]

        # ----------------------------------------------------------------------
        # Neither server nor local manifests contain a "secrets" key. Again,
        # should be impossible.
        # ----------------------------------------------------------------------
        del server_meta[meta]["secrets"]
        local_meta_out, err = manio.align_serviceaccount(local_meta, server_meta)
        assert not err
        assert local_meta_out == local_meta

        # ----------------------------------------------------------------------
        # The server manifest does *not* contain a token - this should be
        # impossible. If it occurs, we will not modify the manifest at all.
        # ----------------------------------------------------------------------
        local_meta[meta]["secrets"] = [{"name": "loc-foo"}]
        server_meta[meta]["secrets"] = [
            {"name": "srv-foo"},
            {"name": "srv-bar"},
        ]
        local_meta_out, err = manio.align_serviceaccount(local_meta, server_meta)
        assert not err
        assert local_meta_out == local_meta

        # ----------------------------------------------------------------------
        # Server manifest has multiple token secrets - `align` must do nothing
        # because the result would be ambiguous.
        # ----------------------------------------------------------------------
        local_meta[meta]["secrets"] = [{"name": "loc-foo"}]
        server_meta[meta]["secrets"] = [
            {"name": "demoapp-token-srv1"},
            {"name": "demoapp-token-srv2"},
        ]
        local_meta_out, err = manio.align_serviceaccount(local_meta, server_meta)
        assert not err
        assert local_meta_out == local_meta

        # ----------------------------------------------------------------------
        # Local manifest has multiple token secrets - `align` must do nothing
        # because the result would be ambiguous.
        # ----------------------------------------------------------------------
        local_meta[meta]["secrets"] = [
            {"name": "demoapp-token-loc1"},
            {"name": "demoapp-token-loc2"},
        ]
        server_meta[meta]["secrets"] = [
            {"name": "demoapp-token-srv"}
        ]
        local_meta_out, err = manio.align_serviceaccount(local_meta, server_meta)
        assert not err
        assert local_meta_out == local_meta


class TestDownloadManifests:
    @mock.patch.object(k8s, 'get')
    async def test_download_ok(self, m_get, config, k8sconfig):
        """Download two kinds of manifests: Deployments and Namespaces.

        The test only mocks the K8s API call. All other functions run normally.

        """
        make_meta = manio.make_meta
        run_cleanup_callback = manio.run_cleanup_callback

        meta = [
            make_manifest("Namespace", None, "ns0"),
            make_manifest("Namespace", None, "ns1"),
            make_manifest("Deployment", "ns0", "d0"),
            make_manifest("Deployment", "ns1", "d1"),
        ]

        # Resource URLs (not namespaced).
        res_ns, err1 = resource(k8sconfig, MetaManifest("", "Namespace", None, ""))
        res_deploy, err2 = resource(k8sconfig, MetaManifest("", "Deployment", None, ""))

        # Namespaced resource URLs.
        res_ns_0, err3 = resource(k8sconfig, MetaManifest("", "Namespace", "ns0", ""))
        res_ns_1, err4 = resource(k8sconfig, MetaManifest("", "Namespace", "ns1", ""))
        res_dply_0, err5 = resource(k8sconfig, MetaManifest("", "Deployment", "ns0", ""))
        res_dply_1, err6 = resource(k8sconfig, MetaManifest("", "Deployment", "ns1", ""))
        assert not any([err1, err2, err3, err4, err5, err6])
        del err1, err2, err3, err4, err5, err6

        # NamespaceList and DeploymentList (all namespaces).
        l_ns = {'apiVersion': 'v1', 'kind': 'NamespaceList', "items": [meta[0], meta[1]]}
        l_dply = {'apiVersion': 'apps/v1', 'kind': 'DeploymentList',
                  "items": [meta[2], meta[3]]}

        # Namespaced NamespaceList and DeploymentList.
        l_ns_0 = {'apiVersion': 'v1', 'kind': 'NamespaceList', "items": [meta[0]]}
        l_ns_1 = {'apiVersion': 'v1', 'kind': 'NamespaceList', "items": [meta[1]]}
        l_dply_0 = {'apiVersion': 'apps/v1', 'kind': 'DeploymentList', "items": [meta[2]]}
        l_dply_1 = {'apiVersion': 'apps/v1', 'kind': 'DeploymentList', "items": [meta[3]]}

        # ----------------------------------------------------------------------
        # Request resources from all namespaces implicitly via namespaces=[]
        # Must make two calls: one for each kind ("Deployment", "Namespace").
        # Must silently ignore the "Unknown" resource.
        # ----------------------------------------------------------------------
        m_get.reset_mock()
        m_get.side_effect = [
            (l_ns, False),
            (l_dply, False),
        ]
        expected = {make_meta(_): run_cleanup_callback(config, _)[0] for _ in meta}
        config.selectors = Selectors(kinds={"Namespace", "Deployment", "Unknown"})
        ret = await manio.download(config, k8sconfig)
        assert ret == (expected, False)
        assert m_get.call_count == 2
        m_get.assert_any_call(k8sconfig, res_deploy.url)
        m_get.assert_any_call(k8sconfig, res_ns.url)

        # ------------------------------------------------------------------------------
        # Request resources from all namespaces explicitly via namespaces=["ns0", "ns1"]
        #
        # Must make four calls: one for each namespace ("ns0" and "ns1") and one for
        # each kind ("Deployment", "Namespace").
        # ------------------------------------------------------------------------------
        m_get.reset_mock()
        m_get.side_effect = [
            (l_ns_0, False),
            (l_dply_0, False),
            (l_ns_1, False),
            (l_dply_1, False),
        ]
        config.selectors = Selectors(kinds={"Namespace", "Deployment"},
                                     namespaces=["ns0", "ns1"])
        ret = await manio.download(config, k8sconfig)
        assert ret == (expected, False)
        assert m_get.call_count == 4
        m_get.assert_any_call(k8sconfig, res_dply_0.url)
        m_get.assert_any_call(k8sconfig, res_ns_0.url)
        m_get.assert_any_call(k8sconfig, res_dply_1.url)
        m_get.assert_any_call(k8sconfig, res_ns_1.url)

        # ----------------------------------------------------------------------
        # Request resources from namespace "ns0" only.
        #
        # Must make two calls: one for each kind ("Deployment", "Namespace") in
        # namespace "ns0".
        # ----------------------------------------------------------------------
        m_get.reset_mock()
        m_get.side_effect = [
            (l_ns_0, False),
            (l_dply_0, False),
        ]
        expected = {
            make_meta(meta[0]): run_cleanup_callback(config, meta[0])[0],
            make_meta(meta[2]): run_cleanup_callback(config, meta[2])[0],
        }
        config.selectors = Selectors(kinds={"Namespace", "Deployment"},
                                     namespaces=["ns0"])
        ret = await manio.download(config, k8sconfig)
        assert ret == (expected, False)
        assert m_get.call_count == 2
        m_get.assert_any_call(k8sconfig, res_dply_0.url)
        m_get.assert_any_call(k8sconfig, res_ns_0.url)

    @mock.patch.object(k8s, 'get')
    async def test_download_err(self, m_get, config, k8sconfig):
        """Simulate a download error."""
        # A valid NamespaceList with one element.
        man_list_ns = {
            'apiVersion': 'v1',
            'kind': 'NamespaceList',
            'items': [make_manifest("Namespace", None, "ns0")],
        }

        # The first call to get will succeed whereas the second will not.
        m_get.side_effect = [(man_list_ns, False), ({}, True)]

        # The request URLs. We will need them to validate the `get` arguments.
        res_ns, err1 = resource(k8sconfig, MetaManifest("", "Namespace", None, ""))
        res_deploy, err2 = resource(k8sconfig, MetaManifest("", "Deployment", None, ""))
        assert not err1 and not err2

        config.selectors = Selectors(kinds={"Namespace", "Deployment"})

        # Run test function and verify it returns an error and no data, despite
        # a successful `NamespaceList` download.
        ret = await manio.download(config, k8sconfig)
        assert ret == ({}, True)
        assert m_get.call_count == 2
        m_get.assert_any_call(k8sconfig, res_deploy.url)
        m_get.assert_any_call(k8sconfig, res_ns.url)
