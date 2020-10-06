import copy
import itertools
import random
import sys
import unittest.mock as mock

import pytest
import yaml

import square.dotdict as dotdict
import square.k8s as k8s
import square.manio as manio
from square.dtypes import Filepath, GroupBy, MetaManifest, Selectors
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
        # A deployment manfiest. All fields must be copied verbatim.
        manifest = make_manifest("Deployment", "namespace", "name")
        expected = MetaManifest(
            apiVersion=manifest["apiVersion"],
            kind=manifest["kind"],
            namespace="namespace",
            name=manifest["metadata"]["name"]
        )
        assert manio.make_meta(manifest) == expected

        # Namespace manifest. The "namespace" field must be populated with the
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

    def test_select(self):
        """Prune a list of manifests based on namespace and resource."""
        # Convenience.
        select = manio.select

        labels = [[], ["app=a"], ["env=e"], ["app=a", "env=e"]]

        # ---------------------------------------------------------------------
        #                      Namespace Manifest
        # ---------------------------------------------------------------------
        # Iterate over (almost) all valid selectors.
        manifest = make_manifest("Namespace", None, "name", {"app": "a", "env": "e"})
        namespaces = (["name"], ["name", "ns5"])
        kinds = (["Namespace"], ["Deployment", "Namespace"])
        for kind, ns, lab in itertools.product(kinds, namespaces, labels):
            assert select(manifest, Selectors(kind, ns, lab)) is True

        # Function must reject these because the selectors match only partially.
        selectors = [
            Selectors({"blah"}, [], []),
            Selectors({"Namespace"}, ["foo-ns"], []),
            Selectors({"Namespace"}, ["ns0"], ["app=b"]),
            Selectors({"Namespace"}, ["ns0"], {"app=a", "env=x"}),
        ]
        for sel in selectors:
            assert select(manifest, sel) is False

        # Reject the manifest if the selector does not specify at least one "kind".
        assert select(manifest, Selectors({"Namespace"}, [], [])) is True
        assert select(manifest, Selectors(set(), [], [])) is False

        # ---------------------------------------------------------------------
        #                      Deployment Manifest
        # ---------------------------------------------------------------------
        # Iterate over (almost) all valid selectors.
        manifest = make_manifest("Deployment", "my-ns", "name", {"app": "a", "env": "e"})
        kinds = ({"Deployment"}, {"Deployment", "Namespace"})
        namespaces = (["my-ns"], ["my-ns", "other-ns"])
        for kind, ns, lab in itertools.product(kinds, namespaces, labels):
            assert select(manifest, Selectors(kind, ns, lab)) is True

        # Function must reject these because the selectors match only partially.
        selectors = [
            Selectors({"blah"}, [], []),
            Selectors({"Deployment"}, ["foo-ns"], []),
            Selectors({"Deployment"}, ["ns0"], ["app=b"]),
            Selectors({"Deployment"}, ["ns0"], ["app=a", "env=x"]),
        ]
        for sel in selectors:
            assert select(manifest, sel) is False

        # Reject the manifest if the selector does not specify at least one "kind".
        assert select(manifest, Selectors({"Deployment"}, [], [])) is True
        assert select(manifest, Selectors(set(), [], [])) is False

        # ---------------------------------------------------------------------
        #                      ClusterRole Manifest
        # ---------------------------------------------------------------------
        # The namespace is irrelevant for a non-namespaced resource like ClusterRole.
        manifest = make_manifest("ClusterRole", None, "name", {"app": "a", "env": "e"})
        kinds = ({"ClusterRole"}, {"ClusterRole", "Namespace"})
        namespaces = (["my-ns"], ["my-ns", "other-ns"])
        for kind, ns, lab in itertools.product(kinds, namespaces, labels):
            assert select(manifest, Selectors(kind, ns, lab)) is True

        # Function must reject these because the selectors match only partially.
        selectors = [
            Selectors({"blah"}, [], []),
            Selectors({"Clusterrole"}, ["ns0"], ["app=b"]),
            Selectors({"Clusterrole"}, ["ns0"], ["app=a", "env=x"]),
        ]
        for sel in selectors:
            assert select(manifest, sel) is False

        # Reject the manifest if the selector does not specify at least one "kind".
        assert select(manifest, Selectors(set(), [], [])) is False

        # ---------------------------------------------------------------------
        #                    Default Service Account
        # ---------------------------------------------------------------------
        # Must always ignore "default" service account.
        kind, ns = "ServiceAccount", "ns1"
        manifest = make_manifest(kind, ns, "default")
        assert select(manifest, Selectors({kind}, {ns}, [])) is False

        # Must select all other Secret that match the selector.
        manifest = make_manifest(kind, ns, "some-service-account")
        assert select(manifest, Selectors({kind}, {ns}, [])) is True

        # ---------------------------------------------------------------------
        #                      Default Token Secret
        # ---------------------------------------------------------------------
        # Must always ignore "default-token-*" Secrets.
        kind, ns = "Secret", "ns1"
        manifest = make_manifest(kind, ns, "default-token-12345")
        assert select(manifest, Selectors({kind}, {ns}, [])) is False

        # Must select all other Secret that match the selector.
        manifest = make_manifest(kind, ns, "some-secret")
        assert select(manifest, Selectors({kind}, {ns}, [])) is True


class TestUnpackParse:
    def test_unpack_list_without_selectors_ok(self):
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

        # Generic selector that matches all manifests in this test.
        selectors = Selectors({"Deployment"}, [], [])

        # The actual DeploymentList returned from K8s.
        manifest_list = {
            'apiVersion': 'apps/v1',
            'kind': 'DeploymentList',
            'items': manifests_nokind,
        }

        # Parse the DeploymentList into a dict. The keys are ManifestTuples and
        # the values are the Deployments (*not* DeploymentList) manifests.
        data, err = manio.unpack_list(manifest_list, selectors)
        assert err is False

        # The test function must not have modified our original dictionaries,
        # which means they must still miss the "kind" field. However, it must
        # have added the correct "kind" value the manifests it returned.
        assert all(["kind" not in _ for _ in manifests_nokind])
        assert all(["kind" in v for k, v in data.items()])

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

    def test_unpack_list_with_selectors_ok(self):
        """Function must correctly apply the `selectors`."""
        # Demo manifests.
        manifests = [
            make_manifest('Deployment', f'ns_{_}', f'name_{_}',
                          {"cluster": "testing", "app": f"d_{_}"})
            for _ in range(3)
        ]

        # The actual DeploymentList returned from K8s.
        manifest_list = {
            'apiVersion': 'apps/v1',
            'kind': 'DeploymentList',
            'items': manifests,
        }

        # Select all.
        for ns in ([], ["ns_0", "ns_1", "ns_2"]):
            selectors = Selectors({"Deployment"}, ns, [])
            data, err = manio.unpack_list(manifest_list, selectors)
            assert err is False and data == {
                MetaManifest('apps/v1', 'Deployment', 'ns_0', 'name_0'): manifests[0],
                MetaManifest('apps/v1', 'Deployment', 'ns_1', 'name_1'): manifests[1],
                MetaManifest('apps/v1', 'Deployment', 'ns_2', 'name_2'): manifests[2],
            }

        # Must select nothing because no resource is in the "foo" namespace.
        selectors = Selectors({"Deployment"}, ["foo"], [])
        data, err = manio.unpack_list(manifest_list, selectors)
        assert err is False and data == {}

        # Must select nothing because we have no resource kind "foo".
        selectors = Selectors({"foo"}, [], [])
        data, err = manio.unpack_list(manifest_list, selectors)
        assert err is False and data == {}

        # Must select the Deployment in the "ns_1" namespace.
        selectors = Selectors({"Deployment"}, ["ns_1"], [])
        data, err = manio.unpack_list(manifest_list, selectors)
        assert err is False and data == {
            MetaManifest('apps/v1', 'Deployment', 'ns_1', 'name_1'): manifests[1],
        }

        # Must select the Deployment in the "ns_1" & "ns_2" namespace.
        selectors = Selectors({"Deployment"}, ["ns_1", "ns_2"], [])
        data, err = manio.unpack_list(manifest_list, selectors)
        assert err is False and data == {
            MetaManifest('apps/v1', 'Deployment', 'ns_1', 'name_1'): manifests[1],
            MetaManifest('apps/v1', 'Deployment', 'ns_2', 'name_2'): manifests[2],
        }

        # Must select the second Deployment due to label selector.
        selectors = Selectors({"Deployment"}, [], ["app=d_1"])
        data, err = manio.unpack_list(manifest_list, selectors)
        assert err is False and data == {
            MetaManifest('apps/v1', 'Deployment', 'ns_1', 'name_1'): manifests[1],
        }

    def test_unpack_list_invalid_list_manifest(self):
        """The input manifest must have `apiVersion`, `kind` and `items`.

        Furthermore, the `kind` *must* be capitalised and end in `List`, eg
        `DeploymentList`.

        """
        # Generic selector that matches all manifests in this test.
        selectors = Selectors({"Deployment"}, [], [])

        # Valid input.
        src = {'apiVersion': 'v1', 'kind': 'DeploymentList', 'items': []}
        ret = manio.unpack_list(src, selectors)
        assert ret == ({}, False)

        # Missing `apiVersion`.
        src = {'kind': 'DeploymentList', 'items': []}
        assert manio.unpack_list(src, selectors) == ({}, True)

        # Missing `kind`.
        src = {'apiVersion': 'v1', 'items': []}
        assert manio.unpack_list(src, selectors) == ({}, True)

        # Missing `items`.
        src = {'apiVersion': 'v1', 'kind': 'DeploymentList'}
        assert manio.unpack_list(src, selectors) == ({}, True)

        # All fields present but `kind` does not end in List (case sensitive).
        for invalid_kind in ('Deploymentlist', 'Deployment'):
            src = {'apiVersion': 'v1', 'kind': invalid_kind, 'items': []}
            assert manio.unpack_list(src, selectors) == ({}, True)


class TestYamlManifestIO:
    def yamlfy(self, data):
        return {
            k: yaml.safe_dump_all(v, default_flow_style=False)
            for k, v in data.items()
        }

    def test_sort_manifests(self):
        """Verify the sorted output for three files with randomly ordered manifests."""
        # Convenience.
        priority = ("Namespace", "Service", "Deployment")

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
        file_manifests = {
            "m0.yaml": sorted_manifests_1.copy(),
            "m1.yaml": sorted_manifests_2.copy(),
            "m2.yaml": sorted_manifests_3.copy(),
        }
        expected = {k: [man for _, man in v] for k, v in file_manifests.items()}

        # Shuffle the manifests in each file and verify that the test function
        # always produces the correct order, ie NS, SVC, DEPLOY, and all
        # manifests in each group sorted by namespace and name.
        random.seed(1)
        for i in range(10):
            for fname in file_manifests:
                random.shuffle(file_manifests[fname])
            assert manio.sort_manifests(file_manifests, priority) == (expected, False)

    def test_sort_manifests_priority(self):
        """Verify the sorted output for three files with randomly ordered manifests."""
        # Convenience.
        fname = "manifests.yaml"
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
        priority = ("Namespace", "Service", "Deployment")
        sorted_manifests = [
            meta_ns_a,
            meta_ns_b,
            meta_svc_a,
            meta_svc_a,
            meta_dpl_a,
            meta_dpl_b,
        ]

        # Compile input and expected output for test function.
        file_manifests = {fname: sorted_manifests.copy()}
        expected = {k: [man for _, man in v] for k, v in file_manifests.items()}

        # Shuffle the manifests in each file and verify the sorted output.
        for i in range(10):
            random.shuffle(file_manifests[fname])
            assert manio.sort_manifests(file_manifests, priority) == (expected, False)

        # --- Define manifests in the correctly prioritised order.
        priority = ("Service", "Namespace", "Deployment")
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
        for i in range(10):
            random.shuffle(file_manifests[fname])
            assert manio.sort_manifests(file_manifests, priority) == (expected, False)

        # --- Define manifests in the correctly prioritised order.
        priority = ("Service", "Deployment")
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
        for i in range(10):
            random.shuffle(file_manifests[fname])
            assert manio.sort_manifests(file_manifests, priority) == (expected, False)

        # --- Define manifests in the correctly prioritised order.
        priority = tuple()
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
        for i in range(10):
            random.shuffle(file_manifests[fname])
            assert manio.sort_manifests(file_manifests, priority) == (expected, False)

    def test_parse_noselector_ok(self):
        """Test function must be able to parse the YAML string and compile a dict."""
        # Generic selector that matches all manifests in this test.
        selectors = Selectors({"Deployment"}, [], [])

        # Construct manifests like `load_files` would return them.
        kind, ns, labels = "Deployment", "namespace", {"app": "name"}
        dply = [make_manifest(kind, ns, f"d_{_}", labels) for _ in range(10)]
        meta = [manio.make_meta(_) for _ in dply]
        fdata_test_in = {
            "m0.yaml": [dply[0], dply[1]],
            "m2.yaml": [dply[2]],
            "m3.yaml": [],
        }
        fdata_test_in = self.yamlfy(fdata_test_in)

        # We expect a dict with the same keys as the input. The dict values
        # must be a list of tuples, each of which contains the MetaManifest and
        # actual manifest as a Python dict.
        expected = {
            "m0.yaml": [(meta[0], dply[0]), (meta[1], dply[1])],
            "m2.yaml": [(meta[2], dply[2])],
        }
        assert manio.parse(fdata_test_in, selectors) == (expected, False)

        # Add a superfluous "---" at the beginning or end of the document. The
        # function must silently remove the empty document the YAML parser
        # would produce.
        fdata_test_blank_pre = copy.deepcopy(fdata_test_in)
        fdata_test_blank_post = copy.deepcopy(fdata_test_in)
        fdata_test_blank_post["m2.yaml"] = fdata_test_blank_pre["m2.yaml"] + "\n---\n"
        fdata_test_blank_pre["m2.yaml"] = "\n---\n" + fdata_test_blank_pre["m2.yaml"]
        out, err = manio.parse(fdata_test_blank_post, selectors)
        assert not err and len(out["m2.yaml"]) == len(expected["m2.yaml"])
        out, err = manio.parse(fdata_test_blank_pre, selectors)
        assert not err and len(out["m2.yaml"]) == len(expected["m2.yaml"])

    def test_parse_with_selector_ok(self):
        """Verify the the function correctly apply the selectors."""
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
        fdata_test_in = self.yamlfy(fdata_test_in)

        # We expect a dict with the same keys as the input. The dict values
        # must be a list of tuples, each of which contains the MetaManifest and
        # actual manifest as a Python dict.
        expected = {
            "m0.yaml": [(meta[0], dply[0]), (meta[1], dply[1])],
            "m2.yaml": [(meta[2], dply[2])],
        }
        # Generic selector that matches all manifests in this test.
        selectors = Selectors({"Deployment"}, [], ["cluster=test"])
        assert manio.parse(fdata_test_in, selectors) == (expected, False)

        # Same as before, but this time with the explicit namespace.
        selectors = Selectors({"Deployment"}, ["namespace"], ["cluster=test"])
        assert manio.parse(fdata_test_in, selectors) == (expected, False)

        # Must match nothing because no resource has a "cluster=foo" label.
        selectors = Selectors({"Deployment"}, [], ["cluster=foo"])
        assert manio.parse(fdata_test_in, selectors) == ({}, False)

        # Must match nothing because we do not have a namespace "blah".
        selectors = Selectors({"Deployment"}, ["blah"], ["cluster=test"])
        assert manio.parse(fdata_test_in, selectors) == ({}, False)

        # Must match nothing because we do not have a resource kind "blah".
        selectors = Selectors({"blah"}, ["namespace"], ["cluster=test"])
        assert manio.parse(fdata_test_in, selectors) == ({}, False)

        # Must match exactly one deployment due to label selector.
        selectors = Selectors({"Deployment"}, ["namespace"], ["app=d_1"])
        expected = {"m0.yaml": [(meta[1], dply[1])]}
        assert manio.parse(fdata_test_in, selectors) == (expected, False)

    def test_parse_err(self):
        """Intercept YAML decoding errors."""
        # Generic selector that matches all manifests in this test.
        selectors = Selectors({"Deployment"}, [], [])

        # Construct manifests like `load_files` would return them.
        for data in ["scanner error :: - yaml", ": parser error -"]:
            assert manio.parse({"m0.yaml": data}, selectors) == ({}, True)

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
        assert manio.parse(fdata_test_in, selectors) == ({}, True)

    def test_unpack_ok(self):
        """Test function must remove the filename dimension.

        All meta manifests are unique in this test. See `test_unpack_err` for
        what happens if not.

        """
        src = {
            "file0.txt": [("meta0", "manifest0"), ("meta1", "manifest1")],
            "file1.txt": [("meta2", "manifest2")],
        }
        ret, err = manio.unpack(src)
        assert err is False
        assert ret == {
            "meta0": "manifest0",
            "meta1": "manifest1",
            "meta2": "manifest2",
        }

    def test_unpack_err(self):
        """The MetaManifests must be unique across all source files."""
        P = Filepath

        # Two resources with same meta information in same file.
        src = {
            P("file0.txt"): [("meta0", "manifest0"), ("meta0", "manifest0")],
        }
        assert manio.unpack(src) == ({}, True)

        # Two resources with same meta information in different files.
        src = {
            P("file0.txt"): [("meta0", "manifest0")],
            P("file1.txt"): [("meta0", "manifest0")],
        }
        assert manio.unpack(src) == ({}, True)

    def test_manifest_lifecycle(self, k8sconfig, tmp_path):
        """Load, sync and save manifests the hard way.

        This test does not cover error scenarios. Instead, it shows how the
        individual functions in `manio` play together.

        This test does not load or save any files.

        """
        # Generic selector that matches all manifests in this test.
        selectors = Selectors({"Deployment"}, [], [])
        groupby = GroupBy(order=[], label="")

        # Construct demo manifests in the same way as `load_files` would.
        dply = [mk_deploy(f"d_{_}", "nsfoo") for _ in range(10)]
        meta = [manio.make_meta(_) for _ in dply]
        fdata_test_in = {
            Filepath("m0.yaml"): [dply[0], dply[1], dply[2]],
            Filepath("m1.yaml"): [dply[3], dply[4]],
            Filepath("m2.yaml"): [dply[5]],
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
        local_manifests, err = manio.unpack(fdata_meta)
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
            Selectors({"Deployment"}, namespaces=[], labels=[]),
            groupby,
        )
        assert err is False

        # Convert the data to YAML. The output would normally be passed to
        # `save_files` but here we will verify it directly (see below).
        # :: Dict[Filename:List[(MetaManifest, YamlDict)]] -> Dict[Filename:YamlStr]
        assert not manio.save(tmp_path, updated_manifests, ("Deployment", ))

        # Expected output after we merged back the changes (reminder: `dply[1]`
        # is different, `dply[{3,5}]` were deleted and `dply[{6,7}]` are new).
        # The new manifests must all end up in "_other.yaml" file because they
        # specify resources in the `nsfoo` namespace.
        expected = {
            Filepath("m0.yaml"): [dply[0], server_manifests[meta[1]], dply[2]],
            Filepath("m1.yaml"): [dply[4]],
            Filepath("_other.yaml"): [dply[6], dply[7]],
        }
        expected = self.yamlfy(expected)
        for fname, ref in expected.items():
            out = yaml.safe_load_all((tmp_path / fname).read_text())
            ref = yaml.safe_load_all(ref)
            out, ref = list(out), list(ref)
            assert ref == out


class TestManifestValidation:
    def test_strip_generic(self, k8sconfig):
        """Create a completely fake filter set to test all options.

        This test has nothing to do with real world manifests. Its only purpose
        is to validate the algorithm that strips the manifests.

        """
        # Define filters for this test.
        filters = [
            "invalid",
            {"metadata": [
                "creationTimestamp",
                {"labels": ["foo"]},
                "foo",
            ]},
            "status",
            {"spec": [{"ports": ["nodePort"]}]},
        ]
        filters = {"Service": filters}

        # Demo manifest. None of its keys matches the filter and
        # `strip` must therefore not remove anything.
        manifest = {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "name", "namespace": "ns"},
            "spec": "spec",
        }
        out, removed, err = manio.strip(k8sconfig, manifest, filters)
        assert (out, removed, err) == (manifest, {}, False)
        assert isinstance(out, dotdict.DotDict)
        del manifest

        # Demo manifest. The "labels.foo" matches the filter and must not survive.
        manifest = {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {
                "name": "name",
                "namespace": "ns",
                "labels": {"foo": "remove"}
            },
            "spec": "spec",
        }
        expected = {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {
                "name": "name",
                "namespace": "ns",
            },
            "spec": "spec",
        }
        out, removed, err = manio.strip(k8sconfig, manifest, filters)
        assert not err
        assert out == expected
        assert removed == {'metadata': {'labels': {'foo': 'remove'}}}
        del manifest

        # Valid manifest with all mandatory and *some* optional keys (
        # "metadata.labels" and "optional-2" are present, "optional-1" is
        # not).
        manifest = {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {
                "name": "mandatory",
                "namespace": "ns",
                "labels": {
                    "foo": "remove",
                    "bar": "keep",
                }
            },
            "spec": "keep",
            "status": "remove",
        }
        expected = {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {
                "name": "mandatory",
                "namespace": "ns",
                "labels": {
                    "bar": "keep",
                }
            },
            "spec": "keep",
        }
        out, removed, err = manio.strip(k8sconfig, manifest, filters)
        assert not err
        assert out == expected
        assert removed == {
            "metadata": {"labels": {"foo": "remove"}},
            "status": "remove",
        }

    def test_strip_ambigous_filters(self, k8sconfig):
        """Must cope with filters that specify the same resource multiple times."""
        # Define filters for this test.
        filters = [
            {"metadata": [
                # Specify "creationTimestamp" twice. Must behave no different
                # than if it was specified only once.
                "creationTimestamp",
                "creationTimestamp",
            ]},

            # Reference "status" twice. This must remove the entire "status" field.
            {"status": [{"foo": ["bar"]}]},
            "status",
        ]
        filters = {"Service": filters}

        # Demo manifest. None of its keys matches the filter and
        # `strip` must therefore not remove anything.
        manifest = {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "name", "namespace": "ns"},
            "spec": "spec",
        }
        out, removed, err = manio.strip(k8sconfig, manifest, filters)
        assert (out, removed, err) == (manifest, {}, False)
        assert isinstance(out, dotdict.DotDict)
        del manifest

        # Demo manifest. The "labels.creationTimestamp" matches the filter and
        # must not survive.
        manifest = {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {
                "name": "name",
                "namespace": "ns",
                "creationTimestamp": "123",
            },
            "spec": "spec",
        }
        expected = {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {
                "name": "name",
                "namespace": "ns",
            },
            "spec": "spec",
        }
        out, removed, err = manio.strip(k8sconfig, manifest, filters)
        assert not err
        assert out == expected
        assert removed == {'metadata': {'creationTimestamp': "123"}}
        del manifest

        # Valid manifest with a "status" field that must not survive.
        manifest = {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {
                "name": "mandatory",
                "namespace": "ns",
            },
            "spec": "keep",
            "status": "remove",
        }
        expected = {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {
                "name": "mandatory",
                "namespace": "ns",
            },
            "spec": "keep",
        }
        out, removed, err = manio.strip(k8sconfig, manifest, filters)
        assert not err
        assert out == expected
        assert removed == {
            "status": "remove",
        }

    def test_strip_sub_hierarchies(self, k8sconfig):
        """Remove an entire sub-tree from the manifest."""
        # Remove the "status" key, irrespective of whether it is a string, dict
        # or list in the actual manifest.
        filters = {"Service": ["status"]}

        # Minimally valid manifest.
        expected = {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "mandatory", "namespace": "ns"},
        }
        manifest = copy.deepcopy(expected)

        manifest["status"] = "string"
        out, removed, err = manio.strip(k8sconfig, manifest, filters)
        assert not err and out == expected and removed == {"status": manifest["status"]}

        manifest["status"] = None
        out, removed, err = manio.strip(k8sconfig, manifest, filters)
        assert not err and out == expected and removed == {"status": manifest["status"]}

        manifest["status"] = ["foo", "bar"]
        out, removed, err = manio.strip(k8sconfig, manifest, filters)
        assert not err and out == expected and removed == {"status": manifest["status"]}

        manifest["status"] = {"foo", "bar"}
        out, removed, err = manio.strip(k8sconfig, manifest, filters)
        assert not err and out == expected and removed == {"status": manifest["status"]}

    def test_strip_lists_simple(self, k8sconfig):
        """Filter the `NodePort` key from a list of dicts."""
        # Filter the "nodePort" element from the port list.
        filters = {"Service": [{"spec": [{"ports": ["nodePort"]}]}]}

        expected = {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "name", "namespace": "ns"},
            "spec": {"type": "NodePort"},
        }
        manifest = copy.deepcopy(expected)
        manifest["spec"]["ports"] = [
            {"nodePort": 1},
            {"nodePort": 3},
        ]
        out, removed, err = manio.strip(k8sconfig, manifest, filters)
        assert not err
        assert out == expected
        assert removed == {"spec": {"ports": [{"nodePort": 1}, {"nodePort": 3}]}}

    def test_strip_lists_service(self, k8sconfig):
        """Filter the `NodePort` key from a list of dicts."""
        # Filter the "nodePort" element from the port list.
        filters = {"Service": [{"spec": [{"ports": ["nodePort"]}]}]}

        expected = {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "name", "namespace": "ns"},
            "spec": {"type": "NodePort"},
        }
        manifest = copy.deepcopy(expected)
        manifest["spec"]["ports"] = [
            {"name": "http", "port": 81, "nodePort": 1},
            {"name": "http", "port": 82},
            {"name": "http", "port": 83, "nodePort": 3},
        ]
        expected["spec"]["ports"] = [
            {"name": "http", "port": 81},
            {"name": "http", "port": 82},
            {"name": "http", "port": 83},
        ]
        out, removed, err = manio.strip(k8sconfig, manifest, filters)
        assert not err
        assert out == expected
        assert removed == {"spec": {"ports": [{"nodePort": 1}, {"nodePort": 3}]}}

    def test_strip_default_filters(self, k8sconfig):
        """Must fall back to default filters unless otherwise specified.

        Here we expect the function to strip out the `metadata.uid` because it
        is part of the default filters (see `square/resources/defaultconfig.yaml`).

        """
        manifest = {
            "apiVersion": "v1",
            "kind": "Service",
            "something": "here",
        }
        expected = manifest.copy()

        manifest["metadata"] = {"name": "name", "namespace": "ns", "uid": "some-uid"}
        expected["metadata"] = {"name": "name", "namespace": "ns"}

        # Must remove the `metadata.uid` field.
        missing = {"metadata": {"uid": "some-uid"}}
        assert manio.strip(k8sconfig, manifest, {}) == (expected, missing, False)

    def test_strip_invalid_version_kind(self, k8sconfig):
        """Must abort gracefully for unknown K8s version or resource kind."""
        # Minimally valid filters for fake resource kind "TEST".
        filters = {"TEST": {"metadata": False}}

        # Valid K8s version with unknown resource type.
        manifest = {"apiVersion": "v1", "kind": "Unknown"}
        assert manio.strip(k8sconfig, manifest, filters) == ({}, {}, True)

        # Invalid K8s version but valid resource type.
        config = k8sconfig._replace(version="unknown")
        manifest = {"apiVersion": "v1", "kind": "TEST"}
        assert manio.strip(config, manifest, filters) == ({}, {}, True)

        # Invalid filter definition.
        config = k8sconfig._replace(version="unknown")
        manifest = {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {
                "name": "name",
                "namespace": "ns",
                "uid": "some-uid"
            }
        }
        filters = {"Service": "should-be-a-list"}
        assert manio.strip(config, manifest, filters) == ({}, {}, True)

    def test_strip_namespace(self, k8sconfig):
        """Filter NAMESPACE manifests."""
        # Valid: a namespace manifest without a `metadata.namespace` field.
        manifest = {
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {"name": "mandatory"},
        }
        assert manio.strip(k8sconfig, manifest, {}) == (manifest, {}, False)
        del manifest

        # Create invalid manifests that either specify a namespace for a
        # non-namespaced resource or vice versa. In any case, the `strip`
        # function must be smart enough to identify them.
        for kind in ("Deployment", "Service"):
            manifest = {
                "apiVersion": "v1",
                "kind": kind,
                "metadata": {
                    "name": "mandatory",
                    "namespace": "maybe",
                },
            }
            if resource(k8sconfig, MetaManifest("", kind, None, ""))[0].namespaced:
                del manifest["metadata"]["namespace"]
            assert manio.strip(k8sconfig, manifest, {}) == ({}, {}, True)

    def test_strip_deployment(self, k8sconfig):
        """Filter DEPLOYMENT manifests."""
        # A valid service manifest with a few optional and irrelevant keys.
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
        out, removed, err = manio.strip(k8sconfig, manifest, {})
        assert not err
        assert out == expected
        assert removed == {
            "metadata": {
                "annotations": {
                    "deployment.kubernetes.io/revision": "remove",
                    "kubectl.kubernetes.io/last-applied-configuration": "remove",
                },
                "creationTimestamp": "remove",
                "resourceVersion": "remove",
                "selfLink": "remove",
                "uid": "remove",
            },
            "status": "remove",
        }

        # Deployments must have a "metadata.namespace" attribute.
        manifest = {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": "mandatory"},
        }
        assert manio.strip(k8sconfig, manifest, {}) == ({}, {}, True)


class TestDiff:
    def test_diff_ok(self, config, k8sconfig):
        """Diff two valid manifests and (roughly) verify the output."""
        # Two valid manifests.
        srv = make_manifest("Deployment", "namespace", "name1")
        loc = make_manifest("Deployment", "namespace", "name2")

        # Test function must able to cope with `DotDicts`.
        srv = dotdict.make(srv)
        loc = dotdict.make(loc)

        # Diff the manifests. Must not return an error.
        diff_str, err = manio.diff(config, k8sconfig, loc, srv)
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
        file_data = {"empty.yaml": "", "nonempty.yaml": "some content"}

        # Saving the files. Verify that the empty one was not created.
        assert manio.save_files(tmp_path, file_data) is False
        assert (tmp_path / "nonempty.yaml").exists()
        assert not (tmp_path / "empty.yaml").exists()

    @pytest.mark.skipif(sys.platform.startswith("win"), reason="Windows")
    def test_save_err_permissions(self, tmp_path):
        """Make temp folder readonly and try to save the manifests."""
        file_data = {"m0.yaml": "Some data"}
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

    def test_load_save_ok(self, tmp_path):
        """Basic test that uses the {load,save} convenience functions."""
        # Generic selector that matches all manifests in this test.
        priority = ("Deployment", )
        selectors = Selectors(set(priority), [], [])

        # Create two YAML files, each with multiple manifests.
        dply = [mk_deploy(f"d_{_}") for _ in range(10)]
        meta = [manio.make_meta(mk_deploy(f"d_{_}")) for _ in range(10)]
        man_files = {
            Filepath("m0.yaml"): [(meta[0], dply[0]), (meta[1], dply[1])],
            Filepath("foo/m1.yaml"): [(meta[2], dply[2])],
        }
        expected = (manio.unpack(man_files)[0], man_files)
        del dply, meta

        # Save the test data, then load it back and verify.
        assert manio.save(tmp_path, man_files, priority) is False
        assert manio.load(tmp_path, selectors) == (*expected, False)

        # Glob the folder and ensure it contains exactly the files specified in
        # the `fdata_test_in` dict.
        fnames_abs = {str(tmp_path / fname) for fname in man_files.keys()}
        assert set(str(_) for _ in tmp_path.rglob("*.yaml")) == fnames_abs

        # Create non-YAML files. The `load_files` function must skip those.
        (tmp_path / "delme.txt").touch()
        (tmp_path / "foo" / "delme.txt").touch()
        assert manio.load(tmp_path, selectors) == (*expected, False)

    def test_load_save_hidden_ok(self, tmp_path):
        """Basic test that uses the {load,save} convenience functions."""
        # Generic selector that matches all manifests in this test.
        priority = ("Deployment", )
        selectors = Selectors(set(priority), [], [])

        # Create two YAML files, each with multiple manifests.
        dply = [mk_deploy(f"d_{_}") for _ in range(3)]
        meta = [manio.make_meta(mk_deploy(f"d_{_}")) for _ in range(3)]
        visible_files = {
            Filepath("m0.yaml"): [(meta[0], dply[0])],
        }
        hidden_files = {
            Filepath(".hidden-0.yaml"): [(meta[1], dply[1])],
            Filepath("foo/.hidden-1.yaml"): [(meta[2], dply[2])],
        }
        del dply, meta

        # Save the hidden and non-hidden files. The function must accept the
        # hidden files but silently ignore and not save them.
        assert manio.save(tmp_path, visible_files, priority) is False
        assert set(str(_) for _ in tmp_path.rglob("*.yaml")) == {str(tmp_path / "m0.yaml")}  # noqa
        assert manio.save(tmp_path, hidden_files, priority) is False
        assert set(str(_) for _ in tmp_path.rglob("*.yaml")) == set()

        # Write the hidden files with dummy content.
        for fname, data in hidden_files.items():
            fname = tmp_path / fname
            fname.parent.mkdir(parents=True, exist_ok=True)
            fname.write_text("")
        assert manio.load(tmp_path, selectors) == ({}, {}, False)

    def test_save_delete_stale_yaml(self, tmp_path):
        """`save_file` must remove all excess YAML files."""
        # Generic selector that matches all manifests in this test.
        priority = ("Deployment", )
        selectors = Selectors(set(priority), [], [])

        # Create two YAML files, each with multiple manifests.
        dply = [mk_deploy(f"d_{_}") for _ in range(10)]
        meta = [manio.make_meta(mk_deploy(f"d_{_}")) for _ in range(10)]
        man_files = {
            Filepath("m0.yaml"): [(meta[0], dply[0])],
            Filepath("m1.yaml"): [(meta[1], dply[1])],
            Filepath("foo/m2.yaml"): [(meta[2], dply[2])],
            Filepath("foo/m3.yaml"): [(meta[3], dply[3])],
            Filepath("bar/m4.yaml"): [(meta[4], dply[4])],
            Filepath("bar/m5.yaml"): [(meta[5], dply[5])],
        }
        expected = (manio.unpack(man_files)[0], man_files)
        del dply, meta

        # Save and load the test data.
        assert manio.save(tmp_path, man_files, priority) is False
        assert manio.load(tmp_path, selectors) == (*expected, False)

        # Save a reduced set of files. Compared to `fdata_full`, it is two
        # files short and a third one ("bar/m4.yaml") is empty.
        fdata_reduced = man_files.copy()
        del fdata_reduced[Filepath("m0.yaml")]
        del fdata_reduced[Filepath("foo/m3.yaml")]
        fdata_reduced[Filepath("bar/m4.yaml")] = []
        expected = (manio.unpack(fdata_reduced)[0], fdata_reduced)

        # Verify that the files still exist from the last call to `save`.
        assert (tmp_path / "m0.yaml").exists()
        assert (tmp_path / "foo/m3.yaml").exists()
        assert (tmp_path / "bar/m4.yaml").exists()

        # Save the reduced set of files.
        assert manio.save(tmp_path, fdata_reduced, priority) is False

        # Load the data. It must neither contain the files we removed from the
        # dict above, nor "bar/m4.yaml" which contained an empty manifest list.
        del fdata_reduced[Filepath("bar/m4.yaml")]
        assert manio.load(tmp_path, selectors) == (*expected, False)

        # Verify that the files physically do not exist anymore.
        assert not (tmp_path / "m0.yaml").exists()
        assert not (tmp_path / "foo/m3.yaml").exists()
        assert not (tmp_path / "bar/m4.yaml").exists()

    @mock.patch.object(manio, "load_files")
    def test_load_err(self, m_load, tmp_path):
        """Simulate an error in `load_files` function."""
        # Generic selector that matches all manifests in this test.
        selectors = Selectors({"Deployment"}, [], [])
        m_load.return_value = ({}, True)
        assert manio.load(tmp_path, selectors) == ({}, {}, True)

    def test_save_invalid_manifest(self, tmp_path):
        """Must handle YAML errors gracefully."""
        # Create valid MetaManifests.
        meta = [manio.make_meta(mk_deploy(f"d_{_}")) for _ in range(10)]

        # Input to test function where one "manifest" is garbage that cannot be
        # converted to a YAML string, eg a Python frozenset.
        file_manifests = {
            Filepath("m0.yaml"): [(meta[0], "0"),
                                  (meta[1], frozenset(("invalid", "input")))],
            Filepath("m1.yaml"): [(meta[2], "2")],
        }

        # Test function must return with an error.
        assert manio.save(tmp_path, file_manifests, ("Deployment", )) is True


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
        assert fun(meta, man, groupby) == (Filepath("_other.yaml"), False)

        # Group by NAMESPACE and kind.
        groupby = GroupBy(order=["ns", "kind"], label="")
        assert fun(meta, man, groupby) == (Filepath("ns/deployment.yaml"), False)

        # Group by KIND and NAMESPACE (inverse of previous test).
        groupby = GroupBy(order=["kind", "ns"], label="")
        assert fun(meta, man, groupby) == (Filepath("deployment/ns.yaml"), False)

        # Group by the existing LABEL "app".
        groupby = GroupBy(order=["label"], label="app")
        assert fun(meta, man, groupby) == (Filepath("app.yaml"), False)

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
        assert fun(meta, man, groupby) == (Filepath("_other.yaml"), False)

        # Group by NAMESPACE and kind.
        groupby = GroupBy(order=["ns", "kind"], label="")
        assert fun(meta, man, groupby) == (Filepath("nsname/namespace.yaml"), False)

        # Group by KIND and NAMESPACE (inverse of previous test).
        groupby = GroupBy(order=["kind", "ns"], label="")
        assert fun(meta, man, groupby) == (Filepath("namespace/nsname.yaml"), False)

        # Group by the existing LABEL "app".
        groupby = GroupBy(order=["label"], label="app")
        assert fun(meta, man, groupby) == (Filepath("app.yaml"), False)

    def test_filename_for_manifest_not_namespaced(self):
        """Resources that exist outside namespaces, like ClusterRole."""
        # Convenience.
        FP = Filepath
        fun = manio.filename_for_manifest

        # Valid manifest with an "app" LABEL.
        for kind in ("ClusterRole", "ClusterRoleBinding"):
            man = make_manifest(kind, None, "name", {"app": "app"})
            meta = manio.make_meta(man)

            # No hierarchy - all resources must end up in `_other.yaml`.
            groupby = GroupBy(order=[], label="")
            assert fun(meta, man, groupby) == (Filepath("_other.yaml"), False)

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
        assert fun(meta, man, groupby) == (Filepath("_other.yaml"), False)

        # Use `_all` as the name if LABEL "app" does not exist.
        groupby = GroupBy(order=["ns", "label"], label="app")
        assert fun(meta, man, groupby) == (Filepath("ns/_other.yaml"), False)

        groupby = GroupBy(order=["label", "ns"], label="app")
        assert fun(meta, man, groupby) == (Filepath("_other/ns.yaml"), False)

    def test_filename_for_manifest_err(self):
        """Verify a few invalid file name conventions."""
        # Convenience.
        fun = manio.filename_for_manifest

        # A valid manifest - content is irrelevant for this test.
        man = make_manifest("Deployment", "ns", "name", {})
        meta = manio.make_meta(man)

        # The "label" must not be a non-empty string if it is in the group.
        groupby = GroupBy(order=["label"], label="")
        assert fun(meta, man, groupby) == (Filepath(), True)

        # Gracefully abort for unknown types.
        groupby = GroupBy(order=["ns", "blah"], label="")
        assert fun(meta, man, groupby) == (Filepath(), True)

    def test_sync_modify_selective_kind_and_namespace_ok(self, k8sconfig):
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
        loc_man = {
            "m0.yaml": [
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
        selectors = Selectors(set(), namespaces=[], labels=[])
        assert fun(loc_man, srv_man, selectors, groupby) == (expected, False)

        selectors = Selectors(set(), namespaces=[], labels=[])
        assert fun(loc_man, srv_man, selectors, groupby) == (expected, False)

        # NOTE: this must *not* sync the Namespace manifest from "ns1" because
        # it was not an explicitly specified resource.
        selectors = Selectors(set(), namespaces=["ns1"], labels=[])
        assert fun(loc_man, srv_man, selectors, groupby) == (expected, False)

        # ----------------------------------------------------------------------
        # Sync all namespaces implicitly (Namespaces, Deployments, Services).
        # ----------------------------------------------------------------------
        expected = {
            "m0.yaml": [
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
            selectors = Selectors(kinds, namespaces=[], labels=[])
            assert fun(loc_man, srv_man, selectors, groupby) == expected

            # Specify all namespaces explicitly.
            for ns in itertools.permutations(["ns0", "ns1"]):
                selectors = Selectors(kinds, namespaces=ns, labels=[])
                assert fun(loc_man, srv_man, selectors, groupby) == expected

        # ----------------------------------------------------------------------
        # Sync the server manifests in namespace "ns0".
        # ----------------------------------------------------------------------
        expected = {
            "m0.yaml": [
                (ns0, ns0_man),
                (dpl_ns0, modify(dpl_ns0_man)),
                (svc_ns0, modify(svc_ns0_man)),
                (ns1, ns1_man),
                (dpl_ns1, dpl_ns1_man),
                (svc_ns1, svc_ns1_man),
            ],
        }, False
        for kinds in itertools.permutations(["Deployment", "Service"]):
            kinds = set(kinds)
            selectors = Selectors(kinds, namespaces=["ns0"], labels=[])
            assert fun(loc_man, srv_man, selectors, groupby) == expected

        # ----------------------------------------------------------------------
        # Sync only Deployments (all namespaces).
        # ----------------------------------------------------------------------
        expected = {
            "m0.yaml": [
                (ns0, ns0_man),
                (dpl_ns0, modify(dpl_ns0_man)),
                (svc_ns0, svc_ns0_man),
                (ns1, ns1_man),
                (dpl_ns1, modify(dpl_ns1_man)),
                (svc_ns1, svc_ns1_man),
            ],
        }, False
        selectors = Selectors({"Deployment"}, namespaces=[], labels=[])
        assert fun(loc_man, srv_man, selectors, groupby) == expected

        # ----------------------------------------------------------------------
        # Sync only Deployments in namespace "ns0".
        # ----------------------------------------------------------------------
        expected = {
            "m0.yaml": [
                (ns0, ns0_man),
                (dpl_ns0, modify(dpl_ns0_man)),
                (svc_ns0, svc_ns0_man),
                (ns1, ns1_man),
                (dpl_ns1, dpl_ns1_man),
                (svc_ns1, svc_ns1_man),
            ],
        }, False
        selectors = Selectors({"Deployment"}, namespaces=["ns0"], labels=[])
        assert fun(loc_man, srv_man, selectors, groupby) == expected

        # ----------------------------------------------------------------------
        # Sync only Services in namespace "ns1".
        # ----------------------------------------------------------------------
        expected = {
            "m0.yaml": [
                (ns0, ns0_man),
                (dpl_ns0, dpl_ns0_man),
                (svc_ns0, svc_ns0_man),
                (ns1, ns1_man),
                (dpl_ns1, dpl_ns1_man),
                (svc_ns1, modify(svc_ns1_man)),
            ],
        }, False
        selectors = Selectors({"Service"}, namespaces=["ns1"], labels=[])
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
        m_fname.return_value = ("catchall.yaml", False)

        # Args for `sync`. In this test, we only have Deployments and want to
        # sync them across all namespaces.
        kinds, namespaces = {"Deployment"}, []

        # First, create the local manifests as `load_files` would return it.
        man_1 = [mk_deploy(f"d_{_}", "ns1") for _ in range(10)]
        man_2 = [mk_deploy(f"d_{_}", "ns2") for _ in range(10)]
        meta_1 = [manio.make_meta(_) for _ in man_1]
        meta_2 = [manio.make_meta(_) for _ in man_2]
        loc_man = {
            "m0.yaml": [(meta_1[0], man_1[0]), (meta_1[1], man_1[1]),
                        (meta_2[2], man_2[2])],
            "m1.yaml": [(meta_2[3], man_2[3]), (meta_1[4], man_1[4])],
            "m2.yaml": [(meta_1[5], man_1[5])],
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
            "m0.yaml": [(meta_1[0], man_1[0]), (meta_1[1], modify(man_1[1])),
                        (meta_2[2], man_2[2])],
            "m1.yaml": [(meta_1[4], man_1[4])],
            "m2.yaml": [],
            "catchall.yaml": [(meta_1[6], man_1[6]), (meta_2[7], man_2[7]),
                              (meta_1[8], man_1[8])],
        }, False
        selectors = Selectors(kinds, namespaces, labels=[])
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
        kinds, namespaces = {"Deployment"}, []
        groupby = GroupBy(order=[], label="")

        # First, create the local manifests as `load_files` would return it.
        # The `{0: "blah"}` like dicts are necessary because the
        # `filename_for_manifest` function requires a dict to operate on. The
        # "0" part of the dict is otherwise meaningless.
        man_1 = [mk_deploy(f"d_{_}", "ns1") for _ in range(10)]
        man_2 = [mk_deploy(f"d_{_}", "ns2") for _ in range(10)]
        meta_1 = [manio.make_meta(_) for _ in man_1]
        meta_2 = [manio.make_meta(_) for _ in man_2]
        loc_man = {
            Filepath("_ns1.yaml"): [
                (meta_1[1], man_1[1]),
                (meta_1[2], man_1[2]),
                (meta_1[3], man_1[3]),
                (meta_1[5], man_1[5]),
            ],
            Filepath("_ns2.yaml"): [
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
            Filepath("_ns1.yaml"): [
                (meta_1[1], modify(man_1[1])),
                (meta_1[2], man_1[2]),
            ],
            Filepath("_ns2.yaml"): [
                (meta_2[6], modify(man_2[6])),
            ],
            Filepath("_other.yaml"): [
                (meta_1[0], man_1[0]),
                (meta_2[0], man_2[0]),
                (meta_2[9], man_2[9]),
                (meta_2[7], man_2[7]),
                (meta_2[5], man_2[5]),
                (meta_2[3], man_2[3]),
            ],
        }, False
        selectors = Selectors(kinds, namespaces, labels=[])
        assert manio.sync(loc_man, srv_man, selectors, groupby) == expected

    def test_sync_filename_err(self, k8sconfig):
        """Must gracefully handle errors in `filename_for_manifest`.

        This test will use an invalid grouping specification to force an error.
        As a consequence, the function must abort cleanly and return an error.

        """
        # Valid selector for Deployment manifests.
        selectors = Selectors({"Deployment"}, namespaces=[], labels=[])

        # Simulate the scenario where the server has a Deployment we lack
        # locally. This will ensure that `sync` will try to create a new file
        # for it, which is what we want to test.
        loc_man = {}
        srv_man = {manio.make_meta(mk_deploy("d_1", "ns1")): mk_deploy("d_1", "ns1")}

        # Define an invalid grouping specification. As a consequence,
        # `filename_for_manifest` will return an error and we can verify if
        # `sync` gracefully handles it and returns an error.
        groupby = GroupBy(order=["blah"], label="")
        assert manio.sync(loc_man, srv_man, selectors, groupby) == ({}, True)

    def test_service_account_support_file(self):
        """Ensure the ServiceAccount support file has the correct setup.

        Some other tests, most notably those for the `align` function, rely on this.
        """
        # Fixtures.
        selectors = Selectors({"ServiceAccount"}, namespaces=[], labels=[])
        name = 'demoapp'

        # Load the test support file and ensure it contains exactly one manifest.
        local_meta, _, err = manio.load("tests/support/", selectors)
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
        selectors = Selectors({"ServiceAccount"}, namespaces=[], labels=[])

        # Load and unpack the ServiceAccount manifest. Make two copies so we
        # can create local/cluster manifest as inputs for the `align` function.
        local_meta, _, _ = manio.load("tests/support/", selectors)
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
    def test_download_ok(self, m_get, config, k8sconfig):
        """Download two kinds of manifests: Deployments and Namespaces.

        The test only mocks the K8s API call. All other functions actually run.

        """
        make_meta = manio.make_meta

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
        expected = {make_meta(_): manio.strip(k8sconfig, _, {})[0] for _ in meta}
        config.selectors = Selectors({"Namespace", "Deployment", "Unknown"}, [], [])
        ret = manio.download(config, k8sconfig)
        assert ret == (expected, False)
        assert m_get.call_args_list == [
            mock.call(k8sconfig.client, res_deploy.url),
            mock.call(k8sconfig.client, res_ns.url),
        ]

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
        config.selectors = Selectors({"Namespace", "Deployment"}, ["ns0", "ns1"], [])
        ret = manio.download(config, k8sconfig)
        assert ret == (expected, False)
        assert m_get.call_args_list == [
            mock.call(k8sconfig.client, res_dply_0.url),
            mock.call(k8sconfig.client, res_ns_0.url),
            mock.call(k8sconfig.client, res_dply_1.url),
            mock.call(k8sconfig.client, res_ns_1.url),
        ]

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
            make_meta(meta[0]): manio.strip(k8sconfig, meta[0], {})[0],
            make_meta(meta[2]): manio.strip(k8sconfig, meta[2], {})[0],
        }
        config.selectors = Selectors({"Namespace", "Deployment"}, ["ns0"], [])
        ret = manio.download(config, k8sconfig)
        assert ret == (expected, False)
        assert m_get.call_args_list == [
            mock.call(k8sconfig.client, res_dply_0.url),
            mock.call(k8sconfig.client, res_ns_0.url),
        ]

    @mock.patch.object(k8s, 'get')
    def test_download_err(self, m_get, config, k8sconfig):
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

        config.selectors = Selectors({"Namespace", "Deployment"}, [], [])

        # Run test function and verify it returns an error and no data, despite
        # a successful `NamespaceList` download.
        ret = manio.download(config, k8sconfig)
        assert ret == ({}, True)
        assert m_get.call_args_list == [
            mock.call(k8sconfig.client, res_deploy.url),
            mock.call(k8sconfig.client, res_ns.url),
        ]

    @mock.patch.object(k8s, 'get')
    def test_download_single(self, m_get, k8sconfig):
        """Download a single manifest.

        The test only mocks the K8s API call.

        """
        manifest = make_manifest("Deployment", "ns", "name")
        meta = manio.make_meta(manifest)
        assert meta == MetaManifest("apps/v1", "Deployment", "ns", "name")

        # Mock the K8s response to return a valid manifest.
        m_get.return_value = (manifest, False)

        # Resource URLs.
        res, err = resource(k8sconfig, meta)
        assert not err

        # Test function must successfully download the resource.
        assert manio.download_single(k8sconfig, res) == (meta, manifest, False)

        # Simulate a download error.
        m_get.return_value = (manifest, True)
        ret = manio.download_single(k8sconfig, res)
        assert ret == (MetaManifest("", "", "", ""), {}, True)
