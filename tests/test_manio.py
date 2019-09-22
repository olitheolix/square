import copy
import itertools
import random
import unittest.mock as mock

import square.dotdict as dotdict
import square.k8s as k8s
import square.manio as manio
import square.schemas as schemas
import square.square as square
import yaml
from square.dtypes import (
    SUPPORTED_KINDS, SUPPORTED_VERSIONS, MetaManifest, Selectors,
)
from square.k8s import urlpath

from .test_helpers import make_manifest, mk_deploy


class TestHelpers:
    @classmethod
    def setup_class(cls):
        square.setup_logging(9)

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
            namespace="name",
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


class TestUnpackParse:
    @classmethod
    def setup_class(cls):
        square.setup_logging(9)

    def test_unpack_list_ok(self):
        """Convert eg a DeploymentList into a Python dict of Deployments."""
        # Demo manifests.
        manifests = [
            make_manifest('Deployment', f'ns_{_}', f'name_{_}')
            for _ in range(3)
        ]

        # The actual DeploymentList returned from K8s.
        manifest_list = {
            'apiVersion': 'v1',
            'kind': 'DeploymentList',
            'items': manifests,
        }

        # Parse the DeploymentList into a dict. The keys are ManifestTuples and
        # the values are the Deployment (*not* DeploymentList) manifests.
        data, err = manio.unpack_list(manifest_list)
        assert err is False

        # Verify the Python dict.
        assert len(manifests) == 3
        assert data == {
            MetaManifest('v1', 'Deployment', 'ns_0', 'name_0'): manifests[0],
            MetaManifest('v1', 'Deployment', 'ns_1', 'name_1'): manifests[1],
            MetaManifest('v1', 'Deployment', 'ns_2', 'name_2'): manifests[2],
        }

        # Function must return deep copies of the manifests to avoid difficult
        # to debug reference bugs.
        for src, out_key in zip(manifests, data):
            assert src == data[out_key]
            assert src is not data[out_key]

    def test_unpack_list_invalid_list_manifest(self):
        """The input manifest must have `apiVersion`, `kind` and `items`.

        Furthermore, the `kind` *must* be capitalised and end in `List`, eg
        `DeploymentList`.

        """
        # Valid input.
        src = {'apiVersion': 'v1', 'kind': 'DeploymentList', 'items': []}
        ret = manio.unpack_list(src)
        assert ret == ({}, False)

        # Missing `apiVersion`.
        src = {'kind': 'DeploymentList', 'items': []}
        assert manio.unpack_list(src) == (None, True)

        # Missing `kind`.
        src = {'apiVersion': 'v1', 'items': []}
        assert manio.unpack_list(src) == (None, True)

        # Missing `items`.
        src = {'apiVersion': 'v1', 'kind': 'DeploymentList'}
        assert manio.unpack_list(src) == (None, True)

        # All fields present but `kind` does not end in List (case sensitive).
        for invalid_kind in ('Deploymentlist', 'Deployment'):
            src = {'apiVersion': 'v1', 'kind': invalid_kind, 'items': []}
            assert manio.unpack_list(src) == (None, True)


class TestYamlManifestIO:
    @classmethod
    def setup_class(cls):
        square.setup_logging(9)

    def yamlfy(self, data):
        return {
            k: yaml.safe_dump_all(v, default_flow_style=False)
            for k, v in data.items()
        }

    def test_parse_ok(self):
        """Test function must be able to parse the Yaml string and compile a dict."""
        # Construct manifests in the way as `load_files` returns them.
        dply = [mk_deploy(f"d_{_}") for _ in range(10)]
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
        assert manio.parse(fdata_test_in) == (expected, False)

        # Add a superfluous "---" at the beginning/end of the document. This
        # mast not throw the parser and it must not include it in the output as
        # an empty document.
        fdata_test_blank_pre = copy.deepcopy(fdata_test_in)
        fdata_test_blank_post = copy.deepcopy(fdata_test_in)
        fdata_test_blank_post["m2.yaml"] = fdata_test_blank_pre["m2.yaml"] + "\n---\n"
        fdata_test_blank_pre["m2.yaml"] = "\n---\n" + fdata_test_blank_pre["m2.yaml"]
        out, err = manio.parse(fdata_test_blank_post)
        assert not err and len(out["m2.yaml"]) == len(expected["m2.yaml"])
        out, err = manio.parse(fdata_test_blank_pre)
        assert not err and len(out["m2.yaml"]) == len(expected["m2.yaml"])

    def test_parse_err(self):
        """Intercept YAML decoding errors."""
        # Construct manifests in the way as `load_files` returns them.
        fdata_test_in = {"m0.yaml": "invalid :: - yaml"}
        assert manio.parse(fdata_test_in) == (None, True)

        # Corrupt manifests (can happen when files are read from local YAML
        # files that are not actually K8s manifests).
        not_a_k8s_manifest = {
            'apiVersion': 'v1', 'kind': 'DeploymentList',
            'items': [{"invalid": "manifest"}]
        }
        fdata_test_in = {"m0.yaml": yaml.safe_dump(not_a_k8s_manifest)}
        assert manio.parse(fdata_test_in) == (None, True)

    def test_unpack_ok(self):
        """Test function must remove the filename dimension.

        All meta manifests are unique in this test. See `test_unpakc_err` for
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

        # Two resources with same meta information in same file.
        src = {
            "file0.txt": [("meta0", "manifest0"), ("meta0", "manifest0")],
        }
        assert manio.unpack(src) == (None, True)

        # Two resources with same meta information in different files.
        src = {
            "file0.txt": [("meta0", "manifest0")],
            "file1.txt": [("meta0", "manifest0")],
        }
        assert manio.unpack(src) == (None, True)

    def test_unparse_ok(self):
        """Basic use case: convert Python dicts to YAML strings."""
        # Create valid MetaManifests.
        meta = [manio.make_meta(mk_deploy(f"d_{_}")) for _ in range(10)]

        # Input to test function and expected output.
        file_manifests = {
            "m0.yaml": [(meta[0], "0"), (meta[1], "1")],
            "m1.yaml": [(meta[2], "2")],
        }
        expected = self.yamlfy({
            "m0.yaml": ["0", "1"],
            "m1.yaml": ["2"],
        })

        # Run the tests.
        assert manio.unparse(file_manifests) == (expected, False)

    def test_unparse_sorted_ok(self):
        """The manifests in each file must be grouped and sorted.

        Every YAML file we create must group all resource types in this order:
        namespaces, services, deployment.

        All resources must be sorted by name inside each group.

        """
        def mm(*args):
            return manio.make_meta(make_manifest(*args))

        # Create valid MetaManifests.
        meta_ns_a = mm("Namespace", "a", None)
        meta_ns_b = mm("Namespace", "b", None)
        meta_svc_a = [mm("Service", "a", f"d_{_}") for _ in range(10)]
        meta_dply_a = [mm("Deployment", "a", f"d_{_}") for _ in range(10)]
        meta_svc_b = [mm("Service", "b", f"d_{_}") for _ in range(10)]
        meta_dply_b = [mm("Deployment", "b", f"d_{_}") for _ in range(10)]

        # Define manifests in the correctly grouped and sorted order for three
        # YAML files.
        sorted_manifests_1 = [
            (meta_ns_a, "ns_a"),
            (meta_ns_b, "ns_b"),
            (meta_svc_a[0], "svc_a_0"),
            (meta_svc_a[1], "svc_a_1"),
            (meta_svc_b[0], "svc_b_0"),
            (meta_svc_b[1], "svc_b_1"),
            (meta_dply_a[0], "dply_a_0"),
            (meta_dply_a[1], "dply_a_1"),
            (meta_dply_b[0], "dply_b_0"),
            (meta_dply_b[1], "dply_b_1"),
        ]
        sorted_manifests_2 = [
            (meta_svc_a[0], "svc_a_0"),
            (meta_svc_a[1], "svc_a_1"),
            (meta_dply_b[0], "dply_b_0"),
            (meta_dply_b[1], "dply_b_1"),
        ]
        sorted_manifests_3 = [
            (meta_ns_a, "ns_a"),
            (meta_svc_b[0], "svc_b_0"),
            (meta_dply_a[1], "dply_a_1"),
        ]

        # Compile input and expected output for test function.
        file_manifests = {
            "m0.yaml": sorted_manifests_1.copy(),
            "m1.yaml": sorted_manifests_2.copy(),
            "m2.yaml": sorted_manifests_3.copy(),
        }
        expected = {k: [man for _, man in v] for k, v in file_manifests.items()}
        expected = self.yamlfy(expected)

        # Shuffle the manifests in each file and verify that the test function
        # always produces the correct order, ie NS, SVC, DEPLOY, and all
        # manifests in each group sorted by namespace and name.
        for i in range(10):
            for fname in file_manifests:
                random.shuffle(file_manifests[fname])
            assert manio.unparse(file_manifests) == (expected, False)

    def test_unparse_invalid_manifest(self):
        """Must handle YAML errors gracefully."""
        # Create valid MetaManifests.
        meta = [manio.make_meta(mk_deploy(f"d_{_}")) for _ in range(10)]

        # Input to test function where one "manifest" is garbage that cannot be
        # converted to a YAML string, eg a Python frozenset.
        file_manifests = {
            "m0.yaml": [(meta[0], "0"), (meta[1], frozenset(("invalid", "input")))],
            "m1.yaml": [(meta[2], "2")],
        }

        # Test function must return with an error.
        assert manio.unparse(file_manifests) == (None, True)

    def test_unparse_unknown_kinds(self):
        """Must handle unknown resource kinds gracefully."""
        invalid_kinds = (
            "deployment",       # wrong capitalisation
            "DEPLOYMENT",       # wrong capitalisation
            "foo",              # unknown
            "Pod",              # we do not support Pod manifests
        )

        # Convenience.
        def mm(*args):
            return manio.make_meta(make_manifest(*args))

        # Test function must gracefully reject all invalid kinds.
        for kind in invalid_kinds:
            file_manifests = {"m0.yaml": [(mm(kind, "ns", "name"), "0")]}
            assert manio.unparse(file_manifests) == (None, True)

    def test_unparse_known_kinds(self):
        """Must handle all known resource kinds without error."""
        # Convenience.
        def mm(*args):
            return manio.make_meta(make_manifest(*args))

        # Test function must gracefully reject all invalid kinds.
        for kind in SUPPORTED_KINDS:
            file_manifests = {"m0.yaml": [(mm(kind, "ns", "name"), "0")]}
            assert manio.unparse(file_manifests)[1] is False

    def test_manifest_lifecycle(self):
        """Load, sync and save manifests the hard way.

        This test does not cover error scenarios. Instead, it shows how the
        individual functions in `manio` play together.

        This test does not load or save any files.

        """
        # Construct demo manifests in the same way as `load_files` would.
        dply = [mk_deploy(f"d_{_}", "nsfoo") for _ in range(10)]
        meta = [manio.make_meta(_) for _ in dply]
        fdata_test_in = {
            "m0.yaml": [dply[0], dply[1], dply[2]],
            "m1.yaml": [dply[3], dply[4]],
            "m2.yaml": [dply[5]],
        }
        fdata_test_in = self.yamlfy(fdata_test_in)
        expected_manifests = {meta[_]: dply[_] for _ in range(6)}

        # ---------- PARSE YAML FILES ----------
        # Parse Yaml string, extract MetaManifest and compile new dict from it.
        # :: Dict[Filename:YamlStr] -> Dict[Filename:List[(MetaManifest, YamlDict)]]
        fdata_meta, err = manio.parse(fdata_test_in)
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
            kinds=["Deployment"], namespaces=None
        )
        assert err is False

        # Convert the data to YAML. The output would normally be passed to
        # `save_files` but here we will verify it directly (see below).
        # :: Dict[Filename:List[(MetaManifest, YamlDict)]] -> Dict[Filename:YamlStr]
        fdata_raw_new, err = manio.unparse(updated_manifests)
        assert err is False

        # Expected output after we merged back the changes (reminder: `dply[1]`
        # is different, `dply[{3,5}]` were deleted and `dply[{6,7}]` are new).
        # The new manifests must all end up in "_nsfoo.yaml" file because they
        # specify resources in the `nsfoo` namespace.
        expected = {
            "m0.yaml": [dply[0], server_manifests[meta[1]], dply[2]],
            "m1.yaml": [dply[4]],
            "_nsfoo.yaml": [dply[6], dply[7]],
        }
        expected = self.yamlfy(expected)
        assert fdata_raw_new == expected


class TestManifestValidation:
    @classmethod
    def setup_class(cls):
        square.setup_logging(9)

    def test_schemas(self):
        """There must be schemas for all supported versions and kinds.

        Furthermore, the schema for each K8s version and resource kind must
        contain only {dict, True, False, None} values.

        """
        def is_sane(schema: dict):
            """Return True if all keys/values are valid."""
            for k, v in schema.items():
                # Field name (eg "metadata") must be a non-zero string.
                assert isinstance(k, str)
                assert len(k) > 0

                # The value must either be a dict or specify whether the key is
                # mandatory (True), forbidden (False) or optional (None).
                if isinstance(v, dict):
                    # Recursively check dicts.
                    is_sane(v)
                else:
                    assert v in {True, False, None}

            # Looks good.
            return True

        # Check that schemas for all kinds and versions exist and that their
        # definitions at least look valid.
        for version in SUPPORTED_VERSIONS:
            assert version in schemas.RESOURCE_SCHEMA
            for kind in SUPPORTED_KINDS:
                assert kind in schemas.RESOURCE_SCHEMA[version]
                assert is_sane(schemas.RESOURCE_SCHEMA[version][kind])

    def test_strip_generic(self):
        """Create a complete fake schema to test all options.

        This test has nothing to do with real world manifests. Its only purpose
        is to validate the algorithm that strips the manifests.

        """
        version = "1.10"
        config = k8s.Config("url", "token", "ca_cert", "client_cert", version, "")
        schema = {
            "invalid": False,
            "metadata": {
                "name": True,
                "labels": None,
                "namespace": False
            },
            "optional-1": None,
            "optional-2": None,
            "spec": True,
        }
        schema = {version: {"TEST": schema}}

        with mock.patch("square.schemas.RESOURCE_SCHEMA", schema):
            # Valid manifest with purely mandatory fields.
            valid = {
                "apiVersion": "v1",
                "kind": "TEST",
                "metadata": {"name": "name"},
                "spec": "spec",
            }
            data, err = manio.strip(config, valid)
            assert (data, err) == (valid, False)
            assert isinstance(data, dotdict.DotDict)
            del valid

            # Valid manifest with all mandatory and *some* optional keys (
            # "metadata.labels" and "optional-2" are present, "optional-1" is
            # not).
            valid = {
                "apiVersion": "v1",
                "kind": "TEST",
                "metadata": {"name": "mandatory", "labels": "optional"},
                "spec": "mandatory",
                "optional-2": "optional",
            }
            expected = {
                "apiVersion": "v1",
                "kind": "TEST",
                "metadata": {"name": "mandatory", "labels": "optional"},
                "spec": "mandatory",
                "optional-2": "optional",
            }
            assert manio.strip(config, valid) == (expected, False)

            # As before but with additional keys "status" and "metadata.annotation" that
            # must not be in the output.
            valid = {
                "apiVersion": "v1",
                "kind": "TEST",
                "metadata": {"name": "mandatory", "labels": "optional", "foo": "bar"},
                "spec": "mandatory",
                "status": "skip",
                "optional-2": "optional",
            }
            assert manio.strip(config, valid) == (expected, False)

            # Add a field that is not allowed.
            invalid = valid.copy()
            invalid["metadata"]["namespace"] = "error"
            assert manio.strip(config, valid) == (None, True)
            del valid, expected

            # Has all mandatory keys in schema but misses `apiVersion`.
            invalid = {
                "kind": "TEST",
                "metadata": {"name": "name"},
                "spec": "spec",
            }
            assert manio.strip(config, invalid) == (None, True)

            # Has all mandatory keys in schema but misses `kind`.
            invalid = {
                "apiVersion": "v1",
                "metadata": {"name": "name"},
                "spec": "spec",
            }
            assert manio.strip(config, invalid) == (None, True)

    def test_strip_generic_all_empty(self):
        """Fake schema where all keys in "metadata" are optional.

        This test verifies that the output will not contain any empty
        dictionaries.

        """
        version = "1.10"
        config = k8s.Config("url", "token", "ca_cert", "client_cert", version, "")
        schema = {
            "metadata": {
                "name": None,
            },
        }
        schema = {version: {"TEST": schema}}

        with mock.patch("square.schemas.RESOURCE_SCHEMA", schema):
            # Valid manifest with the optional metadata key "name".
            manifest = {"apiVersion": "", "kind": "TEST", "metadata": {"name": "name"}}
            assert manio.strip(config, manifest) == (manifest, False)

            # Valid manifest with an empty "metadata" dict. The output
            # must not contain a "metadata" key.
            manifest = {"apiVersion": "", "kind": "TEST", "metadata": {}}
            expected = {"apiVersion": "", "kind": "TEST"}
            assert manio.strip(config, manifest) == (expected, False)

            # Valid manifest with no "metadata" key at all. The output
            # must also not contain a "metadata" key.
            manifest = {"apiVersion": "", "kind": "TEST"}
            assert manio.strip(config, manifest) == (manifest, False)

            # Valid manifest where "metadata" contains an irrelevant key. The output
            # must, again,  not contain a "metadata" key.
            manifest = {"apiVersion": "", "kind": "TEST", "metadata": {"foo": "bar"}}
            expected = {"apiVersion": "", "kind": "TEST"}
            assert manio.strip(config, manifest) == (expected, False)

    def test_strip_invalid_schema(self):
        """Create a corrupt schema and verify we get a proper error message."""
        version = "1.10"
        config = k8s.Config("url", "token", "ca_cert", "client_cert", version, "")
        schema = {version: {"TEST": {"invalid": "foo"}}}

        with mock.patch("square.schemas.RESOURCE_SCHEMA", schema):
            # Valid manifest with purely mandatory fields.
            valid = {"apiVersion": "v1", "kind": "TEST"}
            assert manio.strip(config, valid) == (None, True)

    def test_strip_invalid_version_kind(self):
        """Must abort gracefully for unknown K8s version or resource kind."""
        # Minimal valid schema.
        schema = {"1.10": {"TEST": {"metadata": None}}}

        with mock.patch("square.schemas.RESOURCE_SCHEMA", schema):
            # Valid version but unknown resource.
            config = k8s.Config("url", "token", "ca_cert", "client_cert", "1.10", "")
            manifest = {"apiVersion": "v1", "kind": "Unknown"}
            assert manio.strip(config, manifest) == (None, True)

            # Invalid version but known resource.
            config = k8s.Config("url", "token", "ca_cert", "client_cert", "unknown", "")
            manifest = {"apiVersion": "v1", "kind": "TEST"}
            assert manio.strip(config, manifest) == (None, True)

    def test_strip_namespace(self):
        """Validate NAMESPACE manifests."""
        config = k8s.Config("url", "token", "ca_cert", "client_cert", "1.10", "")

        # A valid namespace manifest with a few optional and irrelevant keys.
        valid = {
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {"name": "mandatory", "labels": "optional", "skip": "this"},
            "spec": "mandatory",
            "skip": "this",
        }
        expected = {
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {"name": "mandatory", "labels": "optional"},
            "spec": "mandatory",
        }
        assert manio.strip(config, valid) == (expected, False)
        del valid, expected

        # Namespace cannot have a "metadata.namespace" attribute.
        invalid = {
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {"name": "mandatory", "namespace": "error"},
            "spec": "mandatory",
            "skip": "this",
        }
        assert manio.strip(config, invalid) == (None, True)

    def test_strip_service(self):
        """Validate SERVICE manifests."""
        config = k8s.Config("url", "token", "ca_cert", "client_cert", "1.10", "")

        # A valid service manifest with a few optional and irrelevant keys.
        valid = {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {
                "name": "mandatory",
                "namespace": "ns",
                "labels": "optional",
                "skip": "this",
            },
            "spec": {
                "ports": "optional",
                "selector": "optional",
                "sessionAffinity": "mandatory",
                "type": "optional",
                "skip": "this"
            },
            "skip": "this",
        }
        expected = {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {
                "name": "mandatory",
                "namespace": "ns",
                "labels": "optional",
            },
            "spec": {
                "ports": "optional",
                "selector": "optional",
                "type": "optional",
            },
        }
        assert manio.strip(config, valid) == (expected, False)
        del valid, expected

        # Services must have a "metadata.namespace" attribute.
        invalid = {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {
                "name": "mandatory",
                "labels": "optional",
                "skip": "this"
            },
            "spec": {
                "ports": "optional",
                "selector": "optional",
                "sessionAffinity": "mandatory",
                "type": "optional",
            },
            "skip": "this",
        }
        assert manio.strip(config, invalid) == (None, True)

    def test_strip_deployment(self):
        """Validate DEPLOYMENT manifests."""
        config = k8s.Config("url", "token", "ca_cert", "client_cert", "1.10", "")

        # A valid service manifest with a few optional and irrelevant keys.
        valid = {
            "apiVersion": "v1",
            "kind": "Deployment",
            "metadata": {
                "name": "mandatory",
                "namespace": "ns",
                "labels": "optional",
                "skip": "this"
            },
            "spec": "mandatory",
            "skip": "this",
        }
        expected = {
            "apiVersion": "v1",
            "kind": "Deployment",
            "metadata": {
                "name": "mandatory",
                "namespace": "ns",
                "labels": "optional",
            },
            "spec": "mandatory",
        }
        assert manio.strip(config, valid) == (expected, False)
        del valid, expected

        # Deployments must have a "metadata.namespace" attribute.
        invalid = {
            "apiVersion": "v1",
            "kind": "Deployment",
            "metadata": {
                "name": "mandatory",
                "labels": "optional",
                "skip": "this"
            },
            "spec": "mandatory",
            "skip": "this",
        }
        assert manio.strip(config, invalid) == (None, True)


class TestDiff:
    @classmethod
    def setup_class(cls):
        square.setup_logging(9)

    def test_diff_ok(self):
        """Diff two valid manifests and (roughly) verify the output."""
        # Dummy config for (only "version" is relevant).
        config = k8s.Config("url", "token", "ca_cert", "client_cert", "1.10", "")

        # Two valid manifests.
        srv = make_manifest("Deployment", "namespace", "name1")
        loc = make_manifest("Deployment", "namespace", "name2")

        # Test function must able to cope with `DotDicts`.
        srv = dotdict.make(srv)
        loc = dotdict.make(loc)

        # Diff the manifests. Must not return an error.
        diff_str, err = manio.diff(config, loc, srv)
        assert err is False

        # Since it is difficult to compare the correct diff string due to
        # formatting characters, we will only verify that the string contains
        # a line that removes the old "names1" and one line to add "name2".
        assert "-  name: name1" in diff_str
        assert "+  name: name2" in diff_str

    def test_diff_err(self):
        """Diff two valid manifests and verify the output."""
        # Dummy config for (only "version" is relevant).
        config = k8s.Config("url", "token", "ca_cert", "client_cert", "1.10", "")

        # Create two valid manifests, then stunt one in such a way that
        # `essential` will reject it.
        valid = make_manifest("Deployment", "namespace", "name1")
        invalid = make_manifest("Deployment", "namespace", "name2")
        del invalid["kind"]

        # Test function must return with an error, irrespective of which
        # manifest was invalid.
        assert manio.diff(config, valid, invalid) == (None, True)
        assert manio.diff(config, invalid, valid) == (None, True)
        assert manio.diff(config, invalid, invalid) == (None, True)


class TestYamlManifestIOIntegration:
    """These integration tests all write files to temporary folders."""

    @classmethod
    def setup_class(cls):
        square.setup_logging(9)

    def test_load_save_files(self, tmp_path):
        """Basic file loading/saving tests."""
        # Demo file names and content.
        fnames = ("m0.yaml", "m1.yaml", "foo/m2.yaml", "foo/bar/m3.yaml")
        file_data = {fname: f"Data in {fname}" for fname in fnames}

        # Asking for non-existing files must return an error.
        assert manio.load_files(tmp_path, fnames) == (None, True)

        # Saving files to the temporary folder and loading them afterwards
        # again must work.
        assert manio.save_files(tmp_path, file_data) == (None, False)
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
        assert manio.save_files(tmp_path, file_data) == (None, False)
        assert (tmp_path / "nonempty.yaml").exists()
        assert not (tmp_path / "empty.yaml").exists()

    def test_save_err_permissions(self, tmp_path):
        """Make temp folder readonly and try to save the manifests."""
        file_data = {"m0.yaml": "Some data"}
        tmp_path.chmod(0o555)
        assert manio.save_files(tmp_path, file_data) == (None, True)
        assert not (tmp_path / "m0.yaml").exists()

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
        assert manio.save_files(tmp_path, {}) == (None, False)
        assert not tmp_file.exists()

        # Create the dummy file again, then make its folder read-only. This
        # time, the function must abort with an error because it cannot delete
        # files inside a read-only folder.
        tmp_file.touch()
        tmp_path.chmod(0o555)
        assert manio.save_files(tmp_path, {}) == (None, True)
        assert tmp_file.exists()

    def test_load_save_ok(self, tmp_path):
        """Basic test that uses the {load,save} convenience functions."""
        # Dummy filter.
        sel = Selectors(["service"], None, None)

        # Create two YAML files, each with multiple manifests.
        dply = [mk_deploy(f"d_{_}") for _ in range(10)]
        meta = [manio.make_meta(mk_deploy(f"d_{_}")) for _ in range(10)]
        man_files = {
            "m0.yaml": [(meta[0], dply[0]), (meta[1], dply[1])],
            "foo/m1.yaml": [(meta[2], dply[2])],
        }
        expected = (manio.unpack(man_files)[0], man_files)
        del dply, meta

        # Save the test data, then load it back and verify.
        assert manio.save(tmp_path, man_files) == (None, False)
        assert manio.load(tmp_path, sel) == (*expected, False)

        # Glob the folder and ensure it contains exactly the files specified in
        # the `fdata_test_in` dict.
        fnames_abs = {str(tmp_path / fname) for fname in man_files.keys()}
        assert set(str(_) for _ in tmp_path.rglob("*.yaml")) == fnames_abs

        # Create non-YAML files. The `load_files` function must skip those.
        (tmp_path / "delme.txt").touch()
        (tmp_path / "foo" / "delme.txt").touch()
        assert manio.load(tmp_path, sel) == (*expected, False)

    def test_save_delete_stale_yaml(self, tmp_path):
        """`save_file` must remove all excess YAML files."""
        # Dummy filter.
        sel = Selectors(["service"], None, None)

        # Create two YAML files, each with multiple manifests.
        dply = [mk_deploy(f"d_{_}") for _ in range(10)]
        meta = [manio.make_meta(mk_deploy(f"d_{_}")) for _ in range(10)]
        man_files = {
            "m0.yaml": [(meta[0], dply[0])],
            "m1.yaml": [(meta[1], dply[1])],
            "foo/m2.yaml": [(meta[2], dply[2])],
            "foo/m3.yaml": [(meta[3], dply[3])],
            "bar/m4.yaml": [(meta[4], dply[4])],
            "bar/m5.yaml": [(meta[5], dply[5])],
        }
        expected = (manio.unpack(man_files)[0], man_files)
        del dply, meta

        # Save and load the test data.
        assert manio.save(tmp_path, man_files) == (None, False)
        assert manio.load(tmp_path, sel) == (*expected, False)

        # Save a reduced set of files. Compared to `fdata_full`, it is two
        # files short and a third one ("bar/m4.yaml") is empty.
        fdata_reduced = man_files.copy()
        del fdata_reduced["m0.yaml"]
        del fdata_reduced["foo/m3.yaml"]
        fdata_reduced["bar/m4.yaml"] = []
        expected = (manio.unpack(fdata_reduced)[0], fdata_reduced)

        # Verify that the files still exist from the last call to `save`.
        assert (tmp_path / "m0.yaml").exists()
        assert (tmp_path / "foo/m3.yaml").exists()
        assert (tmp_path / "bar/m4.yaml").exists()

        # Save the reduced set of files.
        assert manio.save(tmp_path, fdata_reduced) == (None, False)

        # Load the data. It must neither contain the files we removed from the
        # dict above, nor "bar/m4.yaml" which contained an empty manifest list.
        del fdata_reduced["bar/m4.yaml"]
        assert manio.load(tmp_path, sel) == (*expected, False)

        # Verify that the files physically do not exist anymore.
        assert not (tmp_path / "m0.yaml").exists()
        assert not (tmp_path / "foo/m3.yaml").exists()
        assert not (tmp_path / "bar/m4.yaml").exists()

    @mock.patch.object(manio, "load_files")
    def test_load_err(self, m_load, tmp_path):
        """Simulate an error in `load_files` function."""
        # Dummy filter.
        sel = Selectors(["service"], None, None)
        m_load.return_value = (None, True)
        assert manio.load(tmp_path, sel) == (None, None, True)

    @mock.patch.object(manio, "unparse")
    def test_save_err(self, m_unparse, tmp_path):
        """Simulate an error in `unparse` function."""
        m_unparse.return_value = (None, True)
        assert manio.save(tmp_path, "foo") == (None, True)


class TestSync:
    @classmethod
    def setup_class(cls):
        square.setup_logging(9)

    def test_sync_modify_selective_kind_and_namespace_ok(self):
        """Add, modify and delete a few manifests.

        Create fake inputs for the test function, namely local- and remote
        manifests. Their structure is slightly different in that the local
        manifests still carry the file name in their data structure whereas the
        server ones do not.

        """
        # Convenience shorthand.
        fun = manio.sync

        # Various MetaManifests to use in the tests.
        ns0 = manio.make_meta(make_manifest("Namespace", None, "ns0"))
        ns1 = manio.make_meta(make_manifest("Namespace", None, "ns1"))
        dpl_ns0 = manio.make_meta(make_manifest("Deployment", "ns0", "d-ns0"))
        dpl_ns1 = manio.make_meta(make_manifest("Deployment", "ns1", "d-ns1"))
        svc_ns0 = manio.make_meta(make_manifest("Service", "ns0", "s-ns0"))
        svc_ns1 = manio.make_meta(make_manifest("Service", "ns1", "s-ns0"))

        # The local and server manifests will remain fixed for all tests. We
        # will only vary the namespaces and resource kinds (arguments to `sync`).
        loc_man = {
            "m0.yaml": [
                (ns0, "ns0"),
                (dpl_ns0, "d-ns0"),
                (svc_ns0, "s-ns0"),
                (ns1, "ns1"),
                (dpl_ns1, "d-ns1"),
                (svc_ns1, "s-ns1"),
            ],
        }
        srv_man = {
            ns0: "ns0-mod",
            dpl_ns0: "d-ns0-mod",
            svc_ns0: "s-ns0-mod",
            ns1: "ns1-mod",
            dpl_ns1: "d-ns1-mod",
            svc_ns1: "s-ns1-mod",
        }

        # ----------------------------------------------------------------------
        # Special cases: empty list of namespaces and/or resources.
        # Must do nothing.
        # ----------------------------------------------------------------------
        expected = loc_man
        kinds, namespaces = [], None
        assert manio.sync(loc_man, srv_man, kinds, namespaces) == (expected, False)

        kinds, namespaces = [], []
        assert manio.sync(loc_man, srv_man, kinds, namespaces) == (expected, False)

        kinds, namespaces = ["Deployment", "Service"], []
        assert manio.sync(loc_man, srv_man, kinds, namespaces) == (expected, False)

        # NOTE: this must *not* sync the Namespace manifest from "ns1" because
        # it was not an explicitly specified resource.
        kinds, namespaces = [], ["ns1"]
        assert manio.sync(loc_man, srv_man, kinds, namespaces) == (expected, False)

        # Invalid/unsupported kinds.
        kinds, namespaces = ["Foo"], None
        assert manio.sync(loc_man, srv_man, kinds, namespaces) == (None, True)
        del kinds, namespaces

        # ----------------------------------------------------------------------
        # Sync all namespaces implicitly (Namespaces, Deployments, Services).
        # ----------------------------------------------------------------------
        expected = {
            "m0.yaml": [
                (ns0, "ns0-mod"),
                (dpl_ns0, "d-ns0-mod"),
                (svc_ns0, "s-ns0-mod"),
                (ns1, "ns1-mod"),
                (dpl_ns1, "d-ns1-mod"),
                (svc_ns1, "s-ns1-mod"),
            ],
        }

        # Sync the manifests. The order of `kinds` and `namespaces` must not matter.
        for kinds in itertools.permutations(["Namespace", "Deployment", "Service"]):
            # Implicitly use all namespaces.
            assert fun(loc_man, srv_man, kinds, None) == (expected, False)

            # Specify all namespaces explicitly.
            for ns in itertools.permutations(["ns0", "ns1"]):
                assert fun(loc_man, srv_man, kinds, ns) == (expected, False)

        # ----------------------------------------------------------------------
        # Sync the server manifests in namespace "ns0".
        # ----------------------------------------------------------------------
        expected = {
            "m0.yaml": [
                (ns0, "ns0"),
                (dpl_ns0, "d-ns0-mod"),
                (svc_ns0, "s-ns0-mod"),
                (ns1, "ns1"),
                (dpl_ns1, "d-ns1"),
                (svc_ns1, "s-ns1"),
            ],
        }
        for kinds in itertools.permutations(["Deployment", "Service"]):
            assert fun(loc_man, srv_man, kinds, ["ns0"]) == (expected, False)

        # ----------------------------------------------------------------------
        # Sync only Deployments (all namespaces).
        # ----------------------------------------------------------------------
        expected = {
            "m0.yaml": [
                (ns0, "ns0"),
                (dpl_ns0, "d-ns0-mod"),
                (svc_ns0, "s-ns0"),
                (ns1, "ns1"),
                (dpl_ns1, "d-ns1-mod"),
                (svc_ns1, "s-ns1"),
            ],
        }
        kinds, namespaces = ["Deployment"], None
        assert fun(loc_man, srv_man, kinds, namespaces) == (expected, False)

        # ----------------------------------------------------------------------
        # Sync only Deployments in namespace "ns0".
        # ----------------------------------------------------------------------
        expected = {
            "m0.yaml": [
                (ns0, "ns0"),
                (dpl_ns0, "d-ns0-mod"),
                (svc_ns0, "s-ns0"),
                (ns1, "ns1"),
                (dpl_ns1, "d-ns1"),
                (svc_ns1, "s-ns1"),
            ],
        }
        kinds, namespaces = ["Deployment"], ["ns0"]
        assert manio.sync(loc_man, srv_man, kinds, namespaces) == (expected, False)

        # ----------------------------------------------------------------------
        # Sync only Services in namespace "ns1".
        # ----------------------------------------------------------------------
        expected = {
            "m0.yaml": [
                (ns0, "ns0"),
                (dpl_ns0, "d-ns0"),
                (svc_ns0, "s-ns0"),
                (ns1, "ns1"),
                (dpl_ns1, "d-ns1"),
                (svc_ns1, "s-ns1-mod"),
            ],
        }
        kinds, namespaces = ["Service"], ["ns1"]
        assert manio.sync(loc_man, srv_man, kinds, namespaces) == (expected, False)

    def test_sync_modify_delete_ok(self):
        """Add, modify and delete a few manifests.

        Create fake inputs for the test function, namely local- and remote
        manifests. Their structure is slightly different in that the local
        manifests still carry the file name in their data structure whereas the
        server ones do not.

        """
        # Args for `sync`. In this test, we only have Deployments and want to
        # sync them across all namespaces.
        kinds, namespaces = ["Deployment"], None

        # First, create the local manifests as `load_files` would return it.
        meta_1 = [manio.make_meta(mk_deploy(f"d_{_}", "ns1")) for _ in range(10)]
        meta_2 = [manio.make_meta(mk_deploy(f"d_{_}", "ns2")) for _ in range(10)]
        loc_man = {
            "m0.yaml": [(meta_1[0], "0"), (meta_1[1], "1"), (meta_2[2], "2")],
            "m1.yaml": [(meta_2[3], "3"), (meta_1[4], "4")],
            "m2.yaml": [(meta_1[5], "5")],
        }

        # Create server manifests as `download_manifests` would return it. Only
        # the MetaManifests (ie dict keys) are relevant whereas the dict values
        # are not but serve to improve code readability here.
        srv_man = {
            meta_1[0]: "0",         # same
            meta_1[1]: "modified",  # modified
            meta_2[2]: "2",         # same
                                    # delete [3]
            meta_1[4]: "4",         # same
                                    # delete [5]
            meta_1[6]: "new",       # new
            meta_2[7]: "new",       # new
            meta_1[8]: "new",       # new
        }

        # The expected outcome is that the local manifests were updated,
        # overwritten (modified), deleted or put into a default manifest.
        expected = {
            "m0.yaml": [(meta_1[0], "0"), (meta_1[1], "modified"), (meta_2[2], "2")],
            "m1.yaml": [(meta_1[4], "4")],
            "m2.yaml": [],
            "_ns1.yaml": [(meta_1[6], "new"), (meta_1[8], "new")],
            "_ns2.yaml": [(meta_2[7], "new")],
        }
        assert manio.sync(loc_man, srv_man, kinds, namespaces) == (expected, False)

    def test_sync_catch_all_files(self):
        """Verify that syncing the catch-all files works as expected.

        This requires a special test to ensure these auto generated catch-all
        files behave like their "normal" user created counterparts.

        """
        # Args for `sync`. In this test, we only have Deployments and want to
        # sync them across all namespaces.
        kinds, namespaces = ["Deployment"], None

        # First, create the local manifests as `load_files` would return it.
        meta_1 = [manio.make_meta(mk_deploy(f"d_{_}", "ns1")) for _ in range(10)]
        meta_2 = [manio.make_meta(mk_deploy(f"d_{_}", "ns2")) for _ in range(10)]
        loc_man = {
            "_ns1.yaml": [
                (meta_1[1], "1"), (meta_1[2], "2"), (meta_1[3], "3"), (meta_1[5], "5")
            ],
            "_ns2.yaml": [
                (meta_2[2], "2"), (meta_2[6], "6"),
            ]
        }

        # Create server manifests as `download_manifests` would return it. Only
        # the MetaManifests (ie dict keys) are relevant whereas the dict values
        # are not but serve to improve code readability here.
        srv_man = {
            # --- _ns1.yaml ---
            meta_1[0]: "0",         # new
            meta_1[1]: "modified",  # modified
            meta_1[2]: "2",         # same
                                    # delete [3]
                                    # [4] never existed
                                    # delete [5]

            # --- _ns2.yaml ---
            meta_2[0]: "0",         # new
            meta_2[9]: "9",         # new
            meta_2[7]: "7",         # new
            meta_2[6]: "modified",  # modified
                                    # delete [2]
            meta_2[5]: "5",         # new
            meta_2[3]: "3",         # new
        }

        # The expected outcome is that the local manifests were updated,
        # either overwritten (modified), deleted or put into a default
        # manifest.
        # NOTE: this test _assumes_ that the `srv_man` dict iterates over its
        # keys in insertion order, which is guaranteed for Python 3.7.
        expected = {
            "_ns1.yaml": [
                (meta_1[1], "modified"), (meta_1[2], "2"), (meta_1[0], "0"),
            ],
            "_ns2.yaml": [
                (meta_2[6], "modified"), (meta_2[0], "0"), (meta_2[9], "9"),
                (meta_2[7], "7"), (meta_2[5], "5"), (meta_2[3], "3"),
            ]
        }
        assert manio.sync(loc_man, srv_man, kinds, namespaces) == (expected, False)

    def test_sync_new_namespaces(self):
        """Create catch-all files for new namespaces.

        This tests verifies that namespace resources end up in the correct
        catch-all files. This is a corner case because the correct catch-all
        file for namespaces is based on the `name` attributed of that
        namespace's `MetaManifest`, not the `namespace` attribute which is None
        for namespaces.

        """
        # Args for `sync`. In this test, we will use Namespaces, Deployments
        # and Services. We will always want to sync across all namespaces.
        kinds, namespaces = ["Namespace", "Deployment", "Service"], None

        # Convenience to improve readability.
        def mm(*args):
            return manio.make_meta(make_manifest(*args))

        meta_ns_a = mm("Namespace", None, "a")
        meta_ns_b = mm("Namespace", None, "b")
        meta_svc_a = [mm("Service", "a", f"s{_}") for _ in range(10)]
        meta_dply_a = [mm("Deployment", "a", f"d{_}") for _ in range(10)]
        meta_svc_b = [mm("Service", "b", f"s{_}") for _ in range(10)]
        meta_dply_b = [mm("Deployment", "b", f"d{_}") for _ in range(10)]

        # For this test we can pretend that we do not have any local manifest
        # files yet.
        loc_man = {}

        # Create server manifests as `download_manifests` would return it. Only
        # the MetaManifests (ie dict keys) are relevant whereas the dict values
        # are not but serve to improve code readability here.
        srv_man = {
            # --- _a.yaml ---
            meta_ns_a: "ns_a",
            meta_svc_a[0]: "svc_a_0",
            meta_svc_a[1]: "svc_a_1",
            meta_dply_a[3]: "dply_a_3",
            meta_dply_a[4]: "dply_a_4",

            # --- _b.yaml ---
            meta_ns_b: "ns_b",
            meta_svc_b[0]: "svc_b_0",
            meta_svc_b[1]: "svc_b_1",
            meta_dply_b[3]: "dply_b_3",
            meta_dply_b[4]: "dply_b_4",
        }

        # The expected outcome is that the local manifests were updated,
        # either overwritten (modified), deleted or put into a default
        # manifest.
        # NOTE: this test _assumes_ that the `srv_man` dict iterates over its
        # keys in insertion order, which is guaranteed for Python 3.7.
        expected = {
            "_a.yaml": [
                (meta_ns_a, "ns_a"),
                (meta_svc_a[0], "svc_a_0"),
                (meta_svc_a[1], "svc_a_1"),
                (meta_dply_a[3], "dply_a_3"),
                (meta_dply_a[4], "dply_a_4"),
            ],
            "_b.yaml": [
                (meta_ns_b, "ns_b"),
                (meta_svc_b[0], "svc_b_0"),
                (meta_svc_b[1], "svc_b_1"),
                (meta_dply_b[3], "dply_b_3"),
                (meta_dply_b[4], "dply_b_4"),
            ],
        }
        assert manio.sync(loc_man, srv_man, kinds, namespaces) == (expected, False)


class TestDownloadManifests:
    @classmethod
    def setup_class(cls):
        square.setup_logging(9)

    @mock.patch.object(k8s, 'get')
    def test_download_ok(self, m_get):
        """Download two kinds of manifests: Deployments and Namespaces.

        The test only mocks the call to the K8s API. All other functions
        actually execute.

        """
        config = k8s.Config("url", "token", "ca_cert", "client_cert", "1.10", "")
        meta = [
            make_manifest("Namespace", None, "ns0"),
            make_manifest("Namespace", None, "ns1"),
            make_manifest("Deployment", "ns0", "d0"),
            make_manifest("Deployment", "ns1", "d1"),
        ]

        # Resource URLs (not namespaced).
        url_ns, err1 = urlpath(config, "Namespace", None)
        url_deploy, err2 = urlpath(config, "Deployment", None)

        # Namespaced resource URLs.
        url_ns_0, err3 = urlpath(config, "Namespace", "ns0")
        url_ns_1, err4 = urlpath(config, "Namespace", "ns1")
        url_dply_0, err5 = urlpath(config, "Deployment", "ns0")
        url_dply_1, err6 = urlpath(config, "Deployment", "ns1")
        assert not any([err1, err2, err3, err4, err5, err6])
        del err1, err2, err3, err4, err5, err6

        # NamespaceList and DeploymentList (all namespaces).
        l_ns = {'apiVersion': 'v1', 'kind': 'NamespaceList', "items": [meta[0], meta[1]]}
        l_dply = {'apiVersion': 'v1', 'kind': 'DeploymentList',
                  "items": [meta[2], meta[3]]}

        # Namespaced NamespaceList and DeploymentList.
        l_ns_0 = {'apiVersion': 'v1', 'kind': 'NamespaceList', "items": [meta[0]]}
        l_ns_1 = {'apiVersion': 'v1', 'kind': 'NamespaceList', "items": [meta[1]]}
        l_dply_0 = {'apiVersion': 'v1', 'kind': 'DeploymentList', "items": [meta[2]]}
        l_dply_1 = {'apiVersion': 'v1', 'kind': 'DeploymentList', "items": [meta[3]]}

        # ----------------------------------------------------------------------
        # Request resources from all namespaces implicitly via namespaces=None
        # Must make two calls: one for each kind ("Deployment", "Namespace").
        # ----------------------------------------------------------------------
        m_get.reset_mock()
        m_get.side_effect = [
            (l_ns, False),
            (l_dply, False),
        ]
        expected = {
            manio.make_meta(meta[0]): manio.strip(config, meta[0])[0],
            manio.make_meta(meta[1]): manio.strip(config, meta[1])[0],
            manio.make_meta(meta[2]): manio.strip(config, meta[2])[0],
            manio.make_meta(meta[3]): manio.strip(config, meta[3])[0],
        }
        ret = manio.download(
            config, "client",
            Selectors(["Namespace", "Deployment"], None, None)
        )
        assert ret == (expected, False)
        assert m_get.call_args_list == [
            mock.call("client", url_ns),
            mock.call("client", url_deploy),
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
        expected = {
            manio.make_meta(meta[0]): manio.strip(config, meta[0])[0],
            manio.make_meta(meta[1]): manio.strip(config, meta[1])[0],
            manio.make_meta(meta[2]): manio.strip(config, meta[2])[0],
            manio.make_meta(meta[3]): manio.strip(config, meta[3])[0],
        }
        assert (expected, False) == manio.download(
            config, "client",
            Selectors(["Namespace", "Deployment"], ["ns0", "ns1"], None)
        )
        assert m_get.call_args_list == [
            mock.call("client", url_ns_0),
            mock.call("client", url_dply_0),
            mock.call("client", url_ns_1),
            mock.call("client", url_dply_1),
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
            manio.make_meta(meta[0]): manio.strip(config, meta[0])[0],
            manio.make_meta(meta[2]): manio.strip(config, meta[2])[0],
        }
        assert (expected, False) == manio.download(
            config, "client",
            Selectors(["Namespace", "Deployment"], ["ns0"], None)
        )
        assert m_get.call_args_list == [
            mock.call("client", url_ns_0),
            mock.call("client", url_dply_0),
        ]

    @mock.patch.object(k8s, 'get')
    def test_download_err(self, m_get):
        """Simulate a download error."""
        config = k8s.Config("url", "token", "ca_cert", "client_cert", "1.10", "")

        # A valid NamespaceList with one element.
        man_list_ns = {
            'apiVersion': 'v1',
            'kind': 'NamespaceList',
            'items': [make_manifest("Namespace", None, "ns0")],
        }

        # The first call to get will succeed whereas the second will not.
        m_get.side_effect = [(man_list_ns, False), (None, True)]

        # The request URLs. We will need them to validate the `get` arguments.
        url_ns, err1 = urlpath(config, "Namespace", None)
        url_deploy, err2 = urlpath(config, "Deployment", None)
        assert not err1 and not err2

        # Run test function and verify it returns an error and no data, despite
        # a successful `NamespaceList` download.
        ret = manio.download(
            config, "client",
            Selectors(["Namespace", "Deployment"], None, None)
        )
        assert ret == (None, True)
        assert m_get.call_args_list == [
            mock.call("client", url_ns),
            mock.call("client", url_deploy),
        ]
