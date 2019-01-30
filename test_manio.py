import os
import glob
import tempfile

import yaml
import manio
import square
import test_square

from square import RetVal

pjoin = os.path.join


def mk_deploy(name: str):
    return test_square.make_manifest("Deployment", name, "namespace")


class TestYamlManifestIO:
    def yamlfy(self, data):
        return {
            k: yaml.safe_dump_all(v, default_flow_style=False)
            for k, v in data.items()
        }

    def test_parse_ok(self):
        """Test function must be able to parse the Yaml string and compile a dict."""
        # Construct manifests in the way as `load_files` returns them.
        dply = [mk_deploy(f"d_{_}") for _ in range(10)]
        meta = [square.make_meta(_) for _ in dply]
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
        assert manio.parse(fdata_test_in) == RetVal(expected, False)

    def test_parse_err(self):
        """Intercept YAML decoding errors."""
        # Construct manifests in the way as `load_files` returns them.
        fdata_test_in = {"m0.yaml": "invalid :: - yaml"}
        assert manio.parse(fdata_test_in) == RetVal(None, True)

    def test_manifest_lifecycle(self):
        """Load, sync and save manifests the hard way.

        This test does not cover error scenarios. Instead, it shows how the
        individual functions in `manio` play together.

        This test does not load or save any files.

        """
        # Construct demo manifests in the same way as `load_files` would.
        dply = [mk_deploy(f"d_{_}") for _ in range(10)]
        meta = [square.make_meta(_) for _ in dply]
        fdata_test_in = {
            "m0.yaml": [dply[0], dply[1], dply[2]],
            "m1.yaml": [dply[3], dply[4]],
            "m2.yaml": [dply[5]],
        }
        fdata_test_in = {
            k: yaml.safe_dump_all(v, default_flow_style=False)
            for k, v in fdata_test_in.items()
        }
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
        updated_manifests, err = manio.sync(fdata_meta, server_manifests)
        assert err is False

        # Convert the data to YAML. The output would normally be passed to
        # `save_files` but here we will verify it directly (see below).
        # :: Dict[Filename:List[(MetaManifest, YamlDict)]] -> Dict[Filename:YamlStr]
        fdata_raw_new, err = manio.unparse(updated_manifests)
        assert err is False

        # Expected output after we merged back the changes (reminder: `dply[1]`
        # is different, `dply[{3,5}]` were deleted and `dply[{6,7}]` are new).
        # The new manifests must all end up in "default.yaml".
        expected = {
            "m0.yaml": [dply[0], server_manifests[meta[1]], dply[2]],
            "m1.yaml": [dply[4]],
            "default.yaml": [dply[6], dply[7]],
        }
        expected = {
            k: yaml.safe_dump_all(v, default_flow_style=False)
            for k, v in expected.items()
        }
        assert fdata_raw_new == expected


class TestYamlManifestIOIntegration:
    """These integration tests all write files to temporary folders."""

    def test_integration(self):
        """Save manifests then load them back."""
        # Create two YAML files, each with multiple manifests.
        dply = [mk_deploy(f"d_{_}") for _ in range(10)]
        meta = [square.make_meta(_) for _ in dply]
        fdata_test_in = {
            "m0.yaml": [(meta[0], dply[0]), (meta[1], dply[1])],
            "foo/m1.yaml": [(meta[2], dply[2])],
        }
        del dply, meta

        with tempfile.TemporaryDirectory() as tempdir:
            # Save test file in temporary folder.
            fnames_abs = {pjoin(tempdir, _) for _ in fdata_test_in.keys()}

            # Save the test data, then load it back and verify.
            assert manio.save(tempdir, fdata_test_in) == RetVal(None, False)
            assert manio.load(tempdir) == RetVal(fdata_test_in, False)

            # Use `glob` to verify the files ended up in the correct location.
            pattern = os.path.join(tempdir, "**", "*.yaml")
            assert set(glob.glob(pattern, recursive=True)) == fnames_abs

    def test_load_and_save_file_single_dir(self):
        """Use low level functions to save and load files."""
        with tempfile.TemporaryDirectory() as tempdir:
            # Demo input for `save_files` below (content is irrelevant).
            fdata = {
                "m0.yaml": "something 0",
                "foo/m1.yaml": "something 1",
                "bar/m2.yaml": "something 2",
                "foo/bar/blah/m2.yaml": "something 3",
            }

            # Convenience: extract absolute and relative file names.
            fnames_rel = list(fdata.keys())
            fnames_abs = [pjoin(tempdir, _) for _ in fnames_rel]

            # The target folder must be empty before we save the files.
            pattern = os.path.join(tempdir, "**", "*.yaml")
            assert glob.glob(pattern, recursive=True) == []

            # The target folder must now contain the expected files.
            assert manio.save_files(tempdir, fdata) == RetVal(None, False)
            assert set(glob.glob(pattern, recursive=True)) == set(fnames_abs)

            # Load the files from the temporary folder again.
            assert manio.load_files(tempdir, fnames_rel) == RetVal(fdata, False)
