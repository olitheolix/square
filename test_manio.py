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
    def test_integration(self):
        # Setup test.
        dply = [mk_deploy(f"d_{_}") for _ in range(10)]
        meta = [square.make_meta(_) for _ in dply]
        fdata_test_in = {
            "m0.yaml": [(meta[0], dply[0]), (meta[1], dply[1])],
            "foo/m1.yaml": [(meta[2], dply[2])],
        }

        with tempfile.TemporaryDirectory() as tempdir:
            pattern = os.path.join(tempdir, "**", "*.yaml")
            assert glob.glob(pattern, recursive=True) == []
            fnames_abs = {pjoin(tempdir, _) for _ in fdata_test_in.keys()}

            assert manio.save(tempdir, fdata_test_in) == RetVal(None, False)
            assert manio.load(tempdir) == RetVal(fdata_test_in, False)

            assert set(glob.glob(pattern, recursive=True)) == fnames_abs

    def test_manifest_lifecycle(self):
        # Setup test.
        dply = [mk_deploy(f"d_{_}") for _ in range(10)]
        meta = [square.make_meta(_) for _ in dply]
        fdata_test_in = {
            "m0.yaml": yaml.safe_dump_all([dply[0], dply[1], dply[2]]),
            "m1.yaml": yaml.safe_dump_all([dply[3], dply[4]]),
            "m2.yaml": yaml.safe_dump_all([dply[5]]),
        }
        expected_manifests = {meta[_]: dply[_] for _ in range(6)}

        # ---------- LOAD YAML FILES ----------
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
        # Create a fake set of server manifests from the `expected_manifests`.
        # In particular, pretend that K8s supplied two additional manifests,
        # lacks two others and features one with different content.
        server_manifests = expected_manifests
        del expected_manifests

        server_manifests[meta[6]] = dply[6]
        server_manifests[meta[7]] = dply[7]
        del server_manifests[meta[3]], server_manifests[meta[5]]
        server_manifests[meta[1]]["metadata"] = {"new": "label"}

        # ---------- SYNC SERVER MANIFESTS BACK TO LOCAL YAML FILES ----------
        # Merge the server manifests into the correct local YAML file dict.
        # * Upsert local with server values
        # * Delete all local manifests not on the server
        # :: Dict[MetaManifests:YamlDict] -> Dict[Filename:List[(MetaManifest, YamlDict)]]
        updated_manifests, err = manio.sync(fdata_meta, server_manifests)
        assert err is False

        # Strip the meta information
        # :: Dict[Filename:List[(MetaManifest, YamlDict)]] -> Dict[Filename:YamlStr]
        fdata_raw_new, err = manio.unparse(updated_manifests)
        assert err is False

        # Expected output after we merged back the changes (ie `dply[1]` is
        # different, `dply[{3,5}]` were deleted and `dply[{6,7}]` are new).
        # The new manifests must all end up in "default.yaml".
        fdata_test_out = {
            "m0.yaml": yaml.safe_dump_all([dply[0], server_manifests[meta[1]], dply[2]]),
            "m1.yaml": yaml.safe_dump_all([dply[4]]),
            "default.yaml": yaml.safe_dump_all([dply[6], dply[7]]),
        }
        assert fdata_raw_new == fdata_test_out

    def test_load_and_save_single_dir(self):
        with tempfile.TemporaryDirectory() as tempdir:
            fdata_test_in = {
                "m0.yaml": "something 0",
                "m1.yaml": "something 1",
                "m2.yaml": "something 2",
            }
            fnames_rel = list(fdata_test_in.keys())
            fnames_abs = [pjoin(tempdir, _) for _ in fnames_rel]

            pattern = os.path.join(tempdir, "**", "*.yaml")
            assert glob.glob(pattern, recursive=True) == []
            assert manio.save_files(tempdir, fdata_test_in) == RetVal(None, False)
            assert set(glob.glob(pattern, recursive=True)) == set(fnames_abs)

            # Load files.
            # :: List[Filename] -> Dict[Filename:YamlStr]
            fdata_raw, err = manio.load_files(tempdir, fnames_rel)
            assert err is False
            assert fdata_raw == fdata_test_in

    def test_load_and_save_sub_dir(self):
        with tempfile.TemporaryDirectory() as tempdir:
            fdata_test_in = {
                "m0.yaml": "something 0",
                "foo/m1.yaml": "something 1",
                "bar/m2.yaml": "something 2",
                "foo/bar/blah/m2.yaml": "something 3",
            }
            fnames_rel = list(fdata_test_in.keys())
            fnames_abs = [pjoin(tempdir, _) for _ in fnames_rel]

            pattern = os.path.join(tempdir, "**", "*.yaml")
            assert glob.glob(pattern, recursive=True) == []
            assert manio.save_files(tempdir, fdata_test_in) == RetVal(None, False)
            assert set(glob.glob(pattern, recursive=True)) == set(fnames_abs)

            # Load files.
            # :: List[Filename] -> Dict[Filename:YamlStr]
            fdata_raw, err = manio.load_files(tempdir, fnames_rel)
            assert err is False
            assert fdata_raw == fdata_test_in
