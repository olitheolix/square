import copy
import tempfile

import manio
import square
import test_square


def mk_deploy(name: str):
    return test_square.make_manifest("Deployment", name, "namespace")


class TestYamlManifestIO:
    def test_manifest_lifecycle(self):
        # Setup test.
        dply = [mk_deploy(f"d_{_}") for _ in range(10)]
        meta = [square.make_meta(_) for _ in dply]
        fdata_test_in = {
            "m0.yaml": [dply[0], dply[1], dply[2]],
            "m1.yaml": [dply[3], dply[4]],
            "m2.yaml": [dply[5]],
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
        # Merge the server manifests into the local ones.
        # * Upsert local with server values
        # * Delete all local manifests not on the server
        # :: Dict[MetaManifests:YamlDict] -> Dict[MetaManifest:YamlDict]
        updated_manifests = manio.sync(local_manifests, server_manifests)

        # Associate the manifests with the local filenames.
        # :: Dict[MetaManifests:YamlDict] -> Dict[Filename:List[(MetaManifest, YamlDict)]]
        fdata_meta_new = manio.pack(updated_manifests, fdata_meta)

        # Strip the meta information
        # :: Dict[Filename:List[(MetaManifest, YamlDict)]] -> Dict[Filename:YamlStr]
        fdata_raw_new = manio.unparse(fdata_meta_new)

        # Expected output after we merged back the changes (ie `dply[1]` is
        # different, `dply[{3,5}]` were deleted and `dply[{6,7}]` are new).
        # The new manifests must all end up in "default.yaml".
        fdata_test_out = {
            "m0.yaml": [dply[0], server_manifests[meta[1]], dply[2]],
            "m1.yaml": [dply[4]],
            "default.yaml": [dply[6], dply[7]],
        }
        assert fdata_raw_new == fdata_test_out

    def test_load(self):
        dply = [mk_deploy(f"d_{_}") for _ in range(10)]
        fdata_test_in = {
            "m0.yaml": [dply[0], dply[1], dply[2]],
            "m1.yaml": [dply[3], dply[4]],
            "m2.yaml": [dply[5]],
        }
        with tempfile.TemporaryDirectory() as tempdir:
            self.save_test_files(tempdir, fdata_test_in)

            # Find the YAML files (does not actually load them) -> List[Str]
            fnames, _ = manio.find(tempdir)

            # Load files.
            # :: List[Filename] -> Dict[Filename:YamlStr]
            fdata_raw, _ = manio.load(fnames)
            assert fdata_raw == fdata_test_in

    def test_save(self):
        dply = [mk_deploy(f"d_{_}") for _ in range(10)]
        fdata_test_in = {
            "m0.yaml": [dply[0], dply[1], dply[2]],
            "m1.yaml": [dply[3], dply[4]],
            "m2.yaml": [dply[5]],
        }

        # Save the files. Explicitly delete all those files whose YAML string
        # is empty.
        with tempfile.TemporaryDirectory() as tempdir:
            manio.save(tempdir, fdata_test_in)

            # Load back the results.
            assert False

            # Create a new file.
            fdata_test_in["new.yaml"] = dply[6]
            manio.save(tempdir, fdata_test_in)
            assert False

            # Delete one of the files.
            del fdata_test_in["m0.yaml"]
            manio.save(tempdir, fdata_test_in)
            assert False
