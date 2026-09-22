#!/usr/bin/env python3
"""Exercise real Nextflow catalogue discovery, NFS/HTTP fallbacks and resume.

Run inside the combined image (Nextflow, Java, Python and curl required).
Uses temporary files and a loopback HTTP server for all datasource downloads.
"""

import csv
from functools import partial
import http.server
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import threading
import unittest


ROOT = Path(__file__).resolve().parents[1]


@unittest.skipUnless(shutil.which("nextflow"), "Requires Nextflow; run in the combined image")
class DownloadWorkflowTest(unittest.TestCase):
    def test_catalogue_refresh_fallbacks_and_file_cache(self):
        with tempfile.TemporaryDirectory(prefix="grebi-download-test-") as tmp:
            home = Path(tmp)
            ftp = home / "ftp"
            nfs = home / "nfs"
            nfs.mkdir()
            for accession in ("E-MTAB-513", "E-MTAB-4045"):
                shutil.copytree(ROOT / "tests/data/test_expression_atlas" / accession, ftp / accession)
            shutil.copytree(ftp / "E-MTAB-513", nfs / "E-MTAB-513")
            (home / "configs/subgraph_configs").mkdir(parents=True)
            (home / "dataload/00_download").mkdir(parents=True)
            shutil.copy2(ROOT / "dataload/00_download/expression_atlas.py", home / "dataload/00_download/expression_atlas.py")
            (home / "configs/subgraph_configs/test.json").write_text(json.dumps({"datasource_configs": ["source.yaml"]}))
            requests = []

            class Handler(http.server.SimpleHTTPRequestHandler):
                def do_GET(self):
                    requests.append(self.path)
                    super().do_GET()

                def log_message(self, *args):
                    pass

            server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), partial(Handler, directory=str(ftp)))
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                url = f"http://127.0.0.1:{server.server_port}"
                config = {
                    "download_manifests": [{
                        "dest": "expression_atlas/downloads.json",
                        "sources": [str(nfs / "experiments.json"), url + "/experiments.json"],
                        "command": 'python3 "$GREBI_DATALOAD_HOME/00_download/expression_atlas.py" '
                                   f'--source-root "{nfs}" --source-root "{url}" -- "$GREBI_DOWNLOAD_FILENAME"',
                    }],
                    "download": [{"dest": "reference.json", "sources": [str(ROOT / "tests/data/test_expression_atlas/reference_nodes.json")]}],
                }
                (home / "source.yaml").write_text(json.dumps(config))  # JSON is valid YAML
                catalogue = {"experiments": [{"experimentAccession": "E-MTAB-513", "rawExperimentType": "RNASEQ_MRNA_BASELINE"}]}
                downloads = home / "downloads"
                downloads.mkdir()
                env = {**os.environ, "GREBI_HOME": str(home), "GREBI_SUBGRAPH": "test",
                       "GREBI_DOWNLOADS_PATH": str(downloads), "NXF_ANSI_LOG": "false",
                       "NXF_HOME": str(home / "nxf-home")}

                def run(number):
                    (ftp / "experiments.json").write_text(json.dumps(catalogue))
                    result = subprocess.run([
                        "nextflow", str(ROOT / "dataload/nextflow/download.nf"),
                        "-work-dir", str(home / "work"), "-resume", "-ansi-log", "false",
                        "-with-trace", str(home / f"trace-{number}.tsv"),
                    ], cwd=home, env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=180)
                    self.assertEqual(result.returncode, 0, result.stdout)
                    manifest = json.loads((downloads / "expression_atlas/downloads.json").read_text())
                    with (home / f"trace-{number}.tsv").open() as trace:
                        tasks = list(csv.DictReader(trace, delimiter="\t"))
                    discovery = [t for t in tasks if t["name"].startswith("discover_downloads")]
                    self.assertEqual([t["status"] for t in discovery], ["COMPLETED"])
                    return manifest, tasks

                manifest, tasks = run(1)
                self.assertEqual(len(manifest), 3)
                human = downloads / "expression_atlas/experiments/E-MTAB-513/E-MTAB-513-tpms.tsv"
                self.assertTrue(human.is_symlink())  # NFS preferred
                self.assertFalse(any(path.startswith("/E-MTAB-513/") for path in requests))

                catalogue["experiments"].append({"experimentAccession": "E-MTAB-4045", "rawExperimentType": "RNASEQ_MRNA_BASELINE"})
                manifest, tasks = run(2)
                self.assertEqual(len(manifest), 6)
                self.assertEqual(sum(t["status"] == "CACHED" for t in tasks), 4)  # human + static reference
                plant = downloads / "expression_atlas/experiments/E-MTAB-4045/E-MTAB-4045-tpms.tsv"
                self.assertFalse(plant.is_symlink())  # missing NFS files fall back to HTTP
                self.assertEqual(plant.read_bytes(), (ftp / "E-MTAB-4045/E-MTAB-4045-tpms.tsv").read_bytes())

                catalogue["experiments"][0]["rawExperimentType"] = "RNASEQ_MRNA_DIFFERENTIAL"
                manifest, tasks = run(3)
                self.assertEqual(len(manifest), 3)
                self.assertTrue(all("E-MTAB-4045" in e["dest"] for e in manifest))
                self.assertTrue(human.exists())  # stale files retained, no longer listed for ingest
                self.assertEqual(sum(t["status"] == "CACHED" for t in tasks), 4)
                self.assertEqual(requests.count("/experiments.json"), 3)
                self.assertEqual(sum(path.startswith("/E-MTAB-4045/") for path in requests), 3)

                # A partially downloaded manifest must fail ingestion, including
                # when the missing file is a companion rather than a TPM input.
                ingest_root = home / "ingest-downloads/test"
                (ingest_root / "expression_atlas").mkdir(parents=True)
                (ingest_root / "expression_atlas/downloads.json").write_text(json.dumps(manifest))
                for entry in manifest[:-1]:
                    target = ingest_root / entry["dest"]
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.symlink_to(downloads / entry["dest"])
                config["ingests"] = [{"globs": ["expression_atlas/experiments/*/*-tpms.tsv"],
                                      "download_manifest": "expression_atlas/downloads.json"}]
                (home / "source.yaml").write_text(json.dumps(config))
                result = subprocess.run([
                    "nextflow", str(ROOT / "dataload/nextflow/main.nf"), "-ansi-log", "false",
                ], cwd=home, env={**env, "GREBI_SUBGRAPHS": "test", "GREBI_OUT_DIR": str(home / "out"),
                                 "GREBI_QUERY_YAMLS_PATH": str(ROOT / "query_templates"),
                                 "GREBI_DATALOAD_HOME": str(ROOT / "dataload"),
                                 "GREBI_DOWNLOADS_PATH": str(home / "ingest-downloads")},
                    text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=180)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("Missing/empty required download", result.stdout)
                self.assertIn(".condensed-sdrf.tsv", result.stdout)

                # Existing static-only datasource configs still work unchanged.
                del config["download_manifests"]
                (home / "source.yaml").write_text(json.dumps(config))
                result = subprocess.run([
                    "nextflow", str(ROOT / "dataload/nextflow/download.nf"), "-resume", "-ansi-log", "false",
                ], cwd=home, env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=180)
                self.assertEqual(result.returncode, 0, result.stdout)
                self.assertTrue((downloads / "reference.json").is_file())
            finally:
                server.shutdown()
                server.server_close()
                thread.join()

    def test_command_entry_populates_its_directory_and_is_not_cached(self):
        with tempfile.TemporaryDirectory(prefix="grebi-download-test-") as tmp:
            home = Path(tmp)
            (home / "configs/subgraph_configs").mkdir(parents=True)
            (home / "dataload").mkdir()
            (home / "configs/subgraph_configs/test.json").write_text(json.dumps({"datasource_configs": ["source.yaml"]}))
            config = {"download": [
                # The command sees the dest dir (already created) and the dataload home.
                {"dest": "studies/fire/", "command": 'test -d "$GREBI_DOWNLOAD_DEST" && test -d "$GREBI_DATALOAD_HOME" '
                                                    '&& mkdir -p "$GREBI_DOWNLOAD_DEST/S-1" '
                                                    '&& python3 -c "import time; print(time.time_ns())" > "$GREBI_DOWNLOAD_DEST/S-1/S-1.json"'},
                {"dest": "reference.json", "sources": [str(ROOT / "tests/data/test_expression_atlas/reference_nodes.json")]},
            ]}
            (home / "source.yaml").write_text(json.dumps(config))
            downloads = home / "downloads"
            downloads.mkdir()
            env = {**os.environ, "GREBI_HOME": str(home), "GREBI_SUBGRAPH": "test",
                   "GREBI_DOWNLOADS_PATH": str(downloads), "NXF_ANSI_LOG": "false",
                   "NXF_HOME": str(home / "nxf-home"), "GREBI_DATALOAD_HOME": str(ROOT / "dataload")}

            def run(number):
                result = subprocess.run([
                    "nextflow", str(ROOT / "dataload/nextflow/download.nf"),
                    "-work-dir", str(home / "work"), "-resume", "-ansi-log", "false",
                    "-with-trace", str(home / f"trace-{number}.tsv"),
                ], cwd=home, env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=180)
                self.assertEqual(result.returncode, 0, result.stdout)
                with (home / f"trace-{number}.tsv").open() as trace:
                    return {t["name"]: t["status"] for t in csv.DictReader(trace, delimiter="\t")}

            tasks = run(1)
            study = downloads / "studies/fire/S-1/S-1.json"
            self.assertTrue(study.is_file())
            first = study.read_text()
            self.assertTrue((downloads / "reference.json").is_file())
            self.assertEqual(tasks["download_command (studies/fire/)"], "COMPLETED")

            tasks = run(2)
            self.assertEqual(tasks["download_command (studies/fire/)"], "COMPLETED")  # reran: the command decides freshness
            self.assertNotEqual(study.read_text(), first)
            self.assertEqual(sum(status == "CACHED" for name, status in tasks.items() if name.startswith("download_file")), 1)

            # A command entry must own a directory, not a file.
            config["download"][0]["dest"] = "studies/fire.json"
            (home / "source.yaml").write_text(json.dumps(config))
            result = subprocess.run([
                "nextflow", str(ROOT / "dataload/nextflow/download.nf"), "-ansi-log", "false",
            ], cwd=home, env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=180)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("Invalid command download entry", result.stdout)


if __name__ == "__main__":
    unittest.main()
