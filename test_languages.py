"""Run with python -m unittest test_languages; Docker checks are opt-in."""
import ast
import os
from pathlib import Path
import subprocess
import unittest


def load_languages():
    tree = ast.parse(Path(__file__).with_name("worker.py").read_text())
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == "LANGUAGES" for t in node.targets):
            return ast.literal_eval(node.value)
    raise AssertionError("Missing LANGUAGES")


SAMPLES = {
    "javascript": "const a=require('node:fs').readFileSync(0,'utf8').trim().split(/\\s+/).map(Number);console.log(a[0]+a[1]);",
    "typescript": "import {readFileSync} from 'node:fs';const a: number[]=readFileSync(0,'utf8').trim().split(/\\s+/).map(Number);console.log(a[0]+a[1]);",
    "ruby": "a,b=STDIN.read.split.map(&:to_i);puts a+b",
    "php": "<?php $a=preg_split('/\\s+/',trim(stream_get_contents(STDIN)));echo ($a[0]+$a[1]).PHP_EOL;",
    "perl": "my $s=do{local $/;<STDIN>};my @a=split /\\s+/,$s;print $a[0]+$a[1],qq(\\n);",
}


class LanguageTests(unittest.TestCase):
    def test_new_language_configuration(self):
        languages = load_languages()
        for language in SAMPLES:
            with self.subTest(language=language):
                cfg = languages[language]
                self.assertIsNone(cfg["compile"])
                self.assertIn(cfg["filename"], cfg["run"])
                self.assertGreater(cfg["timeout"], 0)
                self.assertTrue(cfg["mem_limit"].endswith("m"))

    @unittest.skipUnless(os.getenv("RUN_DOCKER_TESTS") == "1", "Set RUN_DOCKER_TESTS=1 to run sandbox smoke tests")
    def test_addition_in_sandbox(self):
        import base64
        for language, code in SAMPLES.items():
            cfg = load_languages()[language]
            encoded = base64.b64encode(code.encode()).decode()
            command = f"echo {encoded} | base64 -d > /tmp/{cfg['filename']} && printf '1 2' | {cfg['run']}"
            with self.subTest(language=language):
                result = subprocess.run([
                    "docker", "run", "--rm", "--network=none", "--user=1000:1000",
                    "--cap-drop=ALL", "--pids-limit=50", f"--memory={cfg['mem_limit']}",
                    "--workdir=/tmp", cfg["image"], "sh", "-c", command,
                ], capture_output=True, text=True, timeout=120)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stdout.strip(), "3")
