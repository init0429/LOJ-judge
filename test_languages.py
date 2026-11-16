"""Static checks on the language table; see test_sandbox_integration for Docker runs."""
import ast
from pathlib import Path
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


if __name__ == "__main__":
    unittest.main()
