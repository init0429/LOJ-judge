"""Static checks on the language table; see test_sandbox_integration for Docker runs."""
import unittest

from test_worker_safety import load_worker


class LanguageTests(unittest.TestCase):
    def test_every_language_is_fully_configured(self):
        worker = load_worker()
        STDIN_FILE = worker.STDIN_FILE
        for language, cfg in worker.LANGUAGES.items():
            with self.subTest(language=language):
                self.assertTrue(cfg["image"])
                self.assertGreaterEqual(cfg["time_factor"], 1)
                self.assertGreaterEqual(cfg["time_extra"], 0)
                self.assertGreater(cfg["mem_extra"], 0)
                if cfg["compile"] is None:
                    self.assertNotIn("artifact", cfg)
                else:
                    # 실행 명령은 컴파일 결과물만 쓴다. 소스는 실행 컨테이너에 없다.
                    self.assertIn(cfg["artifact"], cfg["run"])
                    self.assertIn(cfg["filename"], cfg["compile"])
                if cfg.get("prepare"):
                    # 미리 만든 파일은 실행 컨테이너에 같이 들어가므로 실행 명령이 써야 한다.
                    self.assertIn(cfg["prepared"], cfg["run"])
                    self.assertNotIn(cfg["prepared"], (cfg["filename"], STDIN_FILE))

    def test_runtimes_with_their_own_heap_cap_follow_the_problem_limit(self):
        languages = load_worker().LANGUAGES
        for language in ("java", "javascript", "typescript", "php"):
            with self.subTest(language=language):
                self.assertIn("{mem}", languages[language]["run"])


if __name__ == "__main__":
    unittest.main()
