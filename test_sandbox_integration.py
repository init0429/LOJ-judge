"""Real-Docker sandbox checks. Opt in with RUN_DOCKER_TESTS=1 (needs the docker SDK)."""
import os
import unittest
from unittest.mock import MagicMock, patch

RUN = os.getenv("RUN_DOCKER_TESTS") == "1"

# 표준입력의 정수들을 모두 더해 출력하는 프로그램
SUM = {
    "python": "import sys\nprint(sum(map(int, sys.stdin.read().split())))",
    "c": "#include <stdio.h>\nint main(void){long long s=0,x;while(scanf(\"%lld\",&x)==1)s+=x;printf(\"%lld\\n\",s);return 0;}",
    "cpp": "#include <bits/stdc++.h>\nusing namespace std;\nint main(){long long s=0,x;while(cin>>x)s+=x;cout<<s<<endl;}",
    "java": "import java.util.*;\npublic class Main{static class Acc{long s;}\npublic static void main(String[] a){Scanner sc=new Scanner(System.in);Acc acc=new Acc();while(sc.hasNextLong())acc.s+=sc.nextLong();System.out.println(acc.s);}}",
    "javascript": "const a=require('node:fs').readFileSync(0,'utf8').trim().split(/\\s+/).map(Number);console.log(a.reduce((x,y)=>x+y,0));",
    "typescript": "import {readFileSync} from 'node:fs';const a: number[]=readFileSync(0,'utf8').trim().split(/\\s+/).map(Number);console.log(a.reduce((x,y)=>x+y,0));",
    "ruby": "puts STDIN.read.split.map(&:to_i).sum",
    "php": "<?php echo array_sum(preg_split('/\\s+/',trim(stream_get_contents(STDIN)))).PHP_EOL;",
    "perl": "my $s=0;$s+=$_ for split /\\s+/,do{local $/;<STDIN>};print $s,qq(\\n);",
}


def load_worker():
    os.environ.setdefault("SUPABASE_URL", "https://example.supabase.co")
    os.environ.setdefault("SUPABASE_KEY", "test-key")
    with patch("supabase.create_client"), patch("redis.from_url"):
        import worker
    worker.r = MagicMock()
    worker.supabase = MagicMock()
    return worker


@unittest.skipUnless(RUN, "Set RUN_DOCKER_TESTS=1 to run sandbox integration tests")
class SandboxIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.worker = load_worker()
        cls.worker.pull_images_on_startup()

    def judge(self, language, code, cases, problem=None):
        cfg = self.worker.LANGUAGES[language]
        time_limit, memory_mb = self.worker.run_limits(cfg, problem or {})
        program, message = self.worker.build_program("test", cfg, code)
        if program is None:
            return "Compile Error", message
        for case in cases:
            status, actual = self.worker.run_one_testcase("test", cfg, program, case, time_limit, memory_mb)
            if status != "Accepted":
                return status, actual
        return "Accepted", ""

    def test_every_language_sums_small_input(self):
        for language, code in SUM.items():
            with self.subTest(language=language):
                self.assertEqual(self.judge(language, code, [{"input": "1 2\n", "output": "3\n"}]), ("Accepted", ""))

    def test_input_far_beyond_the_argv_limit(self):
        case = {"input": "7 " * 400_000, "output": "2800000"}  # 800 KB
        for language in ("python", "cpp", "java", "javascript"):
            with self.subTest(language=language):
                self.assertEqual(self.judge(language, SUM[language], [case])[0], "Accepted")

    def test_source_far_beyond_the_argv_limit(self):
        code = SUM["python"] + "\n" + "# padding\n" * 20_000  # 200 KB
        self.assertEqual(self.judge("python", code, [{"input": "1 2", "output": "3"}])[0], "Accepted")

    def test_sql_runs_the_query_against_the_setup_script(self):
        case = {"input": "CREATE TABLE t(a,b);INSERT INTO t VALUES(1,'x'),(2,'y');", "output": "2|y\n1|x"}
        self.assertEqual(self.judge("sql", "SELECT a,b FROM t ORDER BY a DESC", [case])[0], "Accepted")

    def test_verdicts(self):
        case = {"input": "1 2", "output": "3"}
        self.assertEqual(self.judge("python", "print(4)", [case])[0], "Wrong Answer")
        self.assertEqual(self.judge("python", "raise SystemExit(3)", [case])[0], "Runtime Error")
        self.assertEqual(self.judge("python", "while True: pass", [case])[0], "Time Limit Exceeded")
        self.assertEqual(self.judge("c", "int main(void){return x;}", [case])[0], "Compile Error")

    def test_problem_time_limit_is_enforced(self):
        case = {"input": "", "output": "done"}
        slow = "#include <stdio.h>\n#include <unistd.h>\nint main(void){usleep(1500000);puts(\"done\");return 0;}"
        self.assertEqual(self.judge("c", slow, [case], {"time_limit_ms": 1000})[0], "Time Limit Exceeded")
        self.assertEqual(self.judge("c", slow, [case], {"time_limit_ms": 3000})[0], "Accepted")

    def test_compile_time_does_not_count_against_the_run(self):
        # 템플릿이 많은 헤더는 컴파일에만 몇 초가 걸린다. 실행 제한은 0.5초.
        case = {"input": "1 2", "output": "3"}
        self.assertEqual(self.judge("cpp", SUM["cpp"], [case], {"time_limit_ms": 500})[0], "Accepted")

    def test_problem_memory_limit_is_enforced(self):
        case = {"input": "", "output": "ok"}
        # volatile로 써야 -O2가 쓰이지 않는 할당을 지워 버리지 않는다.
        hog = "#include <stdio.h>\n#include <stdlib.h>\nint main(void){size_t n=200u<<20;volatile char*p=malloc(n);if(!p)return 1;for(size_t i=0;i<n;i+=1024)p[i]=1;puts(\"ok\");return 0;}"
        self.assertEqual(self.judge("c", hog, [case], {"memory_limit_mb": 64}), ("Runtime Error", "Memory limit exceeded"))
        self.assertEqual(self.judge("c", hog, [case], {"memory_limit_mb": 512})[0], "Accepted")

    def test_runtime_overhead_is_not_charged_to_the_solution(self):
        # 제한이 가장 작은 16MB여도 각 언어의 가장 단순한 풀이는 돌아야 한다.
        case = {"input": "1 2", "output": "3"}
        for language in ("python", "java", "javascript", "ruby", "php", "perl"):
            with self.subTest(language=language):
                self.assertEqual(self.judge(language, SUM[language], [case], {"memory_limit_mb": 16})[0], "Accepted")

    def test_sandbox_blocks_network_and_root(self):
        code = "import os,socket\nprint(os.getuid())\ntry:\n    socket.create_connection(('1.1.1.1',53),2)\n    print('online')\nexcept OSError:\n    print('offline')"
        status, actual = self.judge("python", code, [{"input": "", "output": "1000\noffline"}])
        self.assertEqual((status, actual), ("Accepted", ""))


if __name__ == "__main__":
    unittest.main()
