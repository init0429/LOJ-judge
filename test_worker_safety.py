"""Isolated worker regressions without connecting to external services."""
import importlib.util
import io
import json
import os
from pathlib import Path
import sys
import tarfile
from types import SimpleNamespace
import unittest
import unittest.mock
from unittest.mock import MagicMock, Mock, patch


class APIError(Exception):
    status_code = 500


class ImageNotFound(APIError):
    status_code = 404


def load_worker():
    """Import worker.py with every external service replaced by a stub."""
    docker = SimpleNamespace(
        from_env=Mock(),
        errors=SimpleNamespace(APIError=APIError, ImageNotFound=ImageNotFound),
        types=SimpleNamespace(LogConfig=Mock()),
    )
    redis = MagicMock()
    redis.exceptions.ConnectionError = ConnectionError
    stubs = {
        "docker": docker,
        "docker.errors": docker.errors,
        "docker.types": docker.types,
        "redis": redis,
        "supabase": MagicMock(),
        "requests": MagicMock(),
        "requests.exceptions": SimpleNamespace(ReadTimeout=TimeoutError, ConnectionError=ConnectionError),
        "urllib3": MagicMock(),
        "urllib3.exceptions": SimpleNamespace(ReadTimeoutError=TimeoutError),
    }
    spec = importlib.util.spec_from_file_location("worker_under_test", Path(__file__).with_name("worker.py"))
    module = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, stubs):
        spec.loader.exec_module(module)
    module.client = Mock()
    module.r = Mock()
    module.supabase = Mock()
    return module


FILES = {"main.py": b"print(3)"}
C = {"image": "gcc", "filename": "main.c", "compile": "gcc -o /tmp/main /tmp/main.c",
     "artifact": "main", "run": "/tmp/main", "time_factor": 1, "time_extra": 0.0, "mem_extra": 16}
PYTHON = {"image": "python", "filename": "main.py", "compile": None,
          "run": "python3 /tmp/main.py", "time_factor": 3, "time_extra": 2.0, "mem_extra": 32}
JAVA = {"image": "jdk", "filename": "Main.java", "compile": "javac", "artifact": "main.jar",
        "run": "java -Xmx{mem}m Main", "time_factor": 2, "time_extra": 1.0, "mem_extra": 128}
CASE = {"input": "1 2", "output": "3\n"}


def run(worker, files=FILES, **options):
    options = {"mem_mb": 128, "timeout": 1} | options
    return worker._run_container("python", "run", files, **options)


def sandbox(worker, chunks, wait=None):
    """Wire a fake container whose attached stream yields the given chunks."""
    container = Mock()
    if isinstance(wait, Exception):
        container.wait.side_effect = wait
    else:
        container.wait.return_value = wait or {"StatusCode": 0}
    stream = Mock()
    stream.__iter__ = Mock(return_value=iter(chunks))
    container.attach.return_value = stream
    worker.client.containers.create.return_value = container
    return container, stream


class WorkerSafetyTests(unittest.TestCase):
    def test_expired_test_job_does_not_execute(self):
        worker = load_worker()
        worker._process_submission = Mock()
        worker.process_submission({"is_test": True, "expires_at": 0})
        worker._process_submission.assert_not_called()

    def test_processing_failure_marks_submission_terminal(self):
        worker = load_worker()
        worker._process_submission = Mock(side_effect=RuntimeError("Docker unavailable"))
        worker.update_status = Mock()
        with self.assertLogs(level="ERROR"):
            worker.process_submission({"submission_id": "one"})
        worker.update_status.assert_called_once_with("one", "System Error")

    def test_processing_failure_publishes_test_result(self):
        worker = load_worker()
        worker._process_submission = Mock(side_effect=RuntimeError("Docker unavailable"))
        with self.assertLogs(level="ERROR"):
            worker.process_submission({"submission_id": "one", "is_test": True})
        channel, payload = worker.r.publish.call_args.args
        self.assertEqual(channel, "test_result:one")
        self.assertEqual(json.loads(payload)["status"], "System Error")

    def test_container_storage_limits_and_stdout_capture(self):
        worker = load_worker()
        container, _ = sandbox(worker, [(b"3\n", b"diagnostic")])
        container.attrs = {"State": {
            "StartedAt": "2026-11-17T08:00:00.250000000Z",
            "FinishedAt": "2026-11-17T08:00:01.750000000Z",
            "OOMKilled": False,
        }}
        result = run(worker)
        self.assertEqual(result, worker.SandboxResult(0, b"3\n", b"diagnostic", elapsed=1.5))
        self.assertEqual(result.text(), "3")
        container.logs.assert_not_called()
        worker.docker.types.LogConfig.assert_called_once_with(type="none")
        options = worker.client.containers.create.call_args.kwargs
        self.assertTrue(options["read_only"])
        self.assertEqual((options["mem_limit"], options["memswap_limit"]), ("128m", "128m"))
        self.assertEqual(options["nano_cpus"], 1_000_000_000)
        self.assertEqual(options["security_opt"], ["no-new-privileges"])
        self.assertIn("size=64m", options["tmpfs"]["/tmp"])
        self.assertIn("exec", options["tmpfs"]["/tmp"].split(","))
        container.remove.assert_called_once_with(force=True)

    def test_files_reach_the_sandbox_over_stdin_not_argv(self):
        worker = load_worker()
        container, _ = sandbox(worker, [(b"ok", None)])
        files = {"main.py": b"print(1)", ".stdin": b"7 " * 400_000}
        run(worker, files)
        options = worker.client.containers.create.call_args.kwargs
        self.assertEqual(options["command"], ["sh", "-c", "run"])
        self.assertTrue(options["stdin_open"])
        self.assertFalse(options["detach"])
        raw = container.attach_socket.return_value._sock
        with tarfile.open(fileobj=io.BytesIO(raw.sendall.call_args.args[0])) as archive:
            self.assertEqual({m.name: archive.extractfile(m).read() for m in archive}, files)
            self.assertEqual({m.uid for m in archive}, {1000})
        raw.shutdown.assert_called_once()

    def test_undelivered_bundle_is_a_system_failure(self):
        worker = load_worker()
        sandbox(worker, [(None, f"tar: short read\n{worker.BUNDLE_FAILED}\n".encode())], wait={"StatusCode": 125})
        with self.assertRaises(RuntimeError):
            run(worker)

    def test_program_ending_before_reading_input_is_not_an_error(self):
        worker = load_worker()
        container, _ = sandbox(worker, [(b"3", None)])
        container.attach_socket.return_value._sock.sendall.side_effect = BrokenPipeError()
        self.assertEqual(run(worker).text(), "3")

    def test_testcase_runs_the_prebuilt_program_without_compiling(self):
        worker = load_worker()
        worker._run_container = Mock(return_value=worker.SandboxResult(0, b"3\n", elapsed=0.4))
        status, _ = worker.run_one_testcase("one", C, {"main": b"ELF"}, CASE, 1.0, 256)
        self.assertEqual(status, "Accepted")
        image, command, files = worker._run_container.call_args.args
        self.assertEqual(command, f"{worker.UNPACK} && /tmp/main < /tmp/.stdin")
        self.assertEqual(files, {"main": b"ELF", ".stdin": b"1 2"})
        # 256MB 제한 + 런타임 몫 16MB + tmpfs에 풀리는 묶음 1MB, 제한 1초 + 여유
        self.assertEqual(worker._run_container.call_args.kwargs,
                         {"mem_mb": 256 + 16 + 1, "timeout": 1.0 + worker.TIME_GRACE})

    def test_problem_limits_are_scaled_per_language(self):
        worker = load_worker()
        problem = {"time_limit_ms": 1000, "memory_limit_mb": 256}
        self.assertEqual(worker.run_limits(C, problem), (1.0, 256))
        self.assertEqual(worker.run_limits(JAVA, problem), (3.0, 256))
        self.assertEqual(worker.run_limits(PYTHON, problem), (5.0, 256))
        self.assertEqual(worker.run_limits(C, {}), (2.0, 128))
        self.assertEqual(worker.run_limits(C, {"time_limit_ms": 10**9, "memory_limit_mb": 1}), (30.0, 16))

    def test_memory_limit_reaches_the_runtime_and_the_container(self):
        worker = load_worker()
        worker._run_container = Mock(return_value=worker.SandboxResult(0, b"3", elapsed=0.1))
        worker.run_one_testcase("one", JAVA, {"main.jar": b"PK"}, CASE, 3.0, 256)
        self.assertIn("java -Xmx256m Main", worker._run_container.call_args.args[1])
        self.assertEqual(worker._run_container.call_args.kwargs["mem_mb"], 256 + 128 + 1)

    def test_big_input_does_not_eat_into_the_memory_limit(self):
        worker = load_worker()
        worker._run_container = Mock(return_value=worker.SandboxResult(0, b"3", elapsed=0.1))
        case = {"input": "7" * (5 * 1024 * 1024), "output": "3"}
        worker.run_one_testcase("one", C, {"main": b"ELF"}, case, 1.0, 128)
        self.assertEqual(worker._run_container.call_args.kwargs["mem_mb"], 128 + 16 + 6)

    def test_run_time_decides_time_limit_not_only_the_hard_kill(self):
        worker = load_worker()
        for result, expected in (
            (worker.SandboxResult(0, b"3", elapsed=0.99), "Accepted"),
            (worker.SandboxResult(0, b"3", elapsed=1.2), "Time Limit Exceeded"),
            (worker.SandboxResult(None, timed_out=True), "Time Limit Exceeded"),
            (worker.SandboxResult(137, oom=True, elapsed=0.2), "Runtime Error"),
        ):
            worker._run_container = Mock(return_value=result)
            self.assertEqual(worker.run_one_testcase("one", C, {"main": b"ELF"}, CASE, 1.0, 128)[0], expected)

    def test_trailing_whitespace_and_line_endings_do_not_fail_an_answer(self):
        worker = load_worker()
        same = [("1 2 3 \n4\n", "1 2 3\n4"), ("a\r\nb\r\n", "a\nb"), ("\n\n3\n\n", "3"), ("x\t\ny", "x\ny\n")]
        different = [("1  2", "1 2"), (" 1\n2", "1\n 2"), ("1\n\n2", "1\n2"), ("3", "3\n4")]
        for actual, expected in same:
            self.assertTrue(worker.outputs_match(actual, expected), (actual, expected))
        for actual, expected in different:
            self.assertFalse(worker.outputs_match(actual, expected), (actual, expected))

    def test_run_time_falls_back_to_the_worker_clock(self):
        worker = load_worker()
        container = Mock()
        container.attrs = {"State": {"StartedAt": "garbage", "FinishedAt": "garbage"}}
        self.assertEqual(worker._run_stats(container, fallback=0.7), (0.7, False))
        container.attrs = {"State": {"StartedAt": "2026-11-17T08:00:00Z", "FinishedAt": "2026-11-17T08:00:02.5Z", "OOMKilled": True}}
        self.assertEqual(worker._run_stats(container, fallback=0.7), (2.5, True))

    def test_compiled_languages_build_once_per_submission(self):
        worker = load_worker()
        worker.LANGUAGES = {"c": C}
        worker.update_status = Mock()
        worker._run_container = Mock(side_effect=[worker.SandboxResult(0, b"ELF")] + [worker.SandboxResult(0, b"3")] * 3)
        worker.load_problem = Mock(return_value=({}, [{"input": "1 2", "output": "3"}] * 3))
        worker._process_submission({"submission_id": "one", "problem_id": 7, "language": "c", "code": "int main(){}"})
        worker.load_problem.assert_called_once_with(7, public_only=False)
        commands = [call.args[1] for call in worker._run_container.call_args_list]
        self.assertEqual([C["compile"] in command for command in commands], [True, False, False, False])
        self.assertTrue(commands[0].endswith("1>&2 && cat /tmp/main"))
        compile_options = worker._run_container.call_args_list[0].kwargs
        self.assertEqual(compile_options["mem_mb"], worker.COMPILE_MEM_MB)
        self.assertEqual(compile_options["timeout"], worker.COMPILE_TIMEOUT)
        worker.update_status.assert_called_with("one", "Accepted")

    def test_test_cases_come_from_the_database_not_the_queue(self):
        worker = load_worker()
        query = worker.supabase.table.return_value
        for method in ("select", "eq", "order"):
            getattr(query, method).return_value = query
        query.execute.side_effect = [
            SimpleNamespace(data=[{"time_limit_ms": 1000, "memory_limit_mb": 256}]),
            SimpleNamespace(data=[{"input_data": "1 2", "expected_output": "3"}, {"input_data": None, "expected_output": "0"}]),
        ]
        problem, cases = worker.load_problem(7, public_only=True)
        self.assertEqual(problem, {"time_limit_ms": 1000, "memory_limit_mb": 256})
        self.assertEqual(cases, [{"input": "1 2", "output": "3"}, {"input": "", "output": "0"}])
        query.eq.assert_any_call("is_hidden", False)

    def test_hidden_cases_are_included_for_submissions(self):
        worker = load_worker()
        query = worker.supabase.table.return_value
        for method in ("select", "eq", "order"):
            getattr(query, method).return_value = query
        query.execute.side_effect = [SimpleNamespace(data=[{}]), SimpleNamespace(data=[])]
        worker.load_problem(7, public_only=False)
        self.assertNotIn(unittest.mock.call("is_hidden", False), query.eq.call_args_list)

    def test_missing_problem_is_reported_as_a_system_error(self):
        worker = load_worker()
        worker.supabase.table.return_value.select.return_value.eq.return_value.execute.return_value = SimpleNamespace(data=[])
        worker.update_status = Mock()
        with self.assertLogs(level="ERROR"):
            worker.process_submission({"submission_id": "one", "problem_id": 7, "language": "python", "code": "print(1)"})
        worker.update_status.assert_called_with("one", "System Error")

    def test_interpreted_languages_ship_the_source_as_is(self):
        worker = load_worker()
        worker._run_container = Mock()
        self.assertEqual(worker.build_program("one", PYTHON, "print(3)"), ({"main.py": b"print(3)"}, ""))
        worker._run_container.assert_not_called()

    def test_compile_failures_report_the_compiler_diagnostics(self):
        worker = load_worker()
        for result, expected in (
            (worker.SandboxResult(1, b"", b"main.c:1: error: nope\n"), "main.c:1: error: nope"),
            (worker.SandboxResult(None, timed_out=True), "컴파일 시간 초과"),
            (worker.SandboxResult(1, overflow=True), "컴파일 출력이 너무 큽니다"),
        ):
            worker._run_container = Mock(return_value=result)
            self.assertEqual(worker.build_program("one", C, "int main(){}"), (None, expected))

    def test_output_overflow_never_returns_success(self):
        for chunks in ([(b"3", None), (b"x" * 1048576, None)],
                       [(b"3", b"x" * 1048576)]):
            worker = load_worker()
            container, _ = sandbox(worker, chunks)
            result = run(worker)
            self.assertTrue(result.overflow)
            self.assertNotEqual(result.exit_code, 0)
            worker._run_container = Mock(return_value=result)
            self.assertEqual(worker.run_one_testcase("one", PYTHON, FILES, {"input": "", "output": "3"}, 5.0, 128),
                             ("Runtime Error", "Output limit exceeded"))
            container.kill.assert_called_once()
            container.remove.assert_called_once_with(force=True)

    def test_wrapped_timeout_and_cleanup_failure(self):
        worker = load_worker()
        container, stream = sandbox(worker, [], wait=ConnectionError(TimeoutError("read timed out")))
        stream.close.side_effect = OSError("already closed")
        container.attach_socket.return_value.close.side_effect = RuntimeError("broken")
        with self.assertLogs(level="ERROR"):
            result = run(worker)
        self.assertEqual(result, worker.SandboxResult(None, timed_out=True))
        container.remove.assert_called_once_with(force=True)


class SettingsTests(unittest.TestCase):
    def load(self, cpus, **env):
        with patch.dict("os.environ", env, clear=False), patch("os.cpu_count", return_value=cpus):
            for name in ("JUDGE_CONCURRENCY", "JUDGE_COMPILE_MEMORY_MB"):
                if name not in env:
                    os.environ.pop(name, None)
            return load_worker()

    def test_concurrency_leaves_a_core_for_the_rest_of_the_stack(self):
        self.assertEqual(self.load(2).CONCURRENCY, 1)
        self.assertEqual(self.load(1).CONCURRENCY, 1)
        self.assertEqual(self.load(8).CONCURRENCY, 7)
        self.assertEqual(self.load(None).CONCURRENCY, 1)

    def test_settings_can_be_overridden_but_not_below_a_floor(self):
        worker = self.load(2, JUDGE_CONCURRENCY="3", JUDGE_COMPILE_MEMORY_MB="512")
        self.assertEqual((worker.CONCURRENCY, worker.COMPILE_MEM_MB), (3, 512))
        worker = self.load(2, JUDGE_CONCURRENCY="0", JUDGE_COMPILE_MEMORY_MB="64")
        self.assertEqual((worker.CONCURRENCY, worker.COMPILE_MEM_MB), (1, 192))
        self.assertEqual(self.load(2, JUDGE_CONCURRENCY="").CONCURRENCY, 1)
        self.assertEqual(self.load(2).COMPILE_MEM_MB, 320)


class QueueRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.worker = load_worker()
        self.worker.update_status = Mock()
        self.lists = {self.worker.QUEUE: [], self.worker.PROCESSING: []}
        self.worker.r.rpop.side_effect = lambda key: self.lists[key].pop() if self.lists[key] else None
        self.worker.r.lpush.side_effect = lambda key, value: self.lists[key].insert(0, value)

    def job(self, name, **extra):
        return json.dumps({"submission_id": name, **extra})

    def test_interrupted_jobs_return_to_the_front_in_order(self):
        self.lists[self.worker.QUEUE] = [self.job("waiting")]
        self.lists[self.worker.PROCESSING] = [self.job("first"), self.job("second")]
        self.worker.recover_interrupted_jobs()
        order = [json.loads(item)["submission_id"] for item in self.lists[self.worker.QUEUE]]
        self.assertEqual(order, ["first", "second", "waiting"])
        self.assertEqual(self.lists[self.worker.PROCESSING], [])
        self.assertEqual(json.loads(self.lists[self.worker.QUEUE][0])["attempts"], 1)

    def test_job_that_keeps_killing_the_worker_is_failed(self):
        self.lists[self.worker.PROCESSING] = [self.job("poison", attempts=self.worker.MAX_ATTEMPTS - 1)]
        with self.assertLogs(level="ERROR"):
            self.worker.recover_interrupted_jobs()
        self.assertEqual(self.lists[self.worker.QUEUE], [])
        self.worker.update_status.assert_called_once_with("poison", "System Error")

    def test_malformed_leftovers_are_dropped(self):
        self.lists[self.worker.PROCESSING] = ["not json"]
        self.worker.recover_interrupted_jobs()
        self.assertEqual(self.lists, {self.worker.QUEUE: [], self.worker.PROCESSING: []})


class TestRunPayloadTests(unittest.TestCase):
    def test_huge_fields_are_clipped_for_test_runs(self):
        worker = load_worker()
        worker.LANGUAGES = {"python": PYTHON}
        big = "9" * (worker.TEST_FIELD_LIMIT * 4)
        worker.load_problem = Mock(return_value=({}, [{"input": big, "output": big}]))
        worker._run_container = Mock(return_value=worker.SandboxResult(0, b"1", elapsed=0.1))
        worker._process_submission({"submission_id": "t", "problem_id": 1, "language": "python", "code": "print(1)", "is_test": True})
        payload = worker.r.publish.call_args.args[1]
        result = json.loads(payload)
        self.assertEqual(result["status"], "Wrong Answer")
        self.assertLess(len(payload), worker.TEST_FIELD_LIMIT * 8)
        self.assertTrue(result["test_cases"][0]["input"].endswith("(이하 생략)"))


if __name__ == "__main__":
    unittest.main()
