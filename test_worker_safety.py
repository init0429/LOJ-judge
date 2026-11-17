"""Isolated worker regressions without connecting to external services."""
import importlib.util
import io
import json
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
     "artifact": "main", "run": "/tmp/main", "mem_limit": "128m", "timeout": 5.0}
PYTHON = {"image": "python", "filename": "main.py", "compile": None,
          "run": "python3 /tmp/main.py", "mem_limit": "128m", "timeout": 5.0}


def run(worker, files=FILES, **options):
    options = {"mem_limit": "128m", "timeout": 1} | options
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
        result = run(worker)
        self.assertEqual(result, worker.SandboxResult(0, b"3\n", b"diagnostic"))
        self.assertEqual(result.text(), "3")
        container.logs.assert_not_called()
        worker.docker.types.LogConfig.assert_called_once_with(type="none")
        options = worker.client.containers.create.call_args.kwargs
        self.assertTrue(options["read_only"])
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
        worker._run_container = Mock(return_value=worker.SandboxResult(0, b"3\n"))
        status, _ = worker.run_one_testcase("one", C, {"main": b"ELF"}, {"input": "1 2", "output": "3\n"})
        self.assertEqual(status, "Accepted")
        image, command, files = worker._run_container.call_args.args
        self.assertEqual(command, f"{worker.UNPACK} && /tmp/main < /tmp/.stdin")
        self.assertEqual(files, {"main": b"ELF", ".stdin": b"1 2"})
        self.assertEqual(worker._run_container.call_args.kwargs, {"mem_limit": "128m", "timeout": 5.0})

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
        self.assertEqual(compile_options["mem_limit"], worker.COMPILE_MEM_LIMIT)
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
            self.assertEqual(worker.run_one_testcase("one", PYTHON, FILES, {"input": "", "output": "3"}),
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


if __name__ == "__main__":
    unittest.main()
