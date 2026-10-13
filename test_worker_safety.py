"""Isolated worker regressions without connecting to external services."""
import ast
import json
import logging
import threading
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import Mock


def worker_functions():
    tree = ast.parse(Path(__file__).with_name("worker.py").read_text())
    functions = ast.Module(
        body=[node for node in tree.body if isinstance(node, ast.FunctionDef)],
        type_ignores=[],
    )
    namespace = {"json": json, "logging": logging, "threading": threading,
                 "ReadTimeout": TimeoutError, "RequestsConnectionError": ConnectionError,
                 "ReadTimeoutError": TimeoutError}
    exec(compile(functions, "worker.py", "exec"), namespace)
    return namespace


class WorkerSafetyTests(unittest.TestCase):
    def test_processing_failure_marks_submission_terminal(self):
        ns = worker_functions()
        ns["_process_submission"] = Mock(side_effect=RuntimeError("Docker unavailable"))
        ns["update_status"] = Mock()
        with self.assertLogs(level="ERROR"):
            ns["process_submission"]({"submission_id": "one"})
        ns["update_status"].assert_called_once_with("one", "System Error")

    def test_processing_failure_publishes_test_result(self):
        ns = worker_functions()
        ns["_process_submission"] = Mock(side_effect=RuntimeError("Docker unavailable"))
        ns["r"] = Mock()
        with self.assertLogs(level="ERROR"):
            ns["process_submission"]({"submission_id": "one", "is_test": True})
        channel, payload = ns["r"].publish.call_args.args
        self.assertEqual(channel, "test_result:one")
        self.assertEqual(json.loads(payload)["status"], "System Error")

    def test_container_storage_limits_and_stdout_capture(self):
        ns = worker_functions()
        container = Mock()
        container.wait.return_value = {"StatusCode": 0}
        stream = Mock()
        stream.__iter__ = Mock(return_value=iter([(b"3\n", b"diagnostic")]))
        container.attach.return_value = stream
        client = Mock()
        client.containers.create.return_value = container
        ns["client"] = client
        ns["docker"] = SimpleNamespace(types=SimpleNamespace(LogConfig=Mock()))
        result = ns["_run_container"]({"image": "python", "mem_limit": "128m"}, "run", 1)
        self.assertEqual(result, (0, "3", False))
        container.logs.assert_not_called()
        ns["docker"].types.LogConfig.assert_called_once_with(type="none")
        options = client.containers.create.call_args.kwargs
        self.assertTrue(options["read_only"])
        self.assertIn("size=64m", options["tmpfs"]["/tmp"])
        self.assertIn("exec", options["tmpfs"]["/tmp"].split(","))
        container.remove.assert_called_once_with(force=True)

    def test_output_overflow_never_returns_success(self):
        for chunks in ([(b"3", None), (b"x" * 1048576, None)],
                       [(b"3", b"x" * 1048576)]):
            ns = worker_functions()
            container = Mock()
            container.wait.return_value = {"StatusCode": 0}
            stream = Mock()
            stream.__iter__ = Mock(return_value=iter(chunks))
            container.attach.return_value = stream
            ns["client"] = Mock()
            ns["client"].containers.create.return_value = container
            ns["docker"] = SimpleNamespace(types=SimpleNamespace(LogConfig=Mock()))
            result = ns["_run_container"]({"image": "python", "mem_limit": "128m"}, "run", 1)
            self.assertEqual(result, (1, "Output limit exceeded", False))
            container.kill.assert_called_once()
            container.remove.assert_called_once_with(force=True)

    def test_wrapped_timeout_and_cleanup_failure(self):
        ns = worker_functions()
        container = Mock()
        stream = Mock()
        stream.__iter__ = Mock(return_value=iter([]))
        stream.close.side_effect = OSError("closed")
        container.attach.return_value = stream
        container.wait.side_effect = ConnectionError(TimeoutError("read timed out"))
        ns["client"] = Mock()
        ns["client"].containers.create.return_value = container
        ns["docker"] = SimpleNamespace(types=SimpleNamespace(LogConfig=Mock()))
        with self.assertLogs(level="ERROR"):
            result = ns["_run_container"]({"image": "python", "mem_limit": "128m"}, "run", 1)
        self.assertEqual(result, (None, "", True))
        container.remove.assert_called_once_with(force=True)


if __name__ == "__main__":
    unittest.main()
