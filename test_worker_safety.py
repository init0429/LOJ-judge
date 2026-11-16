"""Isolated worker regressions without connecting to external services."""
import importlib.util
import json
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
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
        result = worker._run_container({"image": "python", "mem_limit": "128m"}, "run", 1)
        self.assertEqual(result, (0, "3", False))
        container.logs.assert_not_called()
        worker.docker.types.LogConfig.assert_called_once_with(type="none")
        options = worker.client.containers.create.call_args.kwargs
        self.assertTrue(options["read_only"])
        self.assertIn("size=64m", options["tmpfs"]["/tmp"])
        self.assertIn("exec", options["tmpfs"]["/tmp"].split(","))
        container.remove.assert_called_once_with(force=True)

    def test_output_overflow_never_returns_success(self):
        for chunks in ([(b"3", None), (b"x" * 1048576, None)],
                       [(b"3", b"x" * 1048576)]):
            worker = load_worker()
            container, _ = sandbox(worker, chunks)
            result = worker._run_container({"image": "python", "mem_limit": "128m"}, "run", 1)
            self.assertEqual(result, (1, "Output limit exceeded", False))
            container.kill.assert_called_once()
            container.remove.assert_called_once_with(force=True)

    def test_wrapped_timeout_and_cleanup_failure(self):
        worker = load_worker()
        container, stream = sandbox(worker, [], wait=ConnectionError(TimeoutError("read timed out")))
        stream.close.side_effect = OSError("closed")
        with self.assertLogs(level="ERROR"):
            result = worker._run_container({"image": "python", "mem_limit": "128m"}, "run", 1)
        self.assertEqual(result, (None, "", True))
        container.remove.assert_called_once_with(force=True)


if __name__ == "__main__":
    unittest.main()
