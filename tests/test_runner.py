import hashlib
import io
import os
import sys
import tempfile
import unittest
from contextlib import redirect_stdout

import runner


class RunnerTests(unittest.TestCase):
    def _run_runner(self, module_name, *module_argv, prepend_sys_path=None):
        old_path = list(sys.path)
        if prepend_sys_path:
            sys.path[:0] = list(prepend_sys_path)

        try:
            buf = io.StringIO()
            with redirect_stdout(buf):
                runner.main("runner", module_name, *module_argv)
            return buf.getvalue()
        finally:
            sys.path[:] = old_path

    def test_file_module_can_import_sibling_module(self):
        with tempfile.TemporaryDirectory() as tmp:
            with open(os.path.join(tmp, "helper.py"), "w", encoding="utf-8") as f:
                f.write("VALUE = 42\n")
            target = os.path.join(tmp, "job.py")
            with open(target, "w", encoding="utf-8") as f:
                f.write(
                    "import helper\n"
                    "def main(*argv):\n"
                    "    print(helper.VALUE)\n"
                )

            output = self._run_runner(target)
            self.assertIn("42", output)

    def test_slash_module_path_is_supported(self):
        with tempfile.TemporaryDirectory() as tmp:
            pkg = os.path.join(tmp, "pkg")
            os.makedirs(pkg)
            with open(os.path.join(pkg, "__init__.py"), "w", encoding="utf-8") as f:
                f.write("")
            with open(os.path.join(pkg, "m.py"), "w", encoding="utf-8") as f:
                f.write("def main(*argv):\n    print('ok')\n")

            output = self._run_runner("pkg/m", prepend_sys_path=[tmp])
            self.assertIn("ok", output)

    def test_file_module_does_not_poison_stdlib_module_name(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = os.path.join(tmp, "json.py")
            with open(target, "w", encoding="utf-8") as f:
                f.write("def main(*argv):\n    print('filejson')\n")

            self._run_runner(target)
            import json  # noqa: PLC0415

            self.assertTrue(hasattr(json, "dumps"))
            self.assertNotEqual(getattr(json, "__file__", ""), target)

    def test_parse_args_list_is_expanded(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = os.path.join(tmp, "job.py")
            with open(target, "w", encoding="utf-8") as f:
                f.write(
                    "def parse_args(argv):\n"
                    "    return ['A', 'B']\n"
                    "def main(*argv):\n"
                    "    print(argv)\n"
                )

            output = self._run_runner(target, "x")
            self.assertIn("('A', 'B')", output)

    def test_non_callable_parse_args_is_ignored(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = os.path.join(tmp, "job.py")
            with open(target, "w", encoding="utf-8") as f:
                f.write(
                    "parse_args = 123\n"
                    "def main(*argv):\n"
                    "    print(argv)\n"
                )

            output = self._run_runner(target, "x")
            self.assertIn("('x',)", output)

    def test_async_parse_args_is_awaited(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = os.path.join(tmp, "job.py")
            with open(target, "w", encoding="utf-8") as f:
                f.write(
                    "import asyncio\n"
                    "async def parse_args(argv):\n"
                    "    await asyncio.sleep(0)\n"
                    "    return ['A']\n"
                    "def main(*argv):\n"
                    "    print(argv)\n"
                )

            output = self._run_runner(target, "x")
            self.assertIn("('A',)", output)

    def test_failed_import_cleans_sys_modules(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = os.path.join(tmp, "bad.py")
            with open(target, "w", encoding="utf-8") as f:
                f.write("raise RuntimeError('boom')\n")

            module_id = (
                "_runner_file_"
                + hashlib.sha1(os.path.abspath(target).encode("utf-8")).hexdigest()
            )

            self.assertNotIn(module_id, sys.modules)
            with self.assertRaises(RuntimeError):
                self._run_runner(target)
            self.assertNotIn(module_id, sys.modules)


if __name__ == "__main__":
    unittest.main()
