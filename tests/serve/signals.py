import signal
import subprocess
import sys
import textwrap
import time

import pytest

SERVE_PLUGIN = """
from cloudquery.sdk import serve
from cloudquery.sdk.plugin import Plugin

serve.PluginCommand(Plugin("test", "v0.0.1")).run(["serve", "--address", "127.0.0.1:0"])
"""

SIGTERM_DURING_LOCK_RESTORE = """
import os, signal, sys, threading
from cloudquery.sdk import serve
from cloudquery.sdk.plugin import Plugin

def signal_handler(sig, frame):
    sys.exit(0)

signal.signal(signal.SIGTERM, signal_handler)

_orig = threading.Condition._acquire_restore
_fired = threading.Event()

def _acquire_restore(self, x):
    if threading.current_thread() is threading.main_thread() and not _fired.is_set():
        _fired.set()
        os.kill(os.getpid(), signal.SIGTERM)
    return _orig(self, x)

threading.Timer(1.0, lambda: setattr(threading.Condition, "_acquire_restore", _acquire_restore)).start()

serve.PluginCommand(Plugin("test", "v0.0.1")).run(["serve", "--address", "127.0.0.1:0"])
"""


def start_plugin(tmp_path, source):
    script = tmp_path / "plugin_main.py"
    script.write_text(textwrap.dedent(source))
    return subprocess.Popen(
        [sys.executable, str(script)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )


def wait_until_serving(process):
    for line in process.stdout:
        if "Starting server" in line:
            time.sleep(0.5)
            return
    pytest.fail("plugin exited before it started serving")


def test_sigterm_during_lock_restore_exits_zero(tmp_path):
    process = start_plugin(tmp_path, SIGTERM_DURING_LOCK_RESTORE)
    output, _ = process.communicate(timeout=30)
    assert process.returncode == 0, output


@pytest.mark.parametrize("signum", [signal.SIGTERM, signal.SIGINT])
def test_signal_while_serving_exits_zero(tmp_path, signum):
    process = start_plugin(tmp_path, SERVE_PLUGIN)
    wait_until_serving(process)
    process.send_signal(signum)
    output, _ = process.communicate(timeout=30)
    assert process.returncode == 0, output
