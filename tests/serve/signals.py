import signal
import socket
import subprocess
import sys
import textwrap
import time

import grpc
import pytest

from cloudquery.plugin_v3 import plugin_pb2, plugin_pb2_grpc

SERVE_PLUGIN = """
from cloudquery.sdk import serve
from cloudquery.sdk.plugin import Plugin

serve.PluginCommand(Plugin("test", "v0.0.1")).run(["serve", "--address", "127.0.0.1:0"])
"""

SERVE_PLUGIN_WITH_BLOCKED_INIT = """
import sys, threading
from cloudquery.sdk import serve
from cloudquery.sdk.plugin import Plugin

class BlockedInitPlugin(Plugin):
    def init(self, spec, no_connection=False):
        threading.Event().wait()

serve.PluginCommand(BlockedInitPlugin("test", "v0.0.1")).run(["serve", "--address", sys.argv[1]])
"""

SERVE_PLUGIN_WITH_INIT_FLOODING_STDOUT = """
import sys
from cloudquery.sdk import serve
from cloudquery.sdk.plugin import Plugin

class FloodingInitPlugin(Plugin):
    def init(self, spec, no_connection=False):
        while True:
            sys.stdout.write("x" * 1024)

serve.PluginCommand(FloodingInitPlugin("test", "v0.0.1")).run(["serve", "--address", sys.argv[1]])
"""

SERVE_PLUGIN_WITH_INIT_BLOCKED_ON_BUFFERED_STDOUT = """
import sys, threading
from cloudquery.sdk import serve
from cloudquery.sdk.plugin import Plugin

class BufferedInitPlugin(Plugin):
    def init(self, spec, no_connection=False):
        sys.stdout.write("buffered and never flushed")
        threading.Event().wait()

serve.PluginCommand(BufferedInitPlugin("test", "v0.0.1")).run(["serve", "--address", sys.argv[1]])
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


def start_plugin(tmp_path, source, *args):
    script = tmp_path / "plugin_main.py"
    script.write_text(textwrap.dedent(source))
    return subprocess.Popen(
        [sys.executable, str(script), *args],
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


def free_address():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return "127.0.0.1:%d" % sock.getsockname()[1]


def start_plugin_with_blocked_init(tmp_path, source=SERVE_PLUGIN_WITH_BLOCKED_INIT):
    address = free_address()
    process = start_plugin(tmp_path, source, address)
    wait_until_serving(process)
    channel = grpc.insecure_channel(address)
    init_call = plugin_pb2_grpc.PluginStub(channel).Init.future(
        plugin_pb2.Init.Request(spec=b"")
    )
    time.sleep(0.5)
    return process, channel, init_call


def test_sigterm_with_blocked_handler_exits_zero_within_bound(tmp_path):
    process, channel, init_call = start_plugin_with_blocked_init(tmp_path)
    try:
        process.send_signal(signal.SIGTERM)
        output, _ = process.communicate(timeout=20)
        assert process.returncode == 0, output
    finally:
        init_call.cancel()
        channel.close()
        process.kill()


def test_second_signal_with_blocked_handler_exits_zero_immediately(tmp_path):
    process, channel, init_call = start_plugin_with_blocked_init(tmp_path)
    try:
        process.send_signal(signal.SIGTERM)
        time.sleep(0.5)
        process.send_signal(signal.SIGINT)
        output, _ = process.communicate(timeout=3)
        assert process.returncode == 0, output
    finally:
        init_call.cancel()
        channel.close()
        process.kill()


def send_termination_signals(process, second_signal):
    process.send_signal(signal.SIGTERM)
    if second_signal:
        time.sleep(0.5)
        process.send_signal(signal.SIGINT)
        return 3
    return 20


@pytest.mark.parametrize("second_signal", [False, True])
def test_forced_exit_with_full_stdout_pipe_exits_zero(tmp_path, second_signal):
    process, channel, init_call = start_plugin_with_blocked_init(
        tmp_path, SERVE_PLUGIN_WITH_INIT_FLOODING_STDOUT
    )
    try:
        timeout = send_termination_signals(process, second_signal)
        assert process.wait(timeout=timeout) == 0
    finally:
        init_call.cancel()
        channel.close()
        process.kill()


@pytest.mark.parametrize("second_signal", [False, True])
def test_forced_exit_with_closed_stdout_pipe_exits_zero(tmp_path, second_signal):
    process, channel, init_call = start_plugin_with_blocked_init(
        tmp_path, SERVE_PLUGIN_WITH_INIT_BLOCKED_ON_BUFFERED_STDOUT
    )
    try:
        process.stdout.close()
        timeout = send_termination_signals(process, second_signal)
        assert process.wait(timeout=timeout) == 0
    finally:
        init_call.cancel()
        channel.close()
        process.kill()
