import argparse
import hashlib
import json
import logging
import os
import shutil
import subprocess
import tarfile
from concurrent import futures
from pathlib import Path

import grpc
import structlog
import sys
from cloudquery.discovery_v1 import discovery_pb2_grpc
from cloudquery.plugin_v3 import plugin_pb2_grpc
from structlog import wrap_logger

from cloudquery.sdk.internal.servers.discovery_v1.discovery import DiscoveryServicer
from cloudquery.sdk.internal.servers.plugin_v3 import PluginServicer
from cloudquery.sdk.plugin.plugin import Plugin

_IS_WINDOWS = sys.platform == "win32"

try:
    import colorama
except ImportError:
    colorama = None

if _IS_WINDOWS:  # pragma: no cover
    # On Windows, use colors by default only if Colorama is installed.
    _has_colors = colorama is not None
else:
    # On other OSes, use colors by default.
    _has_colors = True


def get_logger(args):
    log_level_map = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warning": logging.WARNING,
        "error": logging.ERROR,
        "critical": logging.CRITICAL,
    }

    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=log_level_map.get(args.log_level.lower(), logging.INFO),
    )

    processors = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        structlog.stdlib.filter_by_level,
        structlog.processors.TimeStamper(fmt="%Y-%m-%dT%H:%M:%SZ", utc=True),
    ]
    if args.log_format == "text":
        processors.append(
            structlog.dev.ConsoleRenderer(
                colors=os.environ.get("NO_COLOR", "") == ""
                and (
                    os.environ.get("FORCE_COLOR", "") != ""
                    or (
                        _has_colors
                        and sys.stdout is not None
                        and hasattr(sys.stdout, "isatty")
                        and sys.stdout.isatty()
                    )
                )
            )
        )
    else:
        processors.append(structlog.processors.JSONRenderer())

    log = wrap_logger(logging.getLogger(), processors=processors)
    return log


def calc_sha256_checksum(filename: str):
    with open(filename, "rb") as f:
        file_hash = hashlib.sha256()
        while chunk := f.read(32768):
            file_hash.update(chunk)
        return file_hash.hexdigest()


class PluginCommand:
    def __init__(self, plugin: Plugin):
        self._plugin = plugin

    def run(self, args):
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers(dest="command", required=True)

        self._register_serve_command(subparsers)
        self._register_package_command(subparsers)

        parsed_args = parser.parse_args(args)

        if parsed_args.command == "serve":
            self._serve(parsed_args)
        elif parsed_args.command == "package":
            self._package(parsed_args)
        else:
            parser.print_help()
            sys.exit(1)

    def _register_serve_command(self, subparsers):
        serve_parser = subparsers.add_parser("serve", help="Start plugin server")
        serve_parser.add_argument(
            "--log-format",
            type=str,
            default="text",
            choices=["text", "json"],
            help="logging format",
        )
        serve_parser.add_argument(
            "--log-level",
            type=str,
            default="info",
            choices=["trace", "debug", "info", "warn", "error"],
            help="log level",
        )
        # ignored for now
        serve_parser.add_argument(
            "--no-sentry",
            action="store_true",
            help="disable sentry (placeholder for future use)",
        )
        # ignored for now
        serve_parser.add_argument(
            "--otel-endpoint",
            type=str,
            default="",
            help="Open Telemetry HTTP collector endpoint (placeholder for future use)",
        )
        # ignored for now
        serve_parser.add_argument(
            "--otel-endpoint-insecure",
            type=str,
            default="",
            help="Open Telemetry HTTP collector endpoint (for development only) (placeholder for future use)",
        )
        serve_parser.add_argument(
            "--address",
            type=str,
            default="localhost:7777",
            help="address to serve on. can be tcp: 'localhost:7777' or unix socket: '/tmp/plugin.rpc.sock'",
        )
        serve_parser.add_argument(
            "--network",
            type=str,
            default="tcp",
            choices=["tcp", "unix"],
            help="network to serve on. can be tcp or unix",
        )

    def _register_package_command(self, subparsers):
        package_parser = subparsers.add_parser("package", help="Package plugin for publishing")
        package_parser.add_argument(
            "--log-format",
            type=str,
            default="text",
            choices=["text", "json"],
            help="logging format",
        )
        package_parser.add_argument(
            "--log-level",
            type=str,
            default="info",
            choices=["trace", "debug", "info", "warn", "error"],
            help="log level",
        )
        package_parser.add_argument(
            "--dist-dir",
            type=str,
            default="dist",
        )
        package_parser.add_argument(
            "--message",
            type=str,
            default="",
        )

    def _package(self, args):
        def run_docker_cmd(cmd):
            result = subprocess.run(cmd,capture_output=True)
            if result.returncode != 0:
                err = "" if result.stderr is None else result.stderr.decode('ascii').strip()
                raise ChildProcessError("Unable to run Docker command: %s" % err)

        tmp_dist_dir = "%s/plugin" % args.dist_dir
        Path(tmp_dist_dir).mkdir(0o755, exist_ok=True, parents=True)

        package_json = self._make_package_json(args.message)

        for i, dockerfile in enumerate(self._plugin.options().dockerfiles):
            image_name = "cq-docker-image-%d" % i
            image_path = "%s/%s.tar" % (tmp_dist_dir, image_name)

            print("Building Docker image for Dockerfile '%s'" % dockerfile.path)
            run_docker_cmd(["docker", "build", "--tag", image_name, "--file", dockerfile.path, "."])

            print("Saving Docker image for Dockerfile '%s'" % dockerfile.path)
            run_docker_cmd(["docker", "save", "--output", image_path, image_name])

            for target in dockerfile.build_targets:
                package_json["supported_targets"].append({
                    "path": "%s.tar" % image_name,
                    "os": target.os,
                    "arch": target.arch,
                    "checksum": calc_sha256_checksum(image_path),
                })

        with open("%s/package.json" % tmp_dist_dir, "w") as f:
            package_json = json.dumps(package_json, indent=4)
            f.write(package_json)

        shutil.copyfile("docs/overview.md", "%s/overview.md" % tmp_dist_dir)

        print("Creating tar.gz archive")
        with tarfile.open("%s/plugin.tar.gz" % args.dist_dir, "w:gz") as tar:
            tar.add(tmp_dist_dir, arcname=os.path.basename(tmp_dist_dir))

        shutil.rmtree(tmp_dist_dir)
        print("done")

    def _make_package_json(self, message):
        return {
            "schema_version": 1,
            "name": self._plugin.name(),
            "team": self._plugin.options().team_name,
            "kind": self._plugin.options().plugin_kind,
            "version": self._plugin.version(),
            "message": message,
            "protocols": [3],
            "supported_targets": [],
            "package_type": "docker",
        }

    def _serve(self, args):
        logger = get_logger(args)
        self._plugin.set_logger(logger)
        self._server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        discovery_pb2_grpc.add_DiscoveryServicer_to_server(
            DiscoveryServicer([3]), self._server
        )
        plugin_pb2_grpc.add_PluginServicer_to_server(
            PluginServicer(self._plugin, logger), self._server
        )
        self._server.add_insecure_port(args.address)
        logger.info("Starting server", address=args.address)
        self._server.start()
        self._server.wait_for_termination()

    def stop(self):
        self._server.stop(5)
