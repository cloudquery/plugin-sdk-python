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
from cloudquery.sdk import plugin


from cloudquery.sdk.internal.servers.discovery_v1.discovery import DiscoveryServicer
from cloudquery.sdk.internal.servers.plugin_v3 import PluginServicer
from cloudquery.sdk.plugin.plugin import Plugin
from cloudquery.sdk.schema import table

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
        # ignored for now
        serve_parser.add_argument(
            "--license",
            type=str,
            default="",
            help="set offline license file (placeholder for future use)",
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
        package_parser = subparsers.add_parser(
            "package", help="Package the plugin as a Docker image"
        )
        package_parser.add_argument(
            "version", help="version to tag the Docker image with"
        )
        package_parser.add_argument("plugin-directory")
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
            "-D",
            "--dist-dir",
            type=str,
            help="dist directory to output the built plugin. (default: <plugin_directory>/dist)",
        )
        package_parser.add_argument(
            "--docs-dir",
            type=str,
            help="docs directory containing markdown files to copy to the dist directory. (default: <plugin_directory>/docs)",
        )
        package_parser.add_argument(
            "-m",
            "--message",
            type=str,
            required=True,
            help="message that summarizes what is new or changed in this version. Use @<file> to read from file. Supports markdown.",
        )

    def _package(self, args):
        logger = get_logger(args)
        self._plugin.set_logger(logger)

        def _is_empty(val):
            return val == None or len(val) == 0

        if _is_empty(self._plugin.name()):
            raise Exception("plugin name is required")
        if _is_empty(self._plugin.team()):
            raise Exception("plugin team is required")
        if _is_empty(self._plugin.kind()):
            raise Exception("plugin kind is required")
        if _is_empty(self._plugin.dockerfile()):
            raise Exception("plugin dockerfile is required")
        if _is_empty(self._plugin.build_targets()):
            raise Exception("at least one build target is required")

        plugin_directory, version, message = (
            getattr(args, "plugin-directory"),
            getattr(args, "version"),
            getattr(args, "message"),
        )
        dist_dir = (
            "%s/dist" % plugin_directory if args.dist_dir == None else args.dist_dir
        )
        docs_dir = (
            "%s/docs" % plugin_directory if args.docs_dir == None else args.docs_dir
        )
        Path(dist_dir).mkdir(0o755, exist_ok=True, parents=True)

        self._copy_docs(logger, docs_dir, dist_dir)
        self._write_tables_json(logger, dist_dir)
        supported_targets = self._build_dockerfile(
            logger, plugin_directory, dist_dir, version
        )
        self._write_package_json(logger, dist_dir, message, version, supported_targets)
        logger.info("Done packaging plugin to '%s'" % dist_dir)

    def _write_package_json(self, logger, dist_dir, message, version, supportedTargets):
        package_json_path = "%s/package.json" % dist_dir
        logger.info("Writing package.json to '%s'" % package_json_path)
        content = {
            "schema_version": 1,
            "name": self._plugin.name(),
            "team": self._plugin.team(),
            "kind": self._plugin.kind(),
            "version": version,
            "message": message,
            "protocols": [3],
            "supported_targets": supportedTargets,
            "package_type": "docker",
        }
        with open("%s/package.json" % dist_dir, "w") as f:
            f.write(json.dumps(content, indent=2))

    def _copy_docs(self, logger, docs_dir, dist_dir):
        # check is docs_dir exists
        if not os.path.isdir(docs_dir):
            raise Exception("docs directory '%s' does not exist" % docs_dir)

        output_docs_dir = "%s/docs" % dist_dir
        logger.info("Copying docs from '%s' to '%s'" % (docs_dir, output_docs_dir))
        shutil.copytree(docs_dir, output_docs_dir, dirs_exist_ok=True)

    def _write_tables_json(self, logger, dist_dir):
        if self._plugin.kind() != "source":
            return

        tables_json_output_path = "%s/tables.json" % dist_dir
        logger.info("Writing tables to '%s'" % tables_json_output_path)
        self._plugin.init(spec=b"", no_connection=True)
        tables = self._plugin.get_tables(
            options=plugin.plugin.TableOptions(
                tables=["*"], skip_tables=[], skip_dependent_tables=False
            )
        )
        flattened_tables = table.flatten_tables(tables)

        def column_to_json(column: table.Column):
            return {
                "name": column.name,
                "type": str(column.type),
                "description": column.description,
                "incremental_key": column.incremental_key,
                "primary_key": column.primary_key,
                "not_null": column.not_null,
                "unique": column.unique,
            }

        def table_to_json(table: table.Table):
            return {
                "name": table.name,
                "title": table.title,
                "description": table.description,
                "is_incremental": table.is_incremental,
                "parent": table.parent.name if table.parent else "",
                "relations": list(map(lambda r: r.name, table.relations)),
                "columns": list(map(column_to_json, table.columns)),
            }

        tables_json = list(map(table_to_json, flattened_tables))
        with open(tables_json_output_path, "w") as f:
            f.write(json.dumps(tables_json))
        logger.info(
            "Wrote %d tables to '%s'" % (len(tables_json), tables_json_output_path)
        )

    def _build_dockerfile(self, logger, plugin_dir, dist_dir, version):
        dockerfile_path = "%s/%s" % (plugin_dir, self._plugin.dockerfile())
        if not os.path.isfile(dockerfile_path):
            raise Exception("Dockerfile '%s' does not exist" % dockerfile_path)

        def run_docker_cmd(cmd, plugin_dir):
            result = subprocess.run(cmd, capture_output=True, cwd=plugin_dir)
            if result.returncode != 0:
                err = (
                    ""
                    if result.stderr is None
                    else result.stderr.decode("ascii").strip()
                )
                raise ChildProcessError("Unable to run Docker command: %s" % err)

        def build_target(target: plugin.plugin.BuildTarget):
            image_repository = "docker.cloudquery.io/%s/%s-%s" % (
                self._plugin.team(),
                self._plugin.kind(),
                self._plugin.name(),
            )
            image_tag = "%s:%s-%s-%s" % (
                image_repository,
                version,
                target.os,
                target.arch,
            )
            image_tar = "plugin-%s-%s-%s-%s.tar" % (
                self._plugin.name(),
                version,
                target.os,
                target.arch,
            )
            image_path = "%s/%s" % (dist_dir, image_tar)
            logger.info("Building docker image %s" % image_tag)
            docker_build_arguments = [
                "docker",
                "buildx",
                "build",
                "-t",
                image_tag,
                "--platform",
                "%s/%s" % (target.os, target.arch),
                "-f",
                dockerfile_path,
                ".",
                "--progress",
                "plain",
                "--load",
            ]
            logger.debug(
                "Running command 'docker %s'" % " ".join(docker_build_arguments)
            )
            run_docker_cmd(docker_build_arguments, plugin_dir)
            logger.debug("Saving docker image '%s' to '%s'" % (image_tag, image_path))
            docker_save_arguments = ["docker", "save", "-o", image_path, image_tag]
            logger.debug("Running command 'docker %s'", " ".join(docker_save_arguments))
            run_docker_cmd(docker_save_arguments, plugin_dir)
            return {
                "os": target.os,
                "arch": target.arch,
                "path": image_tar,
                "checksum": calc_sha256_checksum(image_path),
                "docker_image_tag": image_tag,
            }

        logger.info("Building %d targets" % len(self._plugin.build_targets()))
        supported_targets = list(map(build_target, self._plugin.build_targets()))
        return supported_targets

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
