import argparse
import structlog
from concurrent import futures

import grpc
import sys
import os

from cloudquery.discovery_v1 import discovery_pb2_grpc
from cloudquery.plugin_v3 import plugin_pb2_grpc
from cloudquery.sdk.docs.generator import Generator
from cloudquery.sdk.internal.servers.discovery_v1.discovery import DiscoveryServicer
from cloudquery.sdk.internal.servers.plugin_v3 import PluginServicer
from cloudquery.sdk.plugin.plugin import Plugin

DOC_FORMATS = ["json", "markdown"]


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
    processors = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S", utc=False),
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

    # if args.log_format == "json":
    #     processors.append(structlog.processors.JSONRenderer())
    log = structlog.get_logger(processors=processors)
    return log


class PluginCommand:
    def __init__(self, plugin: Plugin):
        self._plugin = plugin

    def run(self, args):
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers(dest="command", required=True)

        serve_parser = subparsers.add_parser("serve", help="Start plugin server")
        serve_parser.add_argument(
            "--log-level",
            type=str,
            default="info",
            choices=["trace", "debug", "info", "warn", "error"],
            help="log level",
        )
        serve_parser.add_argument(
            "--log-format", type=str, default="text", choices=["text", "json"]
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

        doc_parser = subparsers.add_parser(
            "doc",
            formatter_class=argparse.RawTextHelpFormatter,
            help="Generate documentation for tables",
            description="""Generate documentation for tables.

If format is markdown, a destination directory will be created (if necessary) containing markdown files.
Example:
doc ./output 

If format is JSON, a destination directory will be created (if necessary) with a single json file called __tables.json.
Example:
doc --format json .
""",
        )
        doc_parser.add_argument("directory", type=str)
        doc_parser.add_argument(
            "--format",
            type=str,
            default="json",
            help="output format. one of: {}".format(",".join(DOC_FORMATS)),
        )
        parsed_args = parser.parse_args(args)

        if parsed_args.command == "serve":
            self._serve(parsed_args)
        elif parsed_args.command == "doc":
            self._generate_docs(parsed_args)
        else:
            parser.print_help()
            sys.exit(1)

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
        print("Starting server. Listening on " + args.address)
        self._server.start()
        self._server.wait_for_termination()

    def stop(self):
        self._server.stop(5)

    def _generate_docs(self, args):
        print("Generating docs in format: " + args.format)
        generator = Generator(
            self._plugin.name(), self._plugin.get_tables(tables=["*"], skip_tables=[])
        )
        generator.generate(args.directory, args.format)
