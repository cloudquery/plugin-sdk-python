import argparse
from concurrent import futures

import grpc
import sys

from cloudquery.plugin_v3 import plugin_pb2_grpc
from cloudquery.sdk.internal.servers.plugin_v3 import PluginServicer
from cloudquery.sdk.plugin.plugin import Plugin
from cloudquery.sdk.serve.docs import DOC_FORMATS


class PluginCommand:
    def __init__(self, plugin: Plugin):
        self._plugin = plugin

    def run(self, args):
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers(dest='command', required=True)

        serve_parser = subparsers.add_parser("serve", help="Start plugin server")
        serve_parser.add_argument("--log-level", type=str, default="info",
                                  choices=["trace", "debug", "info", "warn", "error"], help="log level")
        serve_parser.add_argument("--log-format", type=str, default="text", choices=["text", "json"])
        serve_parser.add_argument("--address", type=str, default="localhost:7777",
                                  help="address to serve on. can be tcp: 'localhost:7777' or unix socket: '/tmp/plugin.rpc.sock'")
        serve_parser.add_argument("--network", type=str, default="tcp", choices=["tcp", "unix"],
                                  help="network to serve on. can be tcp or unix")

        doc_parser = subparsers.add_parser("doc", help="Generate plugin documentation")
        doc_parser.add_argument("directory", type=str)
        doc_parser.add_argument("--format", type=str, default="json",
                                help="output format. one of: {}".format(",".join(DOC_FORMATS)))
        parsed_args = parser.parse_args(args)

        if parsed_args.command == "serve":
            self._serve(parsed_args)
        elif parsed_args.command == "doc":
            self._generate_docs(parsed_args)
        else:
            parser.print_help()
            sys.exit(1)

    def _serve(self, args):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        plugin_pb2_grpc.add_PluginServicer_to_server(
            PluginServicer(self._plugin), server)
        server.add_insecure_port(args.address)
        print("Starting server. Listening on " + args.address)
        server.start()
        server.wait_for_termination()

    def _generate_docs(self, args):
        print("Generating docs in format: " + args.format)

        raise NotImplementedError()
