import argparse
import sys
from concurrent import futures

import grpc

from cloudquery.plugin_v3 import plugin_pb2_grpc
from cloudquery.sdk.internal.servers.plugin_v3 import PluginServicer


def run_serve(args):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    plugin_pb2_grpc.add_PluginServicer_to_server(
        PluginServicer(), server)
    server.add_insecure_port('[::]:50051')
    print("Starting server. Listening on port 50051")
    server.start()
    server.wait_for_termination()
    pass


def serve():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(required=True)
    serve_parser = subparsers.add_subparsers("serve", help="Start plugin server")
    serve_parser.add_argument("--log-level", type=str, default="info",
                              choices=["trace", "debug", "info", "warn", "error"], help="log level")
    serve_parser.add_argument("--log-format", type=str, default="text", choices=["text", "json"])
    serve_parser.add_argument("--address", type=str, default="localhost:7777",
                              help="address to serve on. can be tcp: 'localhost:7777' or unix socket: '/tmp/plugin.rpc.sock'")
    serve_parser.add_argument("--network", type=str, default="tcp", choices=["tcp", "unix"],
                              help="network to serve on. can be tcp or unix")
    parser.parse_args(sys.argv[1:])
