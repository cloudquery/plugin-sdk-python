from typing import List

from cloudquery.discovery_v1 import discovery_pb2, discovery_pb2_grpc


class DiscoveryServicer(discovery_pb2_grpc.DiscoveryServicer):
    def __init__(self, versions: List[int]):
        self._versions = versions

    def GetVersions(self, request, context):
        return discovery_pb2.GetVersions.Response(versions=self._versions)
