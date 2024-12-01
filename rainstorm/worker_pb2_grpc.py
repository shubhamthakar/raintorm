# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc
import warnings

import worker_pb2 as worker__pb2

GRPC_GENERATED_VERSION = '1.68.0'
GRPC_VERSION = grpc.__version__
_version_not_supported = False

try:
    from grpc._utilities import first_version_is_lower
    _version_not_supported = first_version_is_lower(GRPC_VERSION, GRPC_GENERATED_VERSION)
except ImportError:
    _version_not_supported = True

if _version_not_supported:
    raise RuntimeError(
        f'The grpc package installed is at version {GRPC_VERSION},'
        + f' but the generated code in worker_pb2_grpc.py depends on'
        + f' grpcio>={GRPC_GENERATED_VERSION}.'
        + f' Please upgrade your grpc module to grpcio>={GRPC_GENERATED_VERSION}'
        + f' or downgrade your generated code using grpcio-tools<={GRPC_VERSION}.'
    )


class WorkerStub(object):
    """The service definition for the Worker.
    """

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.RecvData = channel.unary_unary(
                '/worker.Worker/RecvData',
                request_serializer=worker__pb2.DataRequest.SerializeToString,
                response_deserializer=worker__pb2.AckResponse.FromString,
                _registered_method=True)
        self.UpdateMapping = channel.unary_unary(
                '/worker.Worker/UpdateMapping',
                request_serializer=worker__pb2.MappingUpdateRequest.SerializeToString,
                response_deserializer=worker__pb2.UpdateResponse.FromString,
                _registered_method=True)


class WorkerServicer(object):
    """The service definition for the Worker.
    """

    def RecvData(self, request, context):
        """Receives data and responds with an acknowledgment.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def UpdateMapping(self, request, context):
        """Updates the mapping and responds with a status.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_WorkerServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'RecvData': grpc.unary_unary_rpc_method_handler(
                    servicer.RecvData,
                    request_deserializer=worker__pb2.DataRequest.FromString,
                    response_serializer=worker__pb2.AckResponse.SerializeToString,
            ),
            'UpdateMapping': grpc.unary_unary_rpc_method_handler(
                    servicer.UpdateMapping,
                    request_deserializer=worker__pb2.MappingUpdateRequest.FromString,
                    response_serializer=worker__pb2.UpdateResponse.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'worker.Worker', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))
    server.add_registered_method_handlers('worker.Worker', rpc_method_handlers)


 # This class is part of an EXPERIMENTAL API.
class Worker(object):
    """The service definition for the Worker.
    """

    @staticmethod
    def RecvData(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(
            request,
            target,
            '/worker.Worker/RecvData',
            worker__pb2.DataRequest.SerializeToString,
            worker__pb2.AckResponse.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True)

    @staticmethod
    def UpdateMapping(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(
            request,
            target,
            '/worker.Worker/UpdateMapping',
            worker__pb2.MappingUpdateRequest.SerializeToString,
            worker__pb2.UpdateResponse.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True)
