# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc
import warnings

import rainstorm_pb2 as rainstorm__pb2

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
        + f' but the generated code in rainstorm_pb2_grpc.py depends on'
        + f' grpcio>={GRPC_GENERATED_VERSION}.'
        + f' Please upgrade your grpc module to grpcio>={GRPC_GENERATED_VERSION}'
        + f' or downgrade your generated code using grpcio-tools<={GRPC_VERSION}.'
    )


class RainStormStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.SubmitJob = channel.unary_unary(
                '/RainStorm/SubmitJob',
                request_serializer=rainstorm__pb2.JobRequest.SerializeToString,
                response_deserializer=rainstorm__pb2.JobResponse.FromString,
                _registered_method=True)
        self.PrintJson = channel.unary_unary(
                '/RainStorm/PrintJson',
                request_serializer=rainstorm__pb2.JsonRequest.SerializeToString,
                response_deserializer=rainstorm__pb2.AckResponse.FromString,
                _registered_method=True)


class RainStormServicer(object):
    """Missing associated documentation comment in .proto file."""

    def SubmitJob(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def PrintJson(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_RainStormServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'SubmitJob': grpc.unary_unary_rpc_method_handler(
                    servicer.SubmitJob,
                    request_deserializer=rainstorm__pb2.JobRequest.FromString,
                    response_serializer=rainstorm__pb2.JobResponse.SerializeToString,
            ),
            'PrintJson': grpc.unary_unary_rpc_method_handler(
                    servicer.PrintJson,
                    request_deserializer=rainstorm__pb2.JsonRequest.FromString,
                    response_serializer=rainstorm__pb2.AckResponse.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'RainStorm', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))
    server.add_registered_method_handlers('RainStorm', rpc_method_handlers)


 # This class is part of an EXPERIMENTAL API.
class RainStorm(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def SubmitJob(request,
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
            '/RainStorm/SubmitJob',
            rainstorm__pb2.JobRequest.SerializeToString,
            rainstorm__pb2.JobResponse.FromString,
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
    def PrintJson(request,
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
            '/RainStorm/PrintJson',
            rainstorm__pb2.JsonRequest.SerializeToString,
            rainstorm__pb2.AckResponse.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True)