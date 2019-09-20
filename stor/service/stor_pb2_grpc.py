# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
import grpc

from stor.service import stor_pb2 as stor__pb2


class RPCServerStub(object):
  # missing associated documentation comment in .proto file
  pass

  def __init__(self, channel):
    """Constructor.

    Args:
      channel: A grpc.Channel.
    """
    self.call = channel.unary_unary(
        '/RPCServer/call',
        request_serializer=stor__pb2.Request.SerializeToString,
        response_deserializer=stor__pb2.Response.FromString,
        )


class RPCServerServicer(object):
  # missing associated documentation comment in .proto file
  pass

  def call(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')


def add_RPCServerServicer_to_server(servicer, server):
  rpc_method_handlers = {
      'call': grpc.unary_unary_rpc_method_handler(
          servicer.call,
          request_deserializer=stor__pb2.Request.FromString,
          response_serializer=stor__pb2.Response.SerializeToString,
      ),
  }
  generic_handler = grpc.method_handlers_generic_handler(
      'RPCServer', rpc_method_handlers)
  server.add_generic_rpc_handlers((generic_handler,))
