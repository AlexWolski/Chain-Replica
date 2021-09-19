# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: chain_debug.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


import chain_pb2 as chain__pb2


DESCRIPTOR = _descriptor.FileDescriptor(
  name='chain_debug.proto',
  package='chain',
  syntax='proto3',
  serialized_options=b'\n\024edu.sjsu.cs249.chainP\001',
  create_key=_descriptor._internal_create_key,
  serialized_pb=b'\n\x11\x63hain_debug.proto\x12\x05\x63hain\x1a\x0b\x63hain.proto\"\x13\n\x11\x43hainDebugRequest\"\xb6\x01\n\x12\x43hainDebugResponse\x12\x33\n\x05state\x18\x01 \x03(\x0b\x32$.chain.ChainDebugResponse.StateEntry\x12\x0b\n\x03xid\x18\x02 \x01(\r\x12\"\n\x04sent\x18\x03 \x03(\x0b\x32\x14.chain.UpdateRequest\x12\x0c\n\x04logs\x18\x04 \x03(\t\x1a,\n\nStateEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\r:\x02\x38\x01\x32J\n\nChainDebug\x12<\n\x05\x64\x65\x62ug\x12\x18.chain.ChainDebugRequest\x1a\x19.chain.ChainDebugResponseB\x18\n\x14\x65\x64u.sjsu.cs249.chainP\x01\x62\x06proto3'
  ,
  dependencies=[chain__pb2.DESCRIPTOR,])




_CHAINDEBUGREQUEST = _descriptor.Descriptor(
  name='ChainDebugRequest',
  full_name='chain.ChainDebugRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=41,
  serialized_end=60,
)


_CHAINDEBUGRESPONSE_STATEENTRY = _descriptor.Descriptor(
  name='StateEntry',
  full_name='chain.ChainDebugResponse.StateEntry',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='key', full_name='chain.ChainDebugResponse.StateEntry.key', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='value', full_name='chain.ChainDebugResponse.StateEntry.value', index=1,
      number=2, type=13, cpp_type=3, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=b'8\001',
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=201,
  serialized_end=245,
)

_CHAINDEBUGRESPONSE = _descriptor.Descriptor(
  name='ChainDebugResponse',
  full_name='chain.ChainDebugResponse',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='state', full_name='chain.ChainDebugResponse.state', index=0,
      number=1, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='xid', full_name='chain.ChainDebugResponse.xid', index=1,
      number=2, type=13, cpp_type=3, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='sent', full_name='chain.ChainDebugResponse.sent', index=2,
      number=3, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='logs', full_name='chain.ChainDebugResponse.logs', index=3,
      number=4, type=9, cpp_type=9, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[_CHAINDEBUGRESPONSE_STATEENTRY, ],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=63,
  serialized_end=245,
)

_CHAINDEBUGRESPONSE_STATEENTRY.containing_type = _CHAINDEBUGRESPONSE
_CHAINDEBUGRESPONSE.fields_by_name['state'].message_type = _CHAINDEBUGRESPONSE_STATEENTRY
_CHAINDEBUGRESPONSE.fields_by_name['sent'].message_type = chain__pb2._UPDATEREQUEST
DESCRIPTOR.message_types_by_name['ChainDebugRequest'] = _CHAINDEBUGREQUEST
DESCRIPTOR.message_types_by_name['ChainDebugResponse'] = _CHAINDEBUGRESPONSE
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

ChainDebugRequest = _reflection.GeneratedProtocolMessageType('ChainDebugRequest', (_message.Message,), {
  'DESCRIPTOR' : _CHAINDEBUGREQUEST,
  '__module__' : 'chain_debug_pb2'
  # @@protoc_insertion_point(class_scope:chain.ChainDebugRequest)
  })
_sym_db.RegisterMessage(ChainDebugRequest)

ChainDebugResponse = _reflection.GeneratedProtocolMessageType('ChainDebugResponse', (_message.Message,), {

  'StateEntry' : _reflection.GeneratedProtocolMessageType('StateEntry', (_message.Message,), {
    'DESCRIPTOR' : _CHAINDEBUGRESPONSE_STATEENTRY,
    '__module__' : 'chain_debug_pb2'
    # @@protoc_insertion_point(class_scope:chain.ChainDebugResponse.StateEntry)
    })
  ,
  'DESCRIPTOR' : _CHAINDEBUGRESPONSE,
  '__module__' : 'chain_debug_pb2'
  # @@protoc_insertion_point(class_scope:chain.ChainDebugResponse)
  })
_sym_db.RegisterMessage(ChainDebugResponse)
_sym_db.RegisterMessage(ChainDebugResponse.StateEntry)


DESCRIPTOR._options = None
_CHAINDEBUGRESPONSE_STATEENTRY._options = None

_CHAINDEBUG = _descriptor.ServiceDescriptor(
  name='ChainDebug',
  full_name='chain.ChainDebug',
  file=DESCRIPTOR,
  index=0,
  serialized_options=None,
  create_key=_descriptor._internal_create_key,
  serialized_start=247,
  serialized_end=321,
  methods=[
  _descriptor.MethodDescriptor(
    name='debug',
    full_name='chain.ChainDebug.debug',
    index=0,
    containing_service=None,
    input_type=_CHAINDEBUGREQUEST,
    output_type=_CHAINDEBUGRESPONSE,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
])
_sym_db.RegisterServiceDescriptor(_CHAINDEBUG)

DESCRIPTOR.services_by_name['ChainDebug'] = _CHAINDEBUG

# @@protoc_insertion_point(module_scope)
