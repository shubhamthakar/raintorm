# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# NO CHECKED-IN PROTOBUF GENCODE
# source: rainstorm.proto
# Protobuf Python Version: 5.28.1
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(
    _runtime_version.Domain.PUBLIC,
    5,
    28,
    1,
    '',
    'rainstorm.proto'
)
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x0frainstorm.proto\"{\n\nJobRequest\x12\x0f\n\x07op_exes\x18\x01 \x03(\x0c\x12\x14\n\x0cop_exe_names\x18\x02 \x03(\t\x12\x16\n\x0ehydfs_src_file\x18\x03 \x01(\t\x12\x1b\n\x13hydfs_dest_filename\x18\x04 \x01(\t\x12\x11\n\tnum_tasks\x18\x05 \x01(\x05\"\x1e\n\x0bJobResponse\x12\x0f\n\x07message\x18\x01 \x01(\t23\n\tRainStorm\x12&\n\tSubmitJob\x12\x0b.JobRequest\x1a\x0c.JobResponseb\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'rainstorm_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
  DESCRIPTOR._loaded_options = None
  _globals['_JOBREQUEST']._serialized_start=19
  _globals['_JOBREQUEST']._serialized_end=142
  _globals['_JOBRESPONSE']._serialized_start=144
  _globals['_JOBRESPONSE']._serialized_end=174
  _globals['_RAINSTORM']._serialized_start=176
  _globals['_RAINSTORM']._serialized_end=227
# @@protoc_insertion_point(module_scope)
