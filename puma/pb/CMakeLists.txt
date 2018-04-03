add_library(puma_pb_util fd_path.cc util.cc int_compressor.cc)
cxx_link(puma_pb_util puma_base coding storage_proto TRDP::protobuf TRDP::zstd)

add_library(pb_slicer field_slicer.cc field_writers.cc message_slicer.cc)
cxx_link(pb_slicer base file status strings proto_writer TRDP::pmr puma_pb_util set_encoder_lib)

cxx_test(int_compressor_test puma_pb_util LABELS CI)

add_executable(lst2col lst2col.cc)
cxx_link(lst2col file pprint_utils proto_writer pb_slicer puma_write_file
         puma_pb_util task_pool fast_malloc)
