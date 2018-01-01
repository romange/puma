add_library(puma_read_file bare_slice_provider.cc
            volume_reader.cc volume_provider.cc volume_manager.cc)
cxx_link(puma_read_file puma_base puma_pb_util set_encoder_lib storage_proto)

add_library(puma_write_file  volume_writer.cc)
cxx_link(puma_write_file puma_base storage_proto)

add_library(puma_slice_file slice_format.cc slice_writer.cc slice_provider.cc slice_writer_impl.cc)
cxx_link(puma_slice_file puma_write_file puma_read_file)

cxx_test(volume_test file puma_write_file puma_read_file pb_slicer puma_test_utils addressbook_proto LABELS CI)

add_executable(vol_print vol_print.cc)
cxx_link(vol_print puma_pb_util puma_read_file fast_malloc)

