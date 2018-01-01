cxx_proto_lib(puma_types)
cxx_proto_lib(storage DEPENDS puma_types_proto)


jinja_lib(puma ast_op.cc.jinja data_types.cc.jinja)
cxx_link(puma_jinja_lib puma_types_proto strings TRDP::pmr)

add_library(puma_base schema.cc data_types.cc ast_op.cc variant_array.cc nullable_column.cc)
cxx_link(puma_base TRDP::pmr file strings util puma_jinja_lib)


dynasm_lib(ops_builder)
cxx_link(ops_builder_dasm puma_base)

add_library(puma_test_utils test_utils.cc)
cxx_link(puma_test_utils puma_base)
add_include(puma_test_utils ${GTEST_INCLUDE_DIR})


cxx_test(schema_test puma_base LABELS CI)

add_executable(import_volume import_volume.cc)
cxx_link(import_volume file puma_write_file pb_slicer task_pool storage_proto
         fast_malloc stdc++fs)

add_subdirectory(pb)
add_subdirectory(file)
