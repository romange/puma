cur_gen_dir(gen_dir)

set(full_path1 ${gen_dir}/grammar.h ${gen_dir}/grammar.cc)
set(SCANFILE puma_scan)

ADD_CUSTOM_COMMAND(
     OUTPUT ${full_path1}
     COMMAND mkdir -p ${gen_dir}
     COMMAND bison --defines=${gen_dir}/grammar.h -o ${gen_dir}/grammar.cc --report-file=${gen_dir}/grammar.report
                   --report=all grammar.y
     DEPENDS grammar.y
     WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
     COMMENT "Generating lexer from grammar.y" VERBATIM)

ADD_CUSTOM_COMMAND(
     OUTPUT ${gen_dir}/${SCANFILE}.cc ${gen_dir}/${SCANFILE}.h
     COMMAND mkdir -p ${gen_dir}
     COMMAND flex -d --header-file=${gen_dir}/${SCANFILE}.h --outfile=${gen_dir}/${SCANFILE}.cc ${SCANFILE}.l
     DEPENDS ${SCANFILE}.l
     WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
     COMMENT "Generating lexer from scan.l" VERBATIM)

add_library(puma_lang ast_expr.cc parser_handler.cc scan_value.cc  attribute_index.cc
            ${gen_dir}/${SCANFILE}.cc ${gen_dir}/grammar.cc
            ${gen_dir}/grammar.h)

#Due to compiler warning in ${SCANFILE}.cc
add_compile_flag(puma_lang "-Wno-sign-compare")

cxx_link(puma_lang puma_base status strings TRDP::pmr)

cxx_test(parser_test puma_lang base LABELS CI)
cxx_test(attribute_index_test puma_lang base LABELS CI)
