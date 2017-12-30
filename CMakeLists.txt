cmake_minimum_required(VERSION 3.4.0 FATAL_ERROR)
set(PROJECT_CONTACT romange@gmail.com)

project(pumadb CXX)



list(APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/gaia/cmake)
include(third_party)
include(internal)

include_directories(gaia ${ROOT_GEN_DIR}/gaia) 

add_subdirectory(gaia)
add_subdirectory(puma)
