cmake_minimum_required(VERSION 3.4.0 FATAL_ERROR)
set(PROJECT_CONTACT romange@gmail.com)

project(pumadb CXX)



list(APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/gaia/cmake)
include(third_party)
include(internal)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(gaia)

add_subdirectory(gaia)
