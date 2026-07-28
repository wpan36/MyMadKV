cmake_minimum_required(VERSION 3.16)

project(madkv_project1 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(
    CMAKE_RUNTIME_OUTPUT_DIRECTORY
    "${CMAKE_BINARY_DIR}/bin"
)

find_package(Protobuf REQUIRED)
find_package(PkgConfig REQUIRED)

pkg_check_modules(
    GRPCPP
    REQUIRED
    IMPORTED_TARGET
    grpc++
)

find_program(
    PROTOC_EXECUTABLE
    protoc
    REQUIRED
)

find_program(
    GRPC_CPP_PLUGIN_EXECUTABLE
    grpc_cpp_plugin
    REQUIRED
)

set(
    GENERATED_DIR
    "${CMAKE_CURRENT_BINARY_DIR}/generated"
)

file(MAKE_DIRECTORY "${GENERATED_DIR}")

set(
    PROTO_FILE
    "${CMAKE_CURRENT_SOURCE_DIR}/in_memory_kvstore.proto"
)

set(
    PROTO_GENERATED_SOURCES
    "${GENERATED_DIR}/in_memory_kvstore.pb.cc"
    "${GENERATED_DIR}/in_memory_kvstore.grpc.pb.cc"
)

set(
    PROTO_GENERATED_HEADERS
    "${GENERATED_DIR}/in_memory_kvstore.pb.h"
    "${GENERATED_DIR}/in_memory_kvstore.grpc.pb.h"
)

add_custom_command(
    OUTPUT
        ${PROTO_GENERATED_SOURCES}
        ${PROTO_GENERATED_HEADERS}

    COMMAND
        "${PROTOC_EXECUTABLE}"

    ARGS
        "--proto_path=${CMAKE_CURRENT_SOURCE_DIR}"
        "--cpp_out=${GENERATED_DIR}"
        "--grpc_out=${GENERATED_DIR}"
        "--plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${PROTO_FILE}"

    DEPENDS
        "${PROTO_FILE}"

    COMMENT
        "Generating protobuf and gRPC C++ sources"

    VERBATIM
)

add_library(
    kv_proto
    ${PROTO_GENERATED_SOURCES}
    ${PROTO_GENERATED_HEADERS}
)

target_include_directories(
    kv_proto
    PUBLIC
        "${GENERATED_DIR}"
)

target_link_libraries(
    kv_proto
    PUBLIC
        PkgConfig::GRPCPP
        protobuf::libprotobuf
)

add_library(
    in_memory_store
    in_memory_kvstore.cc
)

target_include_directories(
    in_memory_store
    PUBLIC
        "${CMAKE_CURRENT_SOURCE_DIR}"
        "${CMAKE_CURRENT_SOURCE_DIR}/../include"
)

target_compile_options(
    in_memory_store
    PRIVATE
        -Wall
        -Wextra
        -Wpedantic
)

add_executable(
    kv_server
    server.cc
)

target_include_directories(
    kv_server
    PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}"
        "${CMAKE_CURRENT_SOURCE_DIR}/../include"
        "${GENERATED_DIR}"
)

target_link_libraries(
    kv_server
    PRIVATE
        in_memory_store
        kv_proto
)

target_compile_options(
    kv_server
    PRIVATE
        -Wall
        -Wextra
        -Wpedantic
)

add_executable(
    kv_client
    client.cc
)

target_include_directories(
    kv_client
    PRIVATE
        "${GENERATED_DIR}"
)

target_link_libraries(
    kv_client
    PRIVATE
        kv_proto
)

target_compile_options(
    kv_client
    PRIVATE
        -Wall
        -Wextra
        -Wpedantic
)