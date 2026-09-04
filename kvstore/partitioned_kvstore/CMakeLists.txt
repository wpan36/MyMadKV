cmake_minimum_required(VERSION 3.16)

project(madkv_project2 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(
    CMAKE_RUNTIME_OUTPUT_DIRECTORY
    "${CMAKE_BINARY_DIR}/bin"
)

find_package(Protobuf REQUIRED)
find_package(PkgConfig REQUIRED)
find_package(Threads REQUIRED)

find_path(
    ROCKSDB_INCLUDE_DIR
    rocksdb/db.h
    REQUIRED
)

find_library(
    ROCKSDB_LIBRARY
    NAMES rocksdb
    REQUIRED
)

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
    KV_PROTO_FILE
    "${CMAKE_CURRENT_SOURCE_DIR}/in_memory_kvstore.proto"
)

set(
    KV_PROTO_SOURCES
    "${GENERATED_DIR}/in_memory_kvstore.pb.cc"
    "${GENERATED_DIR}/in_memory_kvstore.grpc.pb.cc"
)

set(
    KV_PROTO_HEADERS
    "${GENERATED_DIR}/in_memory_kvstore.pb.h"
    "${GENERATED_DIR}/in_memory_kvstore.grpc.pb.h"
)

add_custom_command(
    OUTPUT
        ${KV_PROTO_SOURCES}
        ${KV_PROTO_HEADERS}

    COMMAND
        "${PROTOC_EXECUTABLE}"

    ARGS
        "--proto_path=${CMAKE_CURRENT_SOURCE_DIR}"
        "--cpp_out=${GENERATED_DIR}"
        "--grpc_out=${GENERATED_DIR}"
        "--plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${KV_PROTO_FILE}"

    DEPENDS
        "${KV_PROTO_FILE}"

    COMMENT
        "Generating KV RPC protobuf and gRPC sources"

    VERBATIM
)

set(
    DURABLE_PROTO_FILE
    "${CMAKE_CURRENT_SOURCE_DIR}/durable_command.proto"
)

set(
    DURABLE_PROTO_SOURCES
    "${GENERATED_DIR}/durable_command.pb.cc"
)

set(
    DURABLE_PROTO_HEADERS
    "${GENERATED_DIR}/durable_command.pb.h"
)

add_custom_command(
    OUTPUT
        ${DURABLE_PROTO_SOURCES}
        ${DURABLE_PROTO_HEADERS}

    COMMAND
        "${PROTOC_EXECUTABLE}"

    ARGS
        "--proto_path=${CMAKE_CURRENT_SOURCE_DIR}"
        "--cpp_out=${GENERATED_DIR}"
        "${DURABLE_PROTO_FILE}"

    DEPENDS
        "${DURABLE_PROTO_FILE}"

    COMMENT
        "Generating durable command protobuf sources"

    VERBATIM
)

set(
    CLUSTER_PROTO_FILE
    "${CMAKE_CURRENT_SOURCE_DIR}/cluster.proto"
)

set(
    CLUSTER_PROTO_SOURCES
    "${GENERATED_DIR}/cluster.pb.cc"
    "${GENERATED_DIR}/cluster.grpc.pb.cc"
)

set(
    CLUSTER_PROTO_HEADERS
    "${GENERATED_DIR}/cluster.pb.h"
    "${GENERATED_DIR}/cluster.grpc.pb.h"
)

add_custom_command(
    OUTPUT
        ${CLUSTER_PROTO_SOURCES}
        ${CLUSTER_PROTO_HEADERS}

    COMMAND
        "${PROTOC_EXECUTABLE}"

    ARGS
        "--proto_path=${CMAKE_CURRENT_SOURCE_DIR}"
        "--cpp_out=${GENERATED_DIR}"
        "--grpc_out=${GENERATED_DIR}"
        "--plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${CLUSTER_PROTO_FILE}"

    DEPENDS
        "${CLUSTER_PROTO_FILE}"

    COMMENT
        "Generating cluster manager protobuf and gRPC sources"

    VERBATIM
)

add_library(
    kv_proto
    ${KV_PROTO_SOURCES}
    ${KV_PROTO_HEADERS}
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
    durable_proto
    ${DURABLE_PROTO_SOURCES}
    ${DURABLE_PROTO_HEADERS}
)

target_include_directories(
    durable_proto
    PUBLIC
        "${GENERATED_DIR}"
)

target_link_libraries(
    durable_proto
    PUBLIC
        protobuf::libprotobuf
)

add_library(
    cluster_proto
    ${CLUSTER_PROTO_SOURCES}
    ${CLUSTER_PROTO_HEADERS}
)

target_include_directories(
    cluster_proto
    PUBLIC
        "${GENERATED_DIR}"
)

target_link_libraries(
    cluster_proto
    PUBLIC
        PkgConfig::GRPCPP
        protobuf::libprotobuf
)

add_library(
    p2_core
    in_memory_kvstore.cc
    durable_command_log.cc
)

target_include_directories(
    p2_core
    PUBLIC
        "${CMAKE_CURRENT_SOURCE_DIR}"
        "${CMAKE_CURRENT_SOURCE_DIR}/../include"
        "${GENERATED_DIR}"
        "${ROCKSDB_INCLUDE_DIR}"
)

target_link_libraries(
    p2_core
    PUBLIC
        durable_proto
        "${ROCKSDB_LIBRARY}"
        Threads::Threads
)

target_compile_options(
    p2_core
    PRIVATE
        -Wall
        -Wextra
        -Wpedantic
)

add_library(
    cluster_client
    cluster_client.cc
)

target_include_directories(
    cluster_client
    PUBLIC
        "${CMAKE_CURRENT_SOURCE_DIR}"
        "${GENERATED_DIR}"
)

target_link_libraries(
    cluster_client
    PUBLIC
        cluster_proto
)

target_compile_options(
    cluster_client
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
        p2_core
        kv_proto
        cluster_client
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
        cluster_client
)

target_compile_options(
    kv_client
    PRIVATE
        -Wall
        -Wextra
        -Wpedantic
)

add_executable(
    kv_manager
    manager.cc
)

target_include_directories(
    kv_manager
    PRIVATE
        "${GENERATED_DIR}"
)

target_link_libraries(
    kv_manager
    PRIVATE
        cluster_proto
)

target_compile_options(
    kv_manager
    PRIVATE
        -Wall
        -Wextra
        -Wpedantic
)
# ---------------------------------------------------------------------------
# Project 3 Raft support
# ---------------------------------------------------------------------------

set(
    RAFT_PROTO_FILE
    "${CMAKE_CURRENT_SOURCE_DIR}/raft.proto"
)

set(
    RAFT_PROTO_SOURCES
    "${GENERATED_DIR}/raft.pb.cc"
    "${GENERATED_DIR}/raft.grpc.pb.cc"
)

set(
    RAFT_PROTO_HEADERS
    "${GENERATED_DIR}/raft.pb.h"
    "${GENERATED_DIR}/raft.grpc.pb.h"
)

add_custom_command(
    OUTPUT
        ${RAFT_PROTO_SOURCES}
        ${RAFT_PROTO_HEADERS}

    COMMAND
        "${PROTOC_EXECUTABLE}"

    ARGS
        "--proto_path=${CMAKE_CURRENT_SOURCE_DIR}"
        "--cpp_out=${GENERATED_DIR}"
        "--grpc_out=${GENERATED_DIR}"
        "--plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${RAFT_PROTO_FILE}"

    DEPENDS
        "${RAFT_PROTO_FILE}"

    COMMENT
        "Generating Raft protobuf and gRPC sources"

    VERBATIM
)

add_library(
    raft_proto
    ${RAFT_PROTO_SOURCES}
    ${RAFT_PROTO_HEADERS}
)

target_include_directories(
    raft_proto
    PUBLIC
        "${GENERATED_DIR}"
)

target_link_libraries(
    raft_proto
    PUBLIC
        PkgConfig::GRPCPP
        protobuf::libprotobuf
)

add_library(
    raft_core
    raft_storage.cc
    raft_node.cc
)

target_include_directories(
    raft_core
    PUBLIC
        "${CMAKE_CURRENT_SOURCE_DIR}"
        "${GENERATED_DIR}"
        "${ROCKSDB_INCLUDE_DIR}"
)

target_link_libraries(
    raft_core
    PUBLIC
        raft_proto
        "${ROCKSDB_LIBRARY}"
        Threads::Threads
)

target_compile_options(
    raft_core
    PRIVATE
        -Wall
        -Wextra
        -Wpedantic
)

# Temporary development executable used only to validate Raft election and
# persistent term/vote state before the KV server is wired into Raft.
add_executable(
    raft_election_demo
    raft_election_demo.cc
)

target_link_libraries(
    raft_election_demo
    PRIVATE
        raft_core
)

target_compile_options(
    raft_election_demo
    PRIVATE
        -Wall
        -Wextra
        -Wpedantic
)
# ---------------------------------------------------------------------------
# Project 3 Step 4: Raft-backed KV state machine
# ---------------------------------------------------------------------------

add_library(
    replicated_kv_service
    replicated_kv_service.cc
)

target_include_directories(
    replicated_kv_service
    PUBLIC
        "${CMAKE_CURRENT_SOURCE_DIR}"
        "${CMAKE_CURRENT_SOURCE_DIR}/../include"
        "${GENERATED_DIR}"
)

target_link_libraries(
    replicated_kv_service
    PUBLIC
        p2_core
        kv_proto
        raft_core
)

target_compile_options(
    replicated_kv_service
    PRIVATE
        -Wall
        -Wextra
        -Wpedantic
)

# Temporary Step-4 integration server. Step 5 will wire the same
# ReplicatedKVService into the real kv_server using manager topology.
add_executable(
    raft_kv_demo_server
    raft_kv_demo_server.cc
)

target_link_libraries(
    raft_kv_demo_server
    PRIVATE
        replicated_kv_service
)

target_compile_options(
    raft_kv_demo_server
    PRIVATE
        -Wall
        -Wextra
        -Wpedantic
)

# Tiny direct client for Step-4 integration testing without manager/client
# topology logic. It is intentionally not the final Project-3 client.
add_executable(
    kv_direct_client_demo
    kv_direct_client_demo.cc
)

target_include_directories(
    kv_direct_client_demo
    PRIVATE
        "${GENERATED_DIR}"
)

target_link_libraries(
    kv_direct_client_demo
    PRIVATE
        kv_proto
)

target_compile_options(
    kv_direct_client_demo
    PRIVATE
        -Wall
        -Wextra
        -Wpedantic
)

# ---------------------------------------------------------------------------
# Project 3 Step 5: wire final server binary to Raft-backed KV service
# ---------------------------------------------------------------------------

target_link_libraries(
    kv_server
    PRIVATE
        replicated_kv_service
        raft_core
)
