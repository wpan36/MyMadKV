set working-directory := '..'

# list project 1 just recipes
default:
    @just --list p1 --unsorted

# install dependencies required by the C++ gRPC implementation
deps:
    sudo apt update
    sudo apt install -y \
        build-essential \
        cmake \
        pkg-config \
        protobuf-compiler \
        protobuf-compiler-grpc \
        libprotobuf-dev \
        libgrpc-dev \
        libgrpc++-dev

# build the Project 1 server and client in release mode
build:
    cmake \
        -S kvstore/in_memory_kvstore \
        -B kvstore/in_memory_kvstore/build \
        -DCMAKE_BUILD_TYPE=Release
    cmake \
        --build kvstore/in_memory_kvstore/build \
        -j"$(nproc)"

# remove Project 1 build outputs
clean:
    rm -rf kvstore/in_memory_kvstore/build

# run the KV store server
server listen="0.0.0.0:3777":
    @exec ./kvstore/in_memory_kvstore/build/bin/kv_server "{{listen}}"

# run the KV store client in stdin/stdout mode
client server="127.0.0.1:3777":
    @exec ./kvstore/in_memory_kvstore/build/bin/kv_client "{{server}}"

# single-client normal operation test
test1 server="127.0.0.1:3777":
    @bash kvstore/in_memory_kvstore/tests/run_test.sh 1 "{{server}}"

# single-client edge-case test
test2 server="127.0.0.1:3777":
    @bash kvstore/in_memory_kvstore/tests/run_test.sh 2 "{{server}}"

# concurrent clients operating on non-conflicting keys
test3 server="127.0.0.1:3777":
    @bash kvstore/in_memory_kvstore/tests/run_test.sh 3 "{{server}}"

# concurrent clients observing write and Swap operations
test4 server="127.0.0.1:3777":
    @bash kvstore/in_memory_kvstore/tests/run_test.sh 4 "{{server}}"

# concurrent clients observing Delete and recreation
test5 server="127.0.0.1:3777":
    @bash kvstore/in_memory_kvstore/tests/run_test.sh 5 "{{server}}"

# kill all processes of your KV store system
kill:
    -pkill -x kv_client >/dev/null 2>&1
    -pkill -x kv_server >/dev/null 2>&1

# NOTE: feel free to add more recipes as you see fit...
#       also feel free to add extra parameters to the recipes as you see fit,
#       but don't change the existing parameters

python_run := if `which uv || true` != "" { "uv run" } else { "python3" }
tmpdir_prefix := "/tmp/madkv-p1"

# launch a long-running KV store service
service listen="0.0.0.0:3777":
    just p1::build
    just utils::build
    cargo run -p runner -r --bin service -- \
        --server-just-args p1::server "{{listen}}" \
        --manager-just-args none

# ensure a subdir under 'tmp/' exists
tmpdir subdir:
    mkdir -p "{{tmpdir_prefix}}/{{subdir}}"

# run a student-provided testcase
testcase num server="127.0.0.1:3777": (tmpdir "tests")
    just p1::build
    just "p1::test{{num}}" "{{server}}" \
        | tee "{{tmpdir_prefix}}/tests/test{{num}}.log"
    just p1::kill

# run a fuzz testing scenario
fuzz nclis conflict="no" server="127.0.0.1:3777": (tmpdir "fuzz")
    just p1::build
    just utils::build
    cargo run -p runner -r --bin fuzzer -- \
        --num-clis "{{nclis}}" \
        {{ if conflict == "no" { "" } else { "--conflict" } }} \
        --client-just-args p1::client "{{server}}" \
        | tee "{{tmpdir_prefix}}/fuzz/fuzz-{{nclis}}-{{conflict}}.log"
    just p1::kill

# run a YCSB benchmark workload
bench nclis wload server="127.0.0.1:3777": (tmpdir "bench")
    just p1::build
    just utils::build
    just utils::ycsb
    cargo run -p runner -r --bin bencher -- \
        --num-clis "{{nclis}}" \
        --workload "{{wload}}" \
        --client-just-args p1::client "{{server}}" \
        | tee "/tmp/madkv-p1/bench/bench-{{nclis}}-{{wload}}.log"
    just p1::kill

# generate .md report template from existing results (wip)
report:
    {{python_run}} sumgen/proj1.py