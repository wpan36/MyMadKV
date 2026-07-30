#!/usr/bin/env bash

set -euo pipefail

PROJECT_DIR="$(
    cd "$(dirname "${BASH_SOURCE[0]}")/.."
    pwd
)"

BIN_DIR="${PROJECT_DIR}/build/bin"

MANAGER="${BIN_DIR}/kv_manager"
SERVER="${BIN_DIR}/kv_server"
CLIENT="${BIN_DIR}/kv_client"

for binary in \
    "${MANAGER}" \
    "${SERVER}" \
    "${CLIENT}"
do
    if [[ ! -x "${binary}" ]]; then
        echo "Missing executable: ${binary}" >&2
        echo "Build the project before running this test." >&2
        exit 1
    fi
done

WORK_DIR="$(
    mktemp -d /tmp/madkv-p2-crash-test.XXXXXX
)"

PIDS=()

cleanup() {
    local status=$?

    trap - EXIT INT TERM
    set +e

    for pid in "${PIDS[@]}"; do
        kill "${pid}" 2>/dev/null || true
    done

    for pid in "${PIDS[@]}"; do
        wait "${pid}" 2>/dev/null || true
    done

    if [[ "${status}" -eq 0 ]]; then
        rm -rf "${WORK_DIR}"
    else
        echo "Test failed; logs preserved at:" >&2
        echo "  ${WORK_DIR}" >&2
    fi

    exit "${status}"
}

trap cleanup EXIT INT TERM

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

wait_for_log() {
    local file="$1"
    local pattern="$2"
    local attempts="${3:-150}"

    for ((i = 0; i < attempts; ++i)); do
        if grep -Fq "${pattern}" "${file}" 2>/dev/null; then
            return 0
        fi

        sleep 0.1
    done

    echo "Timed out waiting for '${pattern}' in ${file}" >&2

    if [[ -f "${file}" ]]; then
        echo "----- ${file} -----" >&2
        cat "${file}" >&2
    fi

    return 1
}

wait_for_exit() {
    local pid="$1"
    local attempts="${2:-150}"

    for ((i = 0; i < attempts; ++i)); do
        if ! kill -0 "${pid}" 2>/dev/null; then
            return 0
        fi

        sleep 0.1
    done

    return 1
}

start_normal_server() {
    local variable_name="$1"
    local server_id="$2"
    local port="$3"
    local log_name="$4"

    env \
      -u MADKV_FAILPOINT_CRASH_CLIENT_ID \
      -u MADKV_FAILPOINT_CRASH_REQUEST_ID \
      "${SERVER}" \
        127.0.0.1:50150 \
        "${server_id}" \
        "127.0.0.1:${port}" \
        "127.0.0.1:${port}" \
        "${WORK_DIR}/data/server-${server_id}" \
        >"${WORK_DIR}/${log_name}.out" \
        2>"${WORK_DIR}/${log_name}.err" &

    local pid=$!
    printf -v "${variable_name}" '%s' "${pid}"
    PIDS+=("${pid}")
}

start_failpoint_server_one() {
    env \
      MADKV_FAILPOINT_CRASH_CLIENT_ID=crash-test-client \
      MADKV_FAILPOINT_CRASH_REQUEST_ID=1 \
      "${SERVER}" \
        127.0.0.1:50150 \
        1 \
        127.0.0.1:50162 \
        127.0.0.1:50162 \
        "${WORK_DIR}/data/server-1" \
        >"${WORK_DIR}/server-1-failpoint.out" \
        2>"${WORK_DIR}/server-1-failpoint.err" &

    SERVER_ONE_FAILPOINT_PID=$!
    PIDS+=("${SERVER_ONE_FAILPOINT_PID}")
}

mkdir -p "${WORK_DIR}/data"

echo "[1/8] Starting manager"

"${MANAGER}" \
  127.0.0.1:50150 \
  127.0.0.1:50161,127.0.0.1:50162,127.0.0.1:50163 \
  >"${WORK_DIR}/manager.out" \
  2>"${WORK_DIR}/manager.err" &

MANAGER_PID=$!
PIDS+=("${MANAGER_PID}")

wait_for_log \
  "${WORK_DIR}/manager.err" \
  "Cluster manager running"

echo "[2/8] Starting three partition servers"

start_normal_server \
  SERVER_ZERO_PID \
  0 \
  50161 \
  server-0-initial

start_normal_server \
  SERVER_ONE_PID \
  1 \
  50162 \
  server-1-initial

start_normal_server \
  SERVER_TWO_PID \
  2 \
  50163 \
  server-2-initial

wait_for_log \
  "${WORK_DIR}/server-0-initial.err" \
  "Server 0 running"

wait_for_log \
  "${WORK_DIR}/server-1-initial.err" \
  "Server 1 running"

wait_for_log \
  "${WORK_DIR}/server-2-initial.err" \
  "Server 2 running"

wait_for_log \
  "${WORK_DIR}/manager.err" \
  "cluster ready=true"

echo "[3/8] Writing initial value to server 1"

# FNV1a64("delta") % 3 == 1.
printf '%s\n' \
  'PUT delta before' \
  | env \
      MADKV_CLIENT_ID=setup-client \
      MADKV_START_REQUEST_ID=1 \
      "${CLIENT}" \
        127.0.0.1:50150 \
        >"${WORK_DIR}/setup.out" \
        2>"${WORK_DIR}/setup.err"

grep -Fxq \
  'PUT delta not_found' \
  "${WORK_DIR}/setup.out" \
  || fail "initial PUT returned unexpected output"

echo "[4/8] Restarting server 1 with crash failpoint"

kill "${SERVER_ONE_PID}"
wait "${SERVER_ONE_PID}" 2>/dev/null || true

start_failpoint_server_one

wait_for_log \
  "${WORK_DIR}/server-1-failpoint.err" \
  "Server 1 running"

echo "[5/8] Sending SWAP that will crash after durable append"

printf '%s\n' \
  'SWAP delta after' \
  | env \
      MADKV_CLIENT_ID=crash-test-client \
      MADKV_START_REQUEST_ID=1 \
      "${CLIENT}" \
        127.0.0.1:50150 \
        >"${WORK_DIR}/swap.out" \
        2>"${WORK_DIR}/swap.err" &

SWAP_CLIENT_PID=$!
PIDS+=("${SWAP_CLIENT_PID}")

if ! wait_for_exit "${SERVER_ONE_FAILPOINT_PID}"; then
    fail "server 1 did not trigger the crash failpoint"
fi

set +e
wait "${SERVER_ONE_FAILPOINT_PID}"
FAILPOINT_EXIT_CODE=$?
set -e

if [[ "${FAILPOINT_EXIT_CODE}" -ne 86 ]]; then
    fail \
      "server 1 exited with ${FAILPOINT_EXIT_CODE}, expected 86"
fi

grep -Fq \
  'FAILPOINT: crashing after durable append' \
  "${WORK_DIR}/server-1-failpoint.err" \
  || fail "failpoint message was not found"

echo "[6/8] Restarting server 1 from the same durable log"

start_normal_server \
  SERVER_ONE_RECOVERED_PID \
  1 \
  50162 \
  server-1-recovered

wait_for_log \
  "${WORK_DIR}/server-1-recovered.err" \
  "Server 1 recovered durable log through sequence 2"

wait_for_log \
  "${WORK_DIR}/server-1-recovered.err" \
  "Server 1 running"

if ! wait_for_exit "${SWAP_CLIENT_PID}"; then
    fail "retrying client did not finish after server recovery"
fi

wait "${SWAP_CLIENT_PID}"

grep -Fxq \
  'SWAP delta before' \
  "${WORK_DIR}/swap.out" \
  || fail "SWAP did not return its original pre-crash reply"

echo "[7/8] Verifying recovered state and durable deduplication"

printf '%s\n' \
  'GET delta' \
  | "${CLIENT}" \
      127.0.0.1:50150 \
      >"${WORK_DIR}/get-after-recovery.out" \
      2>"${WORK_DIR}/get-after-recovery.err"

grep -Fxq \
  'GET delta after' \
  "${WORK_DIR}/get-after-recovery.out" \
  || fail "recovered value is incorrect"

# Submit exactly the same mutation identity again.
printf '%s\n' \
  'SWAP delta after' \
  | env \
      MADKV_CLIENT_ID=crash-test-client \
      MADKV_START_REQUEST_ID=1 \
      "${CLIENT}" \
        127.0.0.1:50150 \
        >"${WORK_DIR}/duplicate.out" \
        2>"${WORK_DIR}/duplicate.err"

grep -Fxq \
  'SWAP delta before' \
  "${WORK_DIR}/duplicate.out" \
  || fail "duplicate request did not return cached original reply"

echo "[8/8] Confirming duplicate retry created no extra log entry"

kill "${SERVER_ONE_RECOVERED_PID}"
wait "${SERVER_ONE_RECOVERED_PID}" 2>/dev/null || true

start_normal_server \
  SERVER_ONE_FINAL_PID \
  1 \
  50162 \
  server-1-final

wait_for_log \
  "${WORK_DIR}/server-1-final.err" \
  "Server 1 recovered durable log through sequence 2"

printf '%s\n' \
  'GET delta' \
  | "${CLIENT}" \
      127.0.0.1:50150 \
      >"${WORK_DIR}/final-get.out" \
      2>"${WORK_DIR}/final-get.err"

grep -Fxq \
  'GET delta after' \
  "${WORK_DIR}/final-get.out" \
  || fail "final recovered value is incorrect"

echo
echo "PASS: deterministic crash/recovery test"
echo "  - command was durable before crash"
echo "  - recovery replayed the command"
echo "  - client retry returned the original SWAP reply"
echo "  - duplicate retry did not append another log entry"
