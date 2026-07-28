#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 2 ]]; then
    echo "Usage: $0 <test-number: 1-5> <server-address>" >&2
    exit 1
fi

test_num="$1"
server_addr="$2"

if [[ ! "$test_num" =~ ^[1-5]$ ]]; then
    echo "Test number must be between 1 and 5." >&2
    exit 1
fi

# tests/ -> in_memory_kvstore/ -> kvstore/ -> repository root
root_dir="$(
    cd "$(dirname "${BASH_SOURCE[0]}")/../../.."
    pwd
)"

client=(
    just
    --justfile "$root_dir/Justfile"
    p1::client
    "$server_addr"
)

tmpdir="$(mktemp -d)"
pids=()

cleanup() {
    for pid in "${pids[@]}"; do
        kill "$pid" >/dev/null 2>&1 || true
    done

    rm -rf "$tmpdir"
}

trap cleanup EXIT INT TERM

# Delete keys before a test so that the test can be run repeatedly
# against the same server.
reset_keys() {
    {
        for key in "$@"; do
            printf 'DELETE %s\n' "$key"
        done

        printf 'STOP\n'
    } | "${client[@]}" >/dev/null
}

run_single_client_test() {
    local test_name="$1"

    "${client[@]}" \
        <"$tmpdir/input" \
        >"$tmpdir/output"

    if ! diff -u "$tmpdir/expected" "$tmpdir/output"; then
        echo "[FAIL] $test_name" >&2
        exit 1
    fi

    cat "$tmpdir/output"
    echo "[PASS] $test_name"
}

run_two_independent_clients() {
    "${client[@]}" \
        <"$tmpdir/client-a.in" \
        >"$tmpdir/client-a.out" &

    pids+=("$!")

    "${client[@]}" \
        <"$tmpdir/client-b.in" \
        >"$tmpdir/client-b.out" &

    pids+=("$!")

    wait "${pids[0]}"
    wait "${pids[1]}"

    diff -u \
        "$tmpdir/client-a.expected" \
        "$tmpdir/client-a.out"

    diff -u \
        "$tmpdir/client-b.expected" \
        "$tmpdir/client-b.out"

    echo "=== Client A ==="
    cat "$tmpdir/client-a.out"

    echo "=== Client B ==="
    cat "$tmpdir/client-b.out"
}

wait_for_line() {
    local file="$1"
    local expected="$2"
    local deadline=$((SECONDS + 10))

    until grep -Fqx "$expected" "$file" 2>/dev/null; do
        if ((SECONDS >= deadline)); then
            echo "Timed out waiting for output:" >&2
            echo "  $expected" >&2
            echo "Current output:" >&2
            cat "$file" >&2 || true
            exit 1
        fi

        sleep 0.02
    done
}

start_interactive_clients() {
    mkfifo "$tmpdir/client-a.fifo"
    mkfifo "$tmpdir/client-b.fifo"

    # Opening each FIFO for both reading and writing prevents startup
    # from blocking while the client processes are being created.
    exec 3<>"$tmpdir/client-a.fifo"
    exec 4<>"$tmpdir/client-b.fifo"

    "${client[@]}" \
        <"$tmpdir/client-a.fifo" \
        >"$tmpdir/client-a.out" &

    pids+=("$!")

    "${client[@]}" \
        <"$tmpdir/client-b.fifo" \
        >"$tmpdir/client-b.out" &

    pids+=("$!")
}

finish_interactive_clients() {
    printf 'STOP\n' >&3
    printf 'STOP\n' >&4

    wait_for_line "$tmpdir/client-a.out" "STOP"
    wait_for_line "$tmpdir/client-b.out" "STOP"

    exec 3>&-
    exec 4>&-

    wait "${pids[0]}"
    wait "${pids[1]}"
}

check_interactive_results() {
    local test_name="$1"

    diff -u \
        "$tmpdir/client-a.expected" \
        "$tmpdir/client-a.out"

    diff -u \
        "$tmpdir/client-b.expected" \
        "$tmpdir/client-b.out"

    echo "=== Client A ==="
    cat "$tmpdir/client-a.out"

    echo "=== Client B ==="
    cat "$tmpdir/client-b.out"

    echo "[PASS] $test_name"
}

case "$test_num" in
    1)
        reset_keys t1_alpha t1_beta t1_delta

        cat >"$tmpdir/input" <<'EOF'
GET t1_alpha
PUT t1_alpha one
PUT t1_alpha two
GET t1_alpha
SWAP t1_alpha three
GET t1_alpha
PUT t1_beta bee
PUT t1_delta dee
SCAN t1_alpha t1_delta
DELETE t1_beta
GET t1_beta
DELETE t1_beta
STOP
EOF

        cat >"$tmpdir/expected" <<'EOF'
GET t1_alpha null
PUT t1_alpha not_found
PUT t1_alpha found
GET t1_alpha two
SWAP t1_alpha two
GET t1_alpha three
PUT t1_beta not_found
PUT t1_delta not_found
SCAN t1_alpha t1_delta BEGIN
  t1_alpha three
  t1_beta bee
  t1_delta dee
SCAN END
DELETE t1_beta found
GET t1_beta null
DELETE t1_beta not_found
STOP
EOF

        run_single_client_test \
            "Test 1: single-client normal operations"
        ;;

    2)
        reset_keys t2_ghost t2_A t2_a t2_aa

        cat >"$tmpdir/input" <<'EOF'
GET t2_ghost
SWAP t2_ghost created
GET t2_ghost
DELETE t2_ghost
PUT t2_A upper
PUT t2_a lower
PUT t2_aa double
SCAN t2_A t2_aa
PUT t2_a lower2
SWAP t2_a lower3
GET t2_a
SCAN zzz aaa
DELETE t2_a
GET t2_a
STOP
EOF

        cat >"$tmpdir/expected" <<'EOF'
GET t2_ghost null
SWAP t2_ghost null
GET t2_ghost created
DELETE t2_ghost found
PUT t2_A not_found
PUT t2_a not_found
PUT t2_aa not_found
SCAN t2_A t2_aa BEGIN
  t2_A upper
  t2_a lower
  t2_aa double
SCAN END
PUT t2_a found
SWAP t2_a lower2
GET t2_a lower3
SCAN zzz aaa BEGIN
SCAN END
DELETE t2_a found
GET t2_a null
STOP
EOF

        run_single_client_test \
            "Test 2: single-client edge cases"
        ;;

    3)
        reset_keys t3_a1 t3_a2 t3_b1

        cat >"$tmpdir/client-a.in" <<'EOF'
PUT t3_a1 one
PUT t3_a2 two
GET t3_a1
SCAN t3_a1 t3_a9
STOP
EOF

        cat >"$tmpdir/client-b.in" <<'EOF'
PUT t3_b1 three
SWAP t3_b1 four
GET t3_b1
DELETE t3_b1
STOP
EOF

        cat >"$tmpdir/client-a.expected" <<'EOF'
PUT t3_a1 not_found
PUT t3_a2 not_found
GET t3_a1 one
SCAN t3_a1 t3_a9 BEGIN
  t3_a1 one
  t3_a2 two
SCAN END
STOP
EOF

        cat >"$tmpdir/client-b.expected" <<'EOF'
PUT t3_b1 not_found
SWAP t3_b1 three
GET t3_b1 four
DELETE t3_b1 found
STOP
EOF

        run_two_independent_clients
        echo "[PASS] Test 3: concurrent clients without conflicts"
        ;;

    4)
        reset_keys t4_shared
        start_interactive_clients

        # Client A's write must be acknowledged before Client B reads it.
        printf 'PUT t4_shared v1\n' >&3
        wait_for_line \
            "$tmpdir/client-a.out" \
            "PUT t4_shared not_found"

        # Client B must observe A's acknowledged write.
        printf 'GET t4_shared\n' >&4
        wait_for_line \
            "$tmpdir/client-b.out" \
            "GET t4_shared v1"

        # Client B replaces the value.
        printf 'SWAP t4_shared v2\n' >&4
        wait_for_line \
            "$tmpdir/client-b.out" \
            "SWAP t4_shared v1"

        # Client A must observe B's acknowledged Swap.
        printf 'GET t4_shared\n' >&3
        wait_for_line \
            "$tmpdir/client-a.out" \
            "GET t4_shared v2"

        finish_interactive_clients

        cat >"$tmpdir/client-a.expected" <<'EOF'
PUT t4_shared not_found
GET t4_shared v2
STOP
EOF

        cat >"$tmpdir/client-b.expected" <<'EOF'
GET t4_shared v1
SWAP t4_shared v1
STOP
EOF

        check_interactive_results \
            "Test 4: cross-client write and Swap visibility"
        ;;

    5)
        reset_keys t5_item
        start_interactive_clients

        printf 'PUT t5_item first\n' >&3
        wait_for_line \
            "$tmpdir/client-a.out" \
            "PUT t5_item not_found"

        # B deletes A's acknowledged value.
        printf 'DELETE t5_item\n' >&4
        wait_for_line \
            "$tmpdir/client-b.out" \
            "DELETE t5_item found"

        # A must observe the acknowledged deletion.
        printf 'GET t5_item\n' >&3
        wait_for_line \
            "$tmpdir/client-a.out" \
            "GET t5_item null"

        # B recreates the deleted key.
        printf 'PUT t5_item second\n' >&4
        wait_for_line \
            "$tmpdir/client-b.out" \
            "PUT t5_item not_found"

        # A replaces B's recreated value.
        printf 'SWAP t5_item third\n' >&3
        wait_for_line \
            "$tmpdir/client-a.out" \
            "SWAP t5_item second"

        # B must observe A's acknowledged Swap.
        printf 'GET t5_item\n' >&4
        wait_for_line \
            "$tmpdir/client-b.out" \
            "GET t5_item third"

        finish_interactive_clients

        cat >"$tmpdir/client-a.expected" <<'EOF'
PUT t5_item not_found
GET t5_item null
SWAP t5_item second
STOP
EOF

        cat >"$tmpdir/client-b.expected" <<'EOF'
DELETE t5_item found
PUT t5_item not_found
GET t5_item third
STOP
EOF

        check_interactive_results \
            "Test 5: cross-client delete and recreate visibility"
        ;;
esac