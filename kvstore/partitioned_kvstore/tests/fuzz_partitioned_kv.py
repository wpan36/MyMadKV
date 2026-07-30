#!/usr/bin/env python3

from __future__ import annotations

import argparse
import difflib
import os
import random
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Dict, List, Optional, TextIO, Tuple


PROJECT_DIR = Path(__file__).resolve().parent.parent
BIN_DIR = PROJECT_DIR / "build" / "bin"

MANAGER_BINARY = BIN_DIR / "kv_manager"
SERVER_BINARY = BIN_DIR / "kv_server"
CLIENT_BINARY = BIN_DIR / "kv_client"

MANAGER_ADDRESS = "127.0.0.1:50250"
SERVER_PORTS = [50261, 50262, 50263]

KEYS = [f"k{index:03d}" for index in range(80)]


class ManagedProcess:
    def __init__(
        self,
        process: subprocess.Popen[str],
        stdout_file: TextIO,
        stderr_file: TextIO,
        description: str,
    ) -> None:
        self.process = process
        self.stdout_file = stdout_file
        self.stderr_file = stderr_file
        self.description = description

    def kill_abruptly(self) -> None:
        if self.process.poll() is None:
            self.process.kill()

        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=5)

        self.close_files()

    def terminate(self) -> None:
        if self.process.poll() is None:
            self.process.terminate()

            try:
                self.process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)

        self.close_files()

    def close_files(self) -> None:
        if not self.stdout_file.closed:
            self.stdout_file.close()

        if not self.stderr_file.closed:
            self.stderr_file.close()


class TestCluster:
    def __init__(self, work_dir: Path) -> None:
        self.work_dir = work_dir
        self.manager: Optional[ManagedProcess] = None
        self.servers: Dict[int, ManagedProcess] = {}
        self.server_generations = [0, 0, 0]

    @staticmethod
    def clean_environment() -> Dict[str, str]:
        environment = os.environ.copy()

        environment.pop(
            "MADKV_FAILPOINT_CRASH_CLIENT_ID",
            None,
        )

        environment.pop(
            "MADKV_FAILPOINT_CRASH_REQUEST_ID",
            None,
        )

        return environment

    def start_manager(self) -> None:
        stdout_path = self.work_dir / "manager.out"
        stderr_path = self.work_dir / "manager.err"

        stdout_file = stdout_path.open("w")
        stderr_file = stderr_path.open("w")

        process = subprocess.Popen(
            [
                str(MANAGER_BINARY),
                MANAGER_ADDRESS,
                ",".join(
                    f"127.0.0.1:{port}"
                    for port in SERVER_PORTS
                ),
            ],
            stdout=stdout_file,
            stderr=stderr_file,
            text=True,
            env=self.clean_environment(),
        )

        self.manager = ManagedProcess(
            process,
            stdout_file,
            stderr_file,
            "manager",
        )

        wait_for_text(
            stderr_path,
            "Cluster manager running",
        )

    def start_server(self, server_id: int) -> None:
        generation = self.server_generations[server_id]
        self.server_generations[server_id] += 1

        stdout_path = (
            self.work_dir
            / f"server-{server_id}-generation-{generation}.out"
        )

        stderr_path = (
            self.work_dir
            / f"server-{server_id}-generation-{generation}.err"
        )

        stdout_file = stdout_path.open("w")
        stderr_file = stderr_path.open("w")

        port = SERVER_PORTS[server_id]
        address = f"127.0.0.1:{port}"

        process = subprocess.Popen(
            [
                str(SERVER_BINARY),
                MANAGER_ADDRESS,
                str(server_id),
                address,
                address,
                str(
                    self.work_dir
                    / "data"
                    / f"server-{server_id}"
                ),
            ],
            stdout=stdout_file,
            stderr=stderr_file,
            text=True,
            env=self.clean_environment(),
        )

        managed = ManagedProcess(
            process,
            stdout_file,
            stderr_file,
            f"server {server_id}",
        )

        self.servers[server_id] = managed

        wait_for_text(
            stderr_path,
            f"Server {server_id} running",
        )

    def kill_and_restart_server(
        self,
        server_id: int,
    ) -> None:
        server = self.servers.pop(server_id)
        server.kill_abruptly()

        self.start_server(server_id)

    def restart_all_servers(self) -> None:
        for server_id in range(len(SERVER_PORTS)):
            server = self.servers.pop(server_id)
            server.kill_abruptly()

        for server_id in range(len(SERVER_PORTS)):
            self.start_server(server_id)

    def cleanup(self) -> None:
        for server in list(self.servers.values()):
            server.terminate()

        self.servers.clear()

        if self.manager is not None:
            self.manager.terminate()
            self.manager = None


def wait_for_text(
    path: Path,
    expected: str,
    timeout_seconds: float = 15.0,
) -> None:
    deadline = time.monotonic() + timeout_seconds

    while time.monotonic() < deadline:
        if path.exists():
            contents = path.read_text(
                errors="replace",
            )

            if expected in contents:
                return

        time.sleep(0.05)

    contents = ""

    if path.exists():
        contents = path.read_text(
            errors="replace",
        )

    raise RuntimeError(
        f"Timed out waiting for {expected!r} in {path}\n"
        f"----- log contents -----\n"
        f"{contents}"
    )


def assert_binaries_exist() -> None:
    for binary in (
        MANAGER_BINARY,
        SERVER_BINARY,
        CLIENT_BINARY,
    ):
        if not binary.is_file():
            raise RuntimeError(
                f"Missing binary: {binary}\n"
                "Build the project before running the fuzz test."
            )

        if not os.access(binary, os.X_OK):
            raise RuntimeError(
                f"Binary is not executable: {binary}"
            )


def make_value(
    seed: int,
    round_index: int,
    operation_index: int,
) -> str:
    return (
        f"v{seed}_"
        f"{round_index}_"
        f"{operation_index}"
    )


def append_scan_expected(
    expected_lines: List[str],
    model: Dict[str, str],
    start_key: str,
    end_key: str,
) -> None:
    expected_lines.append(
        f"SCAN {start_key} {end_key} BEGIN"
    )

    for key in sorted(model):
        if start_key <= key <= end_key:
            expected_lines.append(
                f"  {key} {model[key]}"
            )

    expected_lines.append("SCAN END")


def generate_round(
    rng: random.Random,
    model: Dict[str, str],
    seed: int,
    round_index: int,
    operation_count: int,
) -> Tuple[List[str], str, int]:
    commands: List[str] = []
    expected_lines: List[str] = []
    mutation_count = 0

    operation_types = [
        "PUT",
        "PUT",
        "SWAP",
        "SWAP",
        "GET",
        "GET",
        "GET",
        "DELETE",
        "SCAN",
    ]

    for operation_index in range(operation_count):
        operation = rng.choice(operation_types)

        if operation == "PUT":
            key = rng.choice(KEYS)

            value = make_value(
                seed,
                round_index,
                operation_index,
            )

            found = key in model
            model[key] = value

            commands.append(
                f"PUT {key} {value}"
            )

            expected_lines.append(
                f"PUT {key} "
                f"{'found' if found else 'not_found'}"
            )

            mutation_count += 1

        elif operation == "SWAP":
            key = rng.choice(KEYS)

            value = make_value(
                seed,
                round_index,
                operation_index,
            )

            old_value = model.get(key)
            model[key] = value

            commands.append(
                f"SWAP {key} {value}"
            )

            if old_value is None:
                expected_lines.append(
                    f"SWAP {key} null"
                )
            else:
                expected_lines.append(
                    f"SWAP {key} {old_value}"
                )

            mutation_count += 1

        elif operation == "GET":
            key = rng.choice(KEYS)
            value = model.get(key)

            commands.append(
                f"GET {key}"
            )

            if value is None:
                expected_lines.append(
                    f"GET {key} null"
                )
            else:
                expected_lines.append(
                    f"GET {key} {value}"
                )

        elif operation == "DELETE":
            key = rng.choice(KEYS)
            found = key in model

            if found:
                del model[key]

            commands.append(
                f"DELETE {key}"
            )

            expected_lines.append(
                f"DELETE {key} "
                f"{'found' if found else 'not_found'}"
            )

            mutation_count += 1

        elif operation == "SCAN":
            first = rng.randrange(len(KEYS))
            second = rng.randrange(len(KEYS))

            low = min(first, second)
            high = max(first, second)

            start_key = KEYS[low]
            end_key = KEYS[high]

            commands.append(
                f"SCAN {start_key} {end_key}"
            )

            append_scan_expected(
                expected_lines,
                model,
                start_key,
                end_key,
            )

        else:
            raise AssertionError(
                f"Unknown operation: {operation}"
            )

    expected_output = (
        "\n".join(expected_lines) + "\n"
    )

    return (
        commands,
        expected_output,
        mutation_count,
    )


def run_client(
    commands: List[str],
    client_id: str,
    starting_request_id: int,
    timeout_seconds: float = 60.0,
) -> subprocess.CompletedProcess[str]:
    environment = TestCluster.clean_environment()

    environment["MADKV_CLIENT_ID"] = client_id
    environment["MADKV_START_REQUEST_ID"] = str(
        starting_request_id
    )

    client_input = "\n".join(commands) + "\n"

    result = subprocess.run(
        [
            str(CLIENT_BINARY),
            MANAGER_ADDRESS,
        ],
        input=client_input,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=environment,
        timeout=timeout_seconds,
    )

    if result.returncode != 0:
        raise RuntimeError(
            f"Client exited with code {result.returncode}\n"
            f"----- stdin -----\n"
            f"{client_input}"
            f"----- stdout -----\n"
            f"{result.stdout}"
            f"----- stderr -----\n"
            f"{result.stderr}"
        )

    return result


def compare_output(
    expected: str,
    actual: str,
    description: str,
) -> None:
    if actual == expected:
        return

    difference = "".join(
        difflib.unified_diff(
            expected.splitlines(
                keepends=True
            ),
            actual.splitlines(
                keepends=True
            ),
            fromfile="expected",
            tofile="actual",
        )
    )

    raise AssertionError(
        f"{description} produced incorrect output\n"
        f"{difference}"
    )


def build_final_verification(
    model: Dict[str, str],
) -> Tuple[List[str], str]:
    commands: List[str] = []
    expected_lines: List[str] = []

    for key in KEYS:
        commands.append(f"GET {key}")

        value = model.get(key)

        if value is None:
            expected_lines.append(
                f"GET {key} null"
            )
        else:
            expected_lines.append(
                f"GET {key} {value}"
            )

    commands.append(
        f"SCAN {KEYS[0]} {KEYS[-1]}"
    )

    append_scan_expected(
        expected_lines,
        model,
        KEYS[0],
        KEYS[-1],
    )

    return (
        commands,
        "\n".join(expected_lines) + "\n",
    )


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Fuzz the partitioned MadKV cluster "
            "against a Python reference model."
        )
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=20260730,
        help="reproducible random seed",
    )

    parser.add_argument(
        "--rounds",
        type=int,
        default=6,
        help="number of fuzz rounds",
    )

    parser.add_argument(
        "--operations-per-round",
        type=int,
        default=80,
        help="operations generated in each round",
    )

    return parser.parse_args()


def main() -> int:
    arguments = parse_arguments()

    if arguments.rounds <= 0:
        raise ValueError(
            "--rounds must be positive"
        )

    if arguments.operations_per_round <= 0:
        raise ValueError(
            "--operations-per-round must be positive"
        )

    assert_binaries_exist()

    work_dir = Path(
        tempfile.mkdtemp(
            prefix="madkv-p2-fuzz-",
            dir="/tmp",
        )
    )

    cluster = TestCluster(work_dir)
    succeeded = False

    try:
        print(
            f"Fuzz workspace: {work_dir}"
        )

        print("[1] Starting manager")
        cluster.start_manager()

        print("[2] Starting three partition servers")

        for server_id in range(
            len(SERVER_PORTS)
        ):
            cluster.start_server(server_id)

        wait_for_text(
            work_dir / "manager.err",
            "cluster ready=true",
        )

        rng = random.Random(arguments.seed)
        model: Dict[str, str] = {}

        client_id = (
            f"partition-fuzz-{arguments.seed}"
        )

        next_request_id = 1
        total_operations = 0
        total_mutations = 0

        for round_index in range(
            arguments.rounds
        ):
            commands, expected, mutations = (
                generate_round(
                    rng,
                    model,
                    arguments.seed,
                    round_index,
                    arguments.operations_per_round,
                )
            )

            print(
                f"[3] Round {round_index + 1}/"
                f"{arguments.rounds}: "
                f"{len(commands)} operations, "
                f"starting request ID "
                f"{next_request_id}"
            )

            result = run_client(
                commands,
                client_id,
                next_request_id,
            )

            compare_output(
                expected,
                result.stdout,
                f"fuzz round {round_index + 1}",
            )

            next_request_id += mutations
            total_mutations += mutations
            total_operations += len(commands)

            server_to_restart = rng.randrange(
                len(SERVER_PORTS)
            )

            print(
                f"    SIGKILL and restart "
                f"server {server_to_restart}"
            )

            cluster.kill_and_restart_server(
                server_to_restart
            )

        print(
            "[4] SIGKILL and restart all servers"
        )

        cluster.restart_all_servers()

        verification_commands, verification_expected = (
            build_final_verification(model)
        )

        verification_result = run_client(
            verification_commands,
            client_id,
            next_request_id,
        )

        compare_output(
            verification_expected,
            verification_result.stdout,
            "final recovery verification",
        )

        print()
        print("PASS: partitioned KV fuzz test")
        print(
            f"  seed: {arguments.seed}"
        )
        print(
            f"  random operations: "
            f"{total_operations}"
        )
        print(
            f"  durable mutations: "
            f"{total_mutations}"
        )
        print(
            f"  server restarts: "
            f"{arguments.rounds + len(SERVER_PORTS)}"
        )
        print(
            "  final reference-model state matched"
        )

        succeeded = True
        return 0

    finally:
        cluster.cleanup()

        if succeeded:
            shutil.rmtree(
                work_dir,
                ignore_errors=True,
            )
        else:
            print(
                f"Test failed; logs preserved at "
                f"{work_dir}",
                file=sys.stderr,
            )


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print(
            "Interrupted",
            file=sys.stderr,
        )

        raise SystemExit(130)
    except Exception as error:
        print(
            f"FAIL: {error}",
            file=sys.stderr,
        )

        raise SystemExit(1)
