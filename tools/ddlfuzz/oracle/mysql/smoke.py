#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# ///

from __future__ import annotations

import json
import os
import struct
import subprocess
import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
BIN = ROOT / "build" / "oracle-mysql"

PARSE_BATCH = 1
GET_COVERAGE = 2
HELLO = 3


EXPECTED = {
    "ALTER TABLE CamelCase ADD COLUMN c INT": {
        "verdict": "accept",
        "stmts": [
            {
                "kind": "alter_table",
                "schema": "",
                "table": "CamelCase",
                "specs": [
                    {
                        "op": "add",
                        "cols": [
                            {
                                "name": "c",
                                "type_str": "int",
                                "not_null": False,
                                "params_written": None,
                            }
                        ],
                        "has_position": False,
                    }
                ],
            }
        ],
    },
    "ALTER TABLE t ADD c INT NOT NULL": {
        "verdict": "accept",
        "stmts": [
            {
                "kind": "alter_table",
                "schema": "",
                "table": "t",
                "specs": [
                    {
                        "op": "add",
                        "cols": [
                            {
                                "name": "c",
                                "type_str": "int",
                                "not_null": True,
                                "params_written": None,
                            }
                        ],
                        "has_position": False,
                    }
                ],
            }
        ],
    },
    "ALTER TABLE s.t ADD c DECIMAL(10,2) UNSIGNED": {
        "verdict": "accept",
        "stmts": [
            {
                "kind": "alter_table",
                "schema": "s",
                "table": "t",
                "specs": [
                    {
                        "op": "add",
                        "cols": [
                            {
                                "name": "c",
                                "type_str": "decimal(10,2) unsigned",
                                "not_null": False,
                                "params_written": [10, 2],
                            }
                        ],
                        "has_position": False,
                    }
                ],
            }
        ],
    },
    "ALTER TABLE t MODIFY c BOOL": {
        "verdict": "accept",
        "stmts": [
            {
                "kind": "alter_table",
                "schema": "",
                "table": "t",
                "specs": [
                    {
                        "op": "modify",
                        "cols": [
                            {
                                "name": "c",
                                "type_str": "tinyint(1)",
                                "not_null": False,
                                "params_written": [1],
                            }
                        ],
                        "has_position": False,
                    }
                ],
            }
        ],
    },
    "ALTER TABLE t CHANGE a b INT": {
        "verdict": "accept",
        "stmts": [
            {
                "kind": "alter_table",
                "schema": "",
                "table": "t",
                "specs": [
                    {
                        "op": "change",
                        "old_name": "a",
                        "cols": [
                            {
                                "name": "b",
                                "type_str": "int",
                                "not_null": False,
                                "params_written": None,
                            }
                        ],
                        "has_position": False,
                    }
                ],
            }
        ],
    },
    "ALTER TABLE t DROP c": {
        "verdict": "accept",
        "stmts": [
            {
                "kind": "alter_table",
                "schema": "",
                "table": "t",
                "specs": [{"op": "drop", "old_name": "c"}],
            }
        ],
    },
    "ALTER TABLE t RENAME COLUMN a TO b": {
        "verdict": "accept",
        "stmts": [
            {
                "kind": "alter_table",
                "schema": "",
                "table": "t",
                "specs": [{"op": "rename_col", "old_name": "a", "new_name": "b"}],
            }
        ],
    },
    "ALTER TABLE t ADD c SERIAL": {
        "verdict": "accept",
        "stmts": [
            {
                "kind": "alter_table",
                "schema": "",
                "table": "t",
                "specs": [
                    {
                        "op": "add",
                        "cols": [
                            {
                                "name": "c",
                                "type_str": "bigint unsigned",
                                "not_null": True,
                                "params_written": None,
                            }
                        ],
                        "has_position": False,
                    }
                ],
            }
        ],
    },
    "ALTER TABLE t ADD c INT FIRST": {
        "verdict": "accept",
        "stmts": [
            {
                "kind": "alter_table",
                "schema": "",
                "table": "t",
                "specs": [
                    {
                        "op": "add",
                        "cols": [
                            {
                                "name": "c",
                                "type_str": "int",
                                "not_null": False,
                                "params_written": None,
                            }
                        ],
                        "has_position": True,
                    }
                ],
            }
        ],
    },
    "RENAME TABLE a TO b, c.d TO e.f": {
        "verdict": "accept",
        "stmts": [
            {
                "kind": "rename_table",
                "pairs": [
                    {
                        "old_schema": "",
                        "old_table": "a",
                        "new_schema": "",
                        "new_table": "b",
                    },
                    {
                        "old_schema": "c",
                        "old_table": "d",
                        "new_schema": "e",
                        "new_table": "f",
                    },
                ],
            }
        ],
    },
    "SELECT 1": {"verdict": "accept", "stmts": [{"kind": "other"}]},
    "ALTER TABLE t ADD c INT; ALTER TABLE t DROP c": {
        "verdict": "accept",
        "stmts": [
            {
                "kind": "alter_table",
                "schema": "",
                "table": "t",
                "specs": [
                    {
                        "op": "add",
                        "cols": [
                            {
                                "name": "c",
                                "type_str": "int",
                                "not_null": False,
                                "params_written": None,
                            }
                        ],
                        "has_position": False,
                    }
                ],
            },
            {
                "kind": "alter_table",
                "schema": "",
                "table": "t",
                "specs": [{"op": "drop", "old_name": "c"}],
            },
        ],
    },
    "SET STATEMENT max_statement_time=1 FOR SELECT 1": {
        "verdict": "reject",
        "error_prefix": "1064: ",
    },
    "CREATE EXTERNAL TABLE db.t ENGINE=InnoDB": {
        "verdict": "reject",
        "error_prefix": "6512: ",
    },
    "CREATE EXTERNAL TABLE db.t ENGINE=zzz_bogus": {
        "verdict": "reject",
        "error_prefix": "1286: ",
    },
    "ALTER TABLE t ADD c INT, ENGINE=InnoDB": {
        "verdict": "accept",
        "stmts": [
            {
                "kind": "alter_table",
                "schema": "",
                "table": "t",
                "specs": [
                    {
                        "op": "add",
                        "cols": [
                            {
                                "name": "c",
                                "type_str": "int",
                                "not_null": False,
                                "params_written": None,
                            }
                        ],
                        "has_position": False,
                    }
                ],
            }
        ],
    },
    "ALTER TABLE t ENGINE=MyISAM": {
        "verdict": "accept",
        "stmts": [
            {
                "kind": "alter_table",
                "schema": "",
                "table": "t",
                "specs": [],
            }
        ],
    },
    "CREATE TABLE t (a INT) ENGINE=InnoDB": {
        "verdict": "accept",
        "stmts": [{"kind": "other"}],
    },
}


VARIED = [
    "ALTER TABLE t ADD c INT",
    "ALTER TABLE t ADD c BIGINT UNSIGNED",
    "ALTER TABLE t ADD c DECIMAL",
    "ALTER TABLE t ADD c DECIMAL(12,4) UNSIGNED",
    "ALTER TABLE t ADD c TINYINT(1)",
    "ALTER TABLE t ADD c VARCHAR(32)",
    "ALTER TABLE t ADD c VARBINARY(32)",
    "ALTER TABLE t ADD c TEXT",
    "ALTER TABLE t ADD c BLOB",
    "ALTER TABLE t ADD c BIT",
    "ALTER TABLE t MODIFY c BOOL",
    "ALTER TABLE t CHANGE a b INT AFTER z",
    "ALTER TABLE t DROP c",
    "ALTER TABLE t RENAME COLUMN a TO b",
    "RENAME TABLE a TO b",
    "SELECT 1",
    "CREATE TABLE x (id int)",
    "ALTER TABLE s.t ADD c INT",
    "ALTER TABLE t ADD c INT FIRST",
    "ALTER TABLE t ADD (a INT, b BIGINT)",
]


def frame(msg: int, payload: bytes = b"") -> bytes:
    body = bytes([msg]) + payload
    return struct.pack("<I", len(body)) + body


def read_exact(pipe, n: int) -> bytes:
    chunks = []
    got = 0
    while got < n:
        chunk = pipe.read(n - got)
        if not chunk:
            raise EOFError("oracle closed stdout")
        chunks.append(chunk)
        got += len(chunk)
    return b"".join(chunks)


class Oracle:
    def __init__(self) -> None:
        self.proc = subprocess.Popen(
            [str(BIN)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

    def close(self) -> None:
        if self.proc.stdin:
            self.proc.stdin.close()
        try:
            self.proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait(timeout=10)
        if self.proc.returncode not in (0, None):
            stderr = self.proc.stderr.read().decode("utf-8", "replace")
            raise AssertionError(f"oracle exited {self.proc.returncode}\n{stderr}")

    def request(self, msg: int, payload: bytes = b"") -> bytes:
        assert self.proc.stdin is not None
        assert self.proc.stdout is not None
        self.proc.stdin.write(frame(msg, payload))
        self.proc.stdin.flush()
        size = struct.unpack("<I", read_exact(self.proc.stdout, 4))[0]
        body = read_exact(self.proc.stdout, size)
        assert body and body[0] == msg, body
        return body[1:]

    def hello(self) -> dict:
        payload = self.request(HELLO)
        n = struct.unpack_from("<I", payload, 0)[0]
        raw = payload[4 : 4 + n]
        return json.loads(raw)

    def coverage(self) -> bytes:
        payload = self.request(GET_COVERAGE)
        n = struct.unpack_from("<I", payload, 0)[0]
        counters = payload[4 : 4 + n]
        assert len(counters) == n
        return counters

    def parse(self, cases: list[tuple[int, bytes]]) -> tuple[list[bytes], list[int]]:
        payload = struct.pack("<I", len(cases))
        for mode, sql in cases:
            payload += struct.pack("<QI", mode, len(sql)) + sql
        raw = self.request(PARSE_BATCH, payload)
        off = 0
        count = struct.unpack_from("<I", raw, off)[0]
        off += 4
        assert count == len(cases)
        out = []
        edges = []
        for _ in range(count):
            new_edges, n = struct.unpack_from("<II", raw, off)
            off += 8
            edges.append(new_edges)
            out.append(raw[off : off + n])
            off += n
        assert off == len(raw)
        return out, edges


def popcount(data: bytes) -> int:
    return sum(1 for b in data if b)


def rss_kb(pid: int) -> int:
    out = subprocess.check_output(["ps", "-o", "rss=", "-p", str(pid)], text=True)
    return int(out.strip())


def footprint_kb(pid: int) -> int:
    try:
        out = subprocess.run(
            ["vmmap", "--summary", str(pid)], capture_output=True, text=True
        ).stdout
    except FileNotFoundError:
        return -1
    for line in out.splitlines():
        if line.startswith("Physical footprint:"):
            val = line.split()[-1]
            for suffix, mult in (("G", 1024 * 1024), ("M", 1024), ("K", 1)):
                if val.endswith(suffix):
                    return int(float(val[: -len(suffix)]) * mult)
    return -1


def assert_digest(sql: str, raw: bytes) -> None:
    actual = json.loads(raw)
    expected = EXPECTED[sql]
    if "error_prefix" in expected:
        assert actual["verdict"] == expected["verdict"], actual
        assert actual["error"].startswith(expected["error_prefix"]), actual
        return
    assert actual == expected, f"{sql}\nactual={actual!r}\nexpected={expected!r}"


def main() -> int:
    if not BIN.exists():
        raise SystemExit(f"missing oracle binary: {BIN}")

    oracle = Oracle()
    try:
        assert oracle.hello() == {
            "engine": "mysql",
            "server_version": "9.7.0",
            "protocol": 2,
        }

        cases = [(0, sql.encode()) for sql in EXPECTED]
        cases.append((0, b"ALTER TABLE t ADD"))
        raw, edges = oracle.parse(cases)
        assert edges[0] > 0, edges
        for sql, digest in zip(EXPECTED, raw[:-1], strict=True):
            assert_digest(sql, digest)
        garbage = json.loads(raw[-1])
        assert garbage["verdict"] == "reject", garbage
        assert garbage["error"].startswith("1064: "), garbage

        cov1 = oracle.coverage()
        assert len(cov1) > 0
        oracle.parse([(0, VARIED[0].encode())])
        cov2 = oracle.coverage()
        oracle.parse([(0, sql.encode()) for sql in VARIED])
        cov3 = oracle.coverage()
        assert len(cov2) == len(cov1)
        assert len(cov3) == len(cov1)
        assert popcount(cov3) > popcount(cov2)

        batch100 = [(0, VARIED[i % len(VARIED)].encode()) for i in range(100)]
        first, _ = oracle.parse(batch100)
        second, second_edges = oracle.parse(batch100)
        assert first == second
        # per-case attribution: a repeat of the same batch opens ~nothing new.
        # A tiny residue is real second-execution coverage (state primed by the
        # first run flips a branch), not attribution slop.
        assert sum(second_edges) <= 4, second_edges

        hygiene, _ = oracle.parse(
            [
                (0, b"ALTER TABLE t ADD c INT"),
                (0, b"ALTER TABLE t ADD"),
                (0, b"ALTER TABLE t DROP c"),
            ]
        )
        assert json.loads(hygiene[0])["verdict"] == "accept", hygiene
        assert json.loads(hygiene[1])["verdict"] == "reject", hygiene
        assert json.loads(hygiene[2])["verdict"] == "accept", hygiene

        before = rss_kb(oracle.proc.pid)
        trivial = [(0, b"ALTER TABLE t ADD c INT")] * 5000
        for _ in range(200):
            replies, _ = oracle.parse(trivial)
            assert replies[0] == replies[-1]
        time.sleep(0.2)
        after = rss_kb(oracle.proc.pid)
        assert after - before < 50 * 1024, (before, after)

        sp_before = footprint_kb(oracle.proc.pid)
        if sp_before > 0:
            sp_batch = [
                (
                    0,
                    f"CREATE PROCEDURE sp_{i}() BEGIN DECLARE v INT; "
                    f"SET v = 1; END".encode(),
                )
                for i in range(1000)
            ]
            for _ in range(4):
                replies, _ = oracle.parse(sp_batch)
                assert json.loads(replies[0])["verdict"] == "accept", replies[0]
            sp_after = footprint_kb(oracle.proc.pid)
            assert sp_after - sp_before < 32 * 1024, (sp_before, sp_after)

    finally:
        oracle.close()

    print("mysql oracle smoke passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
