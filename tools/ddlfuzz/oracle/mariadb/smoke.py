#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# ///

from __future__ import annotations

import argparse
import json
import math
import os
import struct
import subprocess
import sys
import threading
import time
from pathlib import Path

MSG_PARSE_BATCH = 1
MSG_GET_COVERAGE = 2
MSG_HELLO = 3

MODE_ORACLE = 1 << 9
MODE_MSSQL = 1 << 10

BUILD = Path(__file__).resolve().parents[2] / "build"
BIN = BUILD / "oracle-mariadb"


class Oracle:
    def __init__(self) -> None:
        self.stderr_lines: list[str] = []
        self.proc = subprocess.Popen(
            [str(BIN)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        assert self.proc.stdin is not None
        assert self.proc.stdout is not None
        assert self.proc.stderr is not None
        self._stderr_thread = threading.Thread(target=self._drain_stderr, daemon=True)
        self._stderr_thread.start()

    def _drain_stderr(self) -> None:
        assert self.proc.stderr is not None
        for raw in self.proc.stderr:
            line = raw.decode("utf-8", "replace").rstrip()
            self.stderr_lines.append(line)
            del self.stderr_lines[:-80]

    def close(self) -> None:
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=5)

    def _fail_context(self) -> str:
        return "\n".join(self.stderr_lines[-20:])

    def write_frame(self, msg: int, payload: bytes = b"") -> None:
        body = bytes([msg]) + payload
        assert self.proc.stdin is not None
        self.proc.stdin.write(struct.pack("<I", len(body)))
        self.proc.stdin.write(body)
        self.proc.stdin.flush()

    def read_frame(self, expected_msg: int) -> bytes:
        assert self.proc.stdout is not None
        hdr = self.proc.stdout.read(4)
        if len(hdr) != 4:
            raise AssertionError(
                f"short frame header, rc={self.proc.poll()}, stderr:\n{self._fail_context()}"
            )
        (n,) = struct.unpack("<I", hdr)
        body = self.proc.stdout.read(n)
        if len(body) != n:
            raise AssertionError(
                f"short frame body, rc={self.proc.poll()}, stderr:\n{self._fail_context()}"
            )
        if not body or body[0] != expected_msg:
            raise AssertionError(f"unexpected frame type: {body[:1]!r}")
        return body[1:]

    def hello(self) -> str:
        self.write_frame(MSG_HELLO)
        payload = self.read_frame(MSG_HELLO)
        (n,) = struct.unpack_from("<I", payload, 0)
        data = payload[4 : 4 + n]
        assert 4 + n == len(payload)
        return data.decode()

    def coverage(self) -> bytes:
        self.write_frame(MSG_GET_COVERAGE)
        payload = self.read_frame(MSG_GET_COVERAGE)
        (n,) = struct.unpack_from("<I", payload, 0)
        data = payload[4 : 4 + n]
        assert 4 + n == len(payload)
        return data

    def parse_batch(self, cases: list[tuple[int, str]]) -> tuple[list[str], list[int]]:
        payload = bytearray()
        payload += struct.pack("<I", len(cases))
        for mode, sql in cases:
            b = sql.encode()
            payload += struct.pack("<QI", mode, len(b))
            payload += b
        self.write_frame(MSG_PARSE_BATCH, bytes(payload))
        resp = self.read_frame(MSG_PARSE_BATCH)
        off = 0
        (count,) = struct.unpack_from("<I", resp, off)
        off += 4
        out: list[str] = []
        edges: list[int] = []
        for _ in range(count):
            (new_edges, n) = struct.unpack_from("<II", resp, off)
            off += 8
            edges.append(new_edges)
            out.append(resp[off : off + n].decode())
            off += n
        assert count == len(cases)
        assert off == len(resp)
        return out, edges


CASES: list[tuple[int, str, str | tuple[str, str]]] = [
    (
        0,
        "ALTER TABLE CamelCase ADD COLUMN c INT",
        '{"verdict":"accept","stmts":[{"kind":"alter_table","schema":"","table":"CamelCase","specs":[{"op":"add","cols":[{"name":"c","type_str":"int(11)","not_null":false,"params_written":[11]}],"has_position":false}]}]}',
    ),
    (
        0,
        "ALTER TABLE t RENAME TO T",
        '{"verdict":"accept","stmts":[{"kind":"alter_table","schema":"","table":"t","new_schema":"","new_table":"T","specs":[]}]}',
    ),
    (
        0,
        "ALTER TABLE t ADD COLUMN c DECIMAL(10,2) UNSIGNED NOT NULL AFTER x",
        '{"verdict":"accept","stmts":[{"kind":"alter_table","schema":"","table":"t","specs":[{"op":"add","cols":[{"name":"c","type_str":"decimal(10,2) unsigned","not_null":true,"params_written":[10,2]}],"has_position":true}]}]}',
    ),
    (
        0,
        "ALTER TABLE db1.t ADD (a int, b varchar(20)), DROP COLUMN old, RENAME COLUMN p TO q",
        '{"verdict":"accept","stmts":[{"kind":"alter_table","schema":"db1","table":"t","specs":[{"op":"add","cols":[{"name":"a","type_str":"int(11)","not_null":false,"params_written":[11]}],"has_position":false},{"op":"add","cols":[{"name":"b","type_str":"varchar(20)","not_null":false,"params_written":[20]}],"has_position":false},{"op":"rename_col","old_name":"p","new_name":"q"},{"op":"drop","old_name":"old"}]}]}',
    ),
    (
        MODE_ORACLE,
        "ALTER TABLE t ADD c VARCHAR2(10)",
        '{"verdict":"accept","stmts":[{"kind":"alter_table","schema":"","table":"t","specs":[{"op":"add","cols":[{"name":"c","type_str":"varchar(10)","not_null":false,"params_written":[10]}],"has_position":false}]}]}',
    ),
    (
        MODE_ORACLE,
        "ALTER TABLE t ADD c NUMBER",
        '{"verdict":"accept","stmts":[{"kind":"alter_table","schema":"","table":"t","specs":[{"op":"add","cols":[{"name":"c","type_str":"double","not_null":false,"params_written":null}],"has_position":false}]}]}',
    ),
    (
        0,
        "/*M!100000 ALTER TABLE t ADD c inet6 */",
        '{"verdict":"accept","stmts":[{"kind":"alter_table","schema":"","table":"t","specs":[{"op":"add","cols":[{"name":"c","type_str":"inet6","not_null":false,"params_written":null}],"has_position":false}]}]}',
    ),
    (
        0,
        "SET STATEMENT max_statement_time=0 FOR ALTER TABLE t ADD c INT",
        '{"verdict":"accept","stmts":[{"kind":"alter_table","schema":"","table":"t","specs":[{"op":"add","cols":[{"name":"c","type_str":"int(11)","not_null":false,"params_written":[11]}],"has_position":false}]}]}',
    ),
    (
        0,
        "ALTER TABLE s.t MODIFY c BIGINT UNSIGNED FIRST; SELECT 1",
        '{"verdict":"accept","stmts":[{"kind":"alter_table","schema":"s","table":"t","specs":[{"op":"modify","cols":[{"name":"c","type_str":"bigint(20) unsigned","not_null":false,"params_written":[20]}],"has_position":true}]},{"kind":"other"}]}',
    ),
    (
        0,
        "RENAME TABLE a TO b, s1.c TO s2.d",
        '{"verdict":"accept","stmts":[{"kind":"rename_table","pairs":[{"old_schema":"","old_table":"a","new_schema":"","new_table":"b"},{"old_schema":"s1","old_table":"c","new_schema":"s2","new_table":"d"}]}]}',
    ),
    (
        MODE_MSSQL,
        "ALTER TABLE [t] ADD [c] INT",
        '{"verdict":"accept","stmts":[{"kind":"alter_table","schema":"","table":"t","specs":[{"op":"add","cols":[{"name":"c","type_str":"int(11)","not_null":false,"params_written":[11]}],"has_position":false}]}]}',
    ),
    (0, "SELECT 1", '{"verdict":"accept","stmts":[{"kind":"other"}]}'),
    (0, "ALTER TABLE t GARBAGE", ("reject_prefix", "1064: ")),
    (
        0,
        "ALTER TABLE t ADD d INT",
        '{"verdict":"accept","stmts":[{"kind":"alter_table","schema":"","table":"t","specs":[{"op":"add","cols":[{"name":"d","type_str":"int(11)","not_null":false,"params_written":[11]}],"has_position":false}]}]}',
    ),
    (0, "ALTER TABLE t ADD c INT; ", ("reject_prefix", "1065: Query was empty")),
    (0, "SET STATEMENT nonexistent_var=1 FOR ALTER TABLE t ADD c INT", ("reject_prefix", "1193: ")),
]

STORM_PRE_MODE = 1050116
STORM_KILL_MODE = 1049092
STORM_PRE_SQL = (
    "/*!/*M!/*!/*M!/*M!ALTER  TABLE t PARTITION BY SYSTEM_TIME INTERVAL  "
    "LOCALTIMESTAMP LIKE ROWNUM SQL_TSI_DAY YEAR_MONTH YEAR_MONTH "
    "MICROSECONDCOLdMN_CHECKX EVERY   SUBPARTITION IN SIMPLE TO  "
    "MASTER_SSL_KEY ENGINES   ADD COLUMN `selec DIAGNOSTICS t` DEC(,2) "
    "AFTER ONLY, CHANGE COLUMN c \"c2\" LOW_PRIORITY AFTER `c`, ADD "
    "\"table\" SOUNDS BYTE COLLATE utf8mb4_bin COMMENT 'has */ marker' "
    "\"double string\" FIRST, DROP VAR_POP COLUMN `system` `system` "
    "CASCADE fk (b) REFERENCES parent(b) MATCH FULL ON DELETE CASCADE ON  "
    "SET ; RENAME TABLE a TO b, c TO d; ALTER ONLINE TABLE `\u4e16\u754c`  "
    "COLUMN IF NOT EXISTS c2 TINYBLOB NO_WRITE_TO_BINLOG NULL"
)
STORM_KILL_SQL = (
    "/*!/*M!/*!/*M!/*M!ALTER  TABLE t PARTITION BY SYSTEM_TIME INTERVAL  "
    "LOCALTIMESTAMP LIKE ROWNUM SQL_TSI_DAY AST(X'CAFE' AS DATETIME) ts "
    "STORAGE DISK AUTO_INCREMENT=42, MODIFY COLUMN period VARCHAR(32) AS AS "
    "(CONCAT((old_col), MAX_UPDATES_PER_HOUR IO_THREAD N`doubled '' quote' "
    "AND _utf8mb4'comma,value */ FOR')) DEFAULT CURRENT_TIMESTAMP(6) "
    "DEFAULT (!CURRENT_TIMESTAMP) NOT NULL ENABLE AFTER period, DROP TINYINT "
    "PARTITION p0, REMOVE PARTITIONING, ALTER COLUMN COLUMN `after`  SET "
    "DEFAULT 'comma,value', RENAME = after_col"
)


def assert_json_equal(actual: str, expected: str | tuple[str, str], sql: str) -> None:
    if isinstance(expected, tuple):
        kind, prefix = expected
        assert kind == "reject_prefix"
        obj = json.loads(actual)
        assert obj["verdict"] == "reject", (sql, actual)
        assert obj["error"].startswith(prefix), (sql, actual, prefix)
        return
    if actual != expected:
        raise AssertionError(f"mismatch for {sql!r}\nactual:   {actual}\nexpected: {expected}")


def rss_kb(pid: int) -> int:
    out = subprocess.check_output(["ps", "-o", "rss=", "-p", str(pid)], text=True)
    return int(out.strip() or "0")


def run_smoke(soak: int | None) -> None:
    if not BIN.exists():
        raise SystemExit(f"missing binary: {BIN}; run build.sh first")

    oracle = Oracle()
    try:
        hello = oracle.hello()
        assert hello == '{"engine":"mariadb","server_version":"13.1.0","protocol":2}', hello

        batch = [(mode, sql) for mode, sql, _ in CASES]
        first, first_edges = oracle.parse_batch(batch)
        for actual, (_, sql, expected) in zip(first, CASES, strict=True):
            assert_json_equal(actual, expected, sql)
        assert first_edges[0] > 0, first_edges

        cov1 = oracle.coverage()
        # deduped single __sancov_cntrs region (~379 KB), not per-TU copies
        assert len(cov1) > 100_000, len(cov1)
        assert any(cov1), "coverage bitmap is all zero"

        second, second_edges = oracle.parse_batch(batch)
        assert first == second, "batch output is not deterministic"
        # per-case attribution: a repeat of the same batch opens ~nothing new.
        # A tiny residue is real second-execution coverage (state primed by the
        # first run flips a branch), not attribution slop.
        assert sum(second_edges) <= 4, second_edges

        storm, _ = oracle.parse_batch(
            [(STORM_PRE_MODE, STORM_PRE_SQL), (STORM_KILL_MODE, STORM_KILL_SQL)]
        )
        for actual, sql in zip(storm, [STORM_PRE_SQL, STORM_KILL_SQL], strict=True):
            assert_json_equal(actual, ("reject_prefix", "4127: "), sql)

        # The guard matches keywords as whole words with arbitrary text
        # between them, so it must survive the mutator padding the gaps with
        # multi-whitespace ("PARTITION BY  SYSTEM_TIME"), an executable comment
        # ("PARTITION /*M!130100 BY SYSTEM_TIME"), or swapping the boolean
        # predicate in the interval expr (LIKE -> REGEXP) -- all reached the
        # parser and crashed the oracle before this guard.
        evasions = (
            lambda s: s.replace(" SYSTEM_TIME ", "  SYSTEM_TIME  "),
            lambda s: s.replace("PARTITION BY", "PARTITION /*M!130100 BY"),
            lambda s: s.replace(" LIKE ", " REGEXP "),
        )
        for evade in evasions:
            ev_storm, _ = oracle.parse_batch(
                [(mode, evade(sql))
                 for mode, sql in [(STORM_PRE_MODE, STORM_PRE_SQL),
                                   (STORM_KILL_MODE, STORM_KILL_SQL)]]
            )
            for actual, sql in zip(ev_storm, [STORM_PRE_SQL, STORM_KILL_SQL],
                                   strict=True):
                assert_json_equal(actual, ("reject_prefix", "4127: "), sql)

        oracle.parse_batch([(0, "ALTER TABLE t ADD c INT")])
        cov2 = oracle.coverage()
        assert len(cov2) == len(cov1)
        assert sum(cov2) >= sum(cov1)

        print(f"smoke ok: {len(batch)} cases, coverage={len(cov2)} bytes")

        if soak:
            loops = math.ceil(soak / len(batch))
            mark = max(1, loops // 10)
            start = time.time()
            rss_start = rss_kb(oracle.proc.pid)
            rss_mark = None
            sent = 0
            for i in range(loops):
                oracle.parse_batch(batch)
                sent += len(batch)
                if i + 1 == mark:
                    rss_mark = rss_kb(oracle.proc.pid)
                    print(
                        f"soak 10%: statements={sent} rss={rss_mark} KiB elapsed={time.time() - start:.1f}s"
                    )
            rss_end = rss_kb(oracle.proc.pid)
            elapsed = time.time() - start
            print(f"soak 100%: statements={sent} rss={rss_end} KiB elapsed={elapsed:.1f}s")
            base = rss_mark if rss_mark is not None else rss_start
            assert rss_end - base < 64 * 1024, (base, rss_end)
    finally:
        oracle.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--soak", type=int, default=None, nargs="?", const=1_000_000)
    args = ap.parse_args()
    run_smoke(args.soak)


if __name__ == "__main__":
    main()
