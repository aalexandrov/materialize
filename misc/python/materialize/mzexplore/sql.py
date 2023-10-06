# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import logging
import ssl
import string
import subprocess
from enum import Enum
from pathlib import Path
from typing import Any, Generator, cast

import pg8000
import sqlparse


class Dialect(Enum):
    PG = 0
    MZ = 1


class Database:
    """An API to the database under test."""

    def __init__(
        self,
        port: int,
        host: str,
        user: str,
        password: str | None,
        database: str | None,
        require_ssl: bool,
    ) -> None:
        logging.debug(f"Initialize Database with host={host} port={port}, user={user}")

        if require_ssl:
            # verify_mode=ssl.CERT_REQUIRED is the default
            ssl_context = ssl.create_default_context()
        else:
            ssl_context = None

        self.conn = pg8000.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            ssl_context=ssl_context,
        )
        self.conn.autocommit = True
        self.dialect = Dialect.MZ if "Materialize" in self.version() else Dialect.PG

    def close(self) -> None:
        self.conn.close()

    def version(self) -> str:
        result = self.query_one("SELECT version()")
        return cast(str, result[0])

    def mz_version(self) -> str | None:
        if self.dialect == Dialect.MZ:
            result = self.query_one("SELECT mz_version()")
            return cast(str, result[0])
        else:
            return None

    def execute_all(self, statements: list[str]) -> None:
        with self.conn.cursor() as cursor:
            for statement in statements:
                cursor.execute(statement)

    def query_one(self, query: str) -> dict[Any, Any]:
        with self.conn.cursor() as cursor:
            cursor.execute(query)
            return cast(dict[Any, Any], cursor.fetchone())

    def query_all(self, query: str) -> Generator[dict[Any, Any], None, None]:
        with self.conn.cursor() as cursor:
            cursor.execute(query)
            cols = [d[0] for d in cursor.description]
            for row in cursor.fetchall():
                yield {key: val for key, val in zip(cols, row)}


# Utility functions
# -----------------


def parse_from_file(path: Path) -> list[str]:
    """Parses a *.sql file to a list of queries."""
    return sqlparse.split(path.read_text())


def parse_query(path: Path) -> str:
    """Parses a *.sql file to a list of queries."""
    queries = parse_from_file(path)
    assert len(queries) == 1, f"Exactly one query expected in {path}"
    return queries[0]


def parse_template(path: Path) -> string.Template:
    return string.Template(path.read_text())


def try_mzfmt(sql: str) -> str:
    sql = sql.rstrip().rstrip(";")

    result = subprocess.run(
        ["mzfmt"],
        shell=True,
        input=sql.encode("utf-8"),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    if result.returncode == 0:
        return result.stdout.decode("utf-8").rstrip()
    else:
        return sql.rstrip().rstrip(";")
