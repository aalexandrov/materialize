# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import csv
import re
import string
import tempfile
from contextlib import closing
from pathlib import Path
from typing import cast

import click
import numpy as np
import pandas as pd

from ..mzexplore import resource_path, sql

# import logging
# logging.basicConfig(encoding='utf-8', level=logging.DEBUG)

# Typer CLI Application
# ---------------------


@click.group()
def app() -> None:
    pass


class Arg:
    repository = dict(
        type=click.Path(
            exists=True,
            file_okay=False,
            dir_okay=True,
            writable=True,
            readable=True,
            resolve_path=True,
        ),
        callback=lambda ctx, param, value: Path(value),
    )


class Opt:
    db_port = dict(
        default=6877,
        help="DB connection port.",
        envvar="PGPORT",
    )

    db_host = dict(
        default="localhost",
        help="DB connection host.",
        envvar="PGHOST",
    )

    db_user = dict(
        default="mz_support",
        help="DB connection user.",
        envvar="PGUSER",
    )

    db_pass = dict(
        default=None,
        help="DB connection password.",
        envvar="PGPASSWORD",
    )

    db_require_ssl = dict(
        is_flag=True,
        help="DB connection requires SSL.",
        envvar="PGREQUIRESSL",
    )

    mzfmt = dict(
        help="Format SQL statements with `mzfmt` if present.",
    )


@app.command("dump")
@click.argument("repository", **Arg.repository)
@click.option("--db-port", **Opt.db_port)
@click.option("--db-host", **Opt.db_host)
@click.option("--db-user", **Opt.db_user)
@click.option("--db-pass", **Opt.db_pass)
@click.option("--db-require-ssl", **Opt.db_require_ssl)
@click.option("--mzfmt/--no-mzfmt", **Opt.mzfmt)
def dump(
    repository: Path,
    db_port: int,
    db_host: str,
    db_user: str,
    db_pass: str | None,
    db_require_ssl: bool,
    mzfmt: bool,
) -> None:
    """Dump SQL statements for dataflow-backed items in the database."""

    info(f"Dumping dataflow-backed item definitions in `{repository}`")

    try:
        with closing(
            sql.Database(
                port=db_port,
                host=db_host,
                user=db_user,
                database=None,
                password=db_pass,
                require_ssl=db_require_ssl,
            )
        ) as db:
            # Dump regular views
            q = sql.parse_query(resource_path("catalog/views.sql"))
            t = sql.parse_template(resource_path("catalog/views.tpl"))
            for i in db.query_all(q):
                # execute template
                stmt = t.substitute(i)
                # format statement (optional)
                stmt = sql.try_mzfmt(stmt) if mzfmt else stmt

                base = repository.joinpath(i["database"], i["schema"])
                base.mkdir(parents=True, exist_ok=True)
                path = base.joinpath(f"{i['name']}.view.sql")

                # print statement
                info(f"writing `{path}`")
                with path.open("w") as file:
                    file.write(f"-- id: {i['id']}\n")
                    file.write(f"-- oid: {i['oid']}\n")
                    file.write(stmt)

            # Dump indexes
            q = sql.parse_query(resource_path("catalog/indexes.sql"))
            t = sql.parse_template(resource_path("catalog/indexes.tpl"))
            for i in db.query_all(q):
                if len(i["key"]) > 0:
                    i["idx_cols"] = "(" + ", ".join((f'"{k}"' for k in i["key"])) + ")"
                    i["idx_type"] = "INDEX"
                else:
                    i["idx_type"] = "DEFAULT INDEX"

                # execute template
                stmt = t.substitute(i)
                # format statement (optional)
                stmt = sql.try_mzfmt(stmt) if mzfmt else stmt

                base = repository.joinpath(i["database"], i["schema"])
                base.mkdir(parents=True, exist_ok=True)
                path = base.joinpath(f"{i['on_name']}.{i['name']}.idx.sql")

                # print statement
                info(f"writing `{path}`")
                with path.open("w") as file:
                    file.write(f"-- id: {i['id']}\n")
                    file.write(f"-- oid: {i['oid']}\n")
                    file.write(stmt)

            # Dump materialized views
            q = sql.parse_query(resource_path("catalog/materialized_views.sql"))
            t = sql.parse_template(resource_path("catalog/materialized_views.tpl"))
            for i in db.query_all(q):
                # execute template
                stmt = t.substitute(i)
                # format statement (optional)
                stmt = sql.try_mzfmt(stmt) if mzfmt else stmt

                base = repository.joinpath(i["database"], i["schema"])
                base.mkdir(parents=True, exist_ok=True)
                path = base.joinpath(f"{i['name']}.mview.sql")

                # print statement
                info(f"writing `{path}`")
                with path.open("w") as file:
                    file.write(f"-- id: {i['id']}\n")
                    file.write(f"-- oid: {i['oid']}\n")
                    file.write(stmt)

    except Exception as e:
        raise click.ClickException(f"run command failed: {e}")


# Utility methods
# ---------------


def print_df(df: pd.DataFrame) -> None:
    with pd.option_context("display.max_rows", None, "display.max_columns", None):
        print(df)


def info(msg: str, fg: str = "green") -> None:
    click.secho(msg, fg=fg)


def err(msg: str, fg: str = "red") -> None:
    click.secho(msg, fg=fg, err=True)


if __name__ == "__main__":
    app()
