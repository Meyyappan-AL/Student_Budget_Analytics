"""Stage 6 - PostgreSQL access layer for the Student Budget Analytics pipeline.

Thin, explicit wrapper around ``psycopg2``. It owns four things and nothing else:

1. Resolving a connection (an explicit DSN, ``DATABASE_URL``, or the standard
   ``PG*`` environment variables that libpq reads on its own).
2. Applying ``sql/schema.sql``.
3. Bulk-loading a CSV with ``COPY``.
4. Verifying the load against the seed data's known ranges.

Everything here is deliberately *not* an ORM. The workload is "run a DDL file,
bulk copy 213 rows, run four verification queries", and an ORM would add a
dependency and a layer of indirection without removing a single line.

``psycopg2`` is imported lazily so that the pure-Python parts of the pipeline
(CSV validation, SQL generation) remain runnable - and testable - on a machine
with no database driver installed.

Usage::

    python src/database.py --dsn postgresql://user@localhost:5432/student_budget
"""

from __future__ import annotations

import os
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[1]

SCHEMA_PATH = PROJECT_ROOT / "sql" / "schema.sql"
TARGET_TABLE = "students_budget"

#: Environment variable consulted for a libpq connection string, ahead of the
#: individual ``PG*`` variables. Both are standard; ``DATABASE_URL`` is listed
#: first because it is the more common convention for application code and it is
#: what a ``.env`` file or a CI secret would most likely hold.
DSN_ENV_VAR = "DATABASE_URL"

_INSTALL_HINT = (
    "psycopg2 is required for database access. Install it with:\n"
    "    pip install -r requirements.txt\n"
    "or:\n"
    "    pip install psycopg2-binary"
)


def _require_psycopg2():
    """Import and return ``psycopg2``, or explain how to install it.

    Imported at call time rather than module scope so the module can be imported
    (and its constants imported by the loader) without a driver present.
    """
    try:
        import psycopg2
    except ImportError as exc:  # pragma: no cover - environment dependent
        raise ImportError(_INSTALL_HINT) from exc
    return psycopg2


# --------------------------------------------------------------------------- #
# Connection
# --------------------------------------------------------------------------- #


def resolve_dsn(explicit: Optional[str] = None) -> Optional[str]:
    """Return the connection string to use, or ``None`` to defer to libpq.

    Resolution order:

    1. ``explicit`` - the ``--dsn`` flag, which always wins.
    2. The ``DATABASE_URL`` environment variable.
    3. ``None`` - ``psycopg2.connect()`` with no argument, which makes libpq fall
       back to the standard ``PGHOST``/``PGPORT``/``PGDATABASE``/``PGUSER``/
       ``PGPASSWORD`` variables and finally to the operating system's default
       connection.

    No credential is ever hard-coded or logged. Callers that print connection
    information should print :func:`describe_dsn`, never the DSN itself.
    """
    if explicit:
        return explicit
    return os.environ.get(DSN_ENV_VAR) or None


def describe_dsn(dsn: Optional[str] = None) -> str:
    """Return a redacted, human-readable description of the target connection.

    Safe to print. The password is replaced with ``***`` and the username is
    reduced to its first character, so a DSN echoed into a log or a transcript
    of a pipeline run cannot leak a secret.
    """
    target = resolve_dsn(dsn)
    if not target:
        return "libpq defaults (PG* environment variables, else OS default)"

    scheme, _, rest = target.partition("://")
    if not rest:
        return f"{scheme}://<redacted>"

    credentials, _, location = rest.rpartition("@")
    if not credentials:
        return f"{scheme}://{location}"

    user, _, _password = credentials.partition(":")
    initial = f"{user[:1]}***" if user else "***"
    return f"{scheme}://{initial}@{location}"


@contextmanager
def connect(dsn: Optional[str] = None) -> Iterator[Any]:
    """Open a connection, commit on success, roll back on error, always close.

    Yields a connection in ``autocommit`` mode.

    Autocommit is required rather than merely convenient: ``sql/schema.sql``
    wraps its own DDL in an explicit ``BEGIN;``/``COMMIT;``, and under
    psycopg2's default implicit transaction that would raise
    ``BEGIN`` inside a transaction without a corresponding block. Leaving the
    transaction to the script keeps one transaction boundary in the file itself,
    which is also what makes the DDL atomic in PostgreSQL.
    """
    psycopg2 = _require_psycopg2()
    connection = psycopg2.connect(resolve_dsn(dsn))
    connection.autocommit = True
    try:
        yield connection
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


# --------------------------------------------------------------------------- #
# Schema
# --------------------------------------------------------------------------- #


def read_schema(path: Optional[Path] = None) -> str:
    """Return the DDL script text.

    Parameters
    ----------
    path:
        Schema file to read. Defaults to ``sql/schema.sql``.

    Raises
    ------
    FileNotFoundError
        If the schema file is absent.
    """
    source = Path(path) if path is not None else SCHEMA_PATH
    if not source.is_file():
        raise FileNotFoundError(f"Schema file not found: {source}")
    return source.read_text(encoding="utf-8")


def schema_drops_table(script: Optional[str] = None) -> bool:
    """Report whether the script contains a ``DROP TABLE``.

    ``sql/schema.sql`` starts with ``DROP TABLE IF EXISTS students_budget;``, so
    re-running it destroys any loaded data. The pipeline checks this before
    applying the schema so the destructive step cannot happen unnoticed.
    """
    text = script if script is not None else read_schema()
    return "drop table" in text.lower()


def apply_schema(connection: Any, path: Optional[Path] = None) -> int:
    """Execute the schema script against an open connection.

    The whole file is sent as one ``execute()`` call so the script controls its
    own ``BEGIN``/``COMMIT`` and the DDL remains atomic. Splitting the file on
    semicolons would be wrong here: it would also split the semicolons inside
    the ``COMMENT ON`` string literals.

    Returns the number of characters executed, which is only ever used for
    reporting.
    """
    script = read_schema(path)
    with connection.cursor() as cursor:
        cursor.execute(script)
    return len(script)


# --------------------------------------------------------------------------- #
# Bulk load
# --------------------------------------------------------------------------- #


def copy_csv(connection: Any, table: str, csv_path: Path) -> int:
    """Stream a CSV file into an existing table with ``COPY``.

    ``COPY ... FROM STDIN`` is used rather than row-by-row ``INSERT`` because it
    is a single round trip and is the only practical way to load a file into
    PostgreSQL from Python without relying on a superuser server-side file read.

    The column list is emitted explicitly, derived from the table's own
    ``information_schema`` metadata, so the load keeps working if the staging
    table gains a column. Without it, ``COPY`` would map positionally and a
    reordered CSV header would silently populate the wrong columns.

    Parameters
    ----------
    connection:
        An open ``psycopg2`` connection.
    table:
        Destination table name. It must already exist.
    csv_path:
        CSV to load. Must include a header row.

    Returns
    -------
    int
        The number of rows reported copied by PostgreSQL.
    """
    source = Path(csv_path)
    if not source.is_file():
        raise FileNotFoundError(f"CSV to load not found: {source}")

    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = ANY (current_schemas(false))
              AND table_name = %s
            ORDER BY ordinal_position
            """,
            (table,),
        )
        columns: List[str] = [row[0] for row in cursor.fetchall()]
        if not columns:
            raise ValueError(f"Destination table {table!r} does not exist or has no columns.")

        column_list = ", ".join(_quote_identifier(name) for name in columns)
        statement = f"COPY {_quote_identifier(table)} ({column_list}) FROM STDIN WITH (FORMAT csv, HEADER true)"

        with source.open("r", encoding="utf-8", newline="") as handle:
            cursor.copy_expert(statement, handle)
        return cursor.rowcount


def _quote_identifier(name: str) -> str:
    """Quote a SQL identifier, doubling any embedded double quotes.

    Needed for the staging table's headers such as ``work/internship`` and
    ``budget usage %``, which are not valid bare identifiers.
    """
    return '"' + name.replace('"', '""') + '"'


# --------------------------------------------------------------------------- #
# Queries
# --------------------------------------------------------------------------- #


def fetch_all(connection: Any, sql: str, params: Optional[Sequence[Any]] = None) -> List[Tuple[Any, ...]]:
    """Run a query and return every row as a tuple list."""
    with connection.cursor() as cursor:
        cursor.execute(sql, params)
        return list(cursor.fetchall())


def fetch_one(connection: Any, sql: str, params: Optional[Sequence[Any]] = None) -> Optional[Tuple[Any, ...]]:
    """Run a query and return its first row, or ``None`` if it returned nothing."""
    with connection.cursor() as cursor:
        cursor.execute(sql, params)
        return cursor.fetchone()


def table_exists(connection: Any, table: str = TARGET_TABLE) -> bool:
    """Report whether the target table is present in the current schema."""
    row = fetch_one(
        connection,
        """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = ANY (current_schemas(false))
              AND table_name = %s
        )
        """,
        (table,),
    )
    return bool(row and row[0])


def table_row_count(connection: Any, table: str = TARGET_TABLE) -> int:
    """Return the exact row count of the target table."""
    row = fetch_one(connection, f"SELECT count(*) FROM {_quote_identifier(table)}")
    return int(row[0]) if row else 0


# --------------------------------------------------------------------------- #
# Verification
# --------------------------------------------------------------------------- #

#: Row count of the seed dataset. Checked because a silently partial COPY is the
#: most likely failure mode of this stage, and it is invisible without a count.
EXPECTED_ROW_COUNT = 213

#: Seed-data ranges used to prove the columns were mapped to the right CSV
#: headers. A mapping slip (say, ``food`` and ``transport`` transposed) would
#: leave every constraint satisfied and every aggregate plausible, but would
#: change these bounds, so they act as a mapping checksum.
EXPECTED_USAGE_RANGE = (125.0, 8600.0)
EXPECTED_SAVINGS_RANGE = (-24000.0, -250.0)
EXPECTED_TOTAL_RANGE = (1500.0, 25000.0)


def verification_checks() -> List[Dict[str, Any]]:
    """Return the post-load checks as data, so they can be reported or tested.

    Each entry is a name, the SQL to run, and a callable that turns the first
    row into a pass/fail plus a message. Keeping them as data rather than
    inline prints means :func:`verify_load` is reusable by a test.
    """
    return [
        {
            "name": "row_count",
            "sql": f"SELECT count(*) FROM {_quote_identifier(TARGET_TABLE)}",
            "check": lambda row: (row[0] == EXPECTED_ROW_COUNT, f"{row[0]} rows (expected {EXPECTED_ROW_COUNT})"),
        },
        {
            "name": "budget_usage_pct_range",
            "sql": f"SELECT min(budget_usage_pct), max(budget_usage_pct) FROM {_quote_identifier(TARGET_TABLE)}",
            "check": lambda row: (
                (float(row[0]), float(row[1])) == EXPECTED_USAGE_RANGE,
                f"{row[0]} .. {row[1]} (expected {EXPECTED_USAGE_RANGE[0]} .. {EXPECTED_USAGE_RANGE[1]})",
            ),
        },
        {
            "name": "savings_range",
            "sql": f"SELECT min(savings), max(savings) FROM {_quote_identifier(TARGET_TABLE)}",
            "check": lambda row: (
                (float(row[0]), float(row[1])) == EXPECTED_SAVINGS_RANGE,
                f"{row[0]} .. {row[1]} (expected {EXPECTED_SAVINGS_RANGE[0]} .. {EXPECTED_SAVINGS_RANGE[1]})",
            ),
        },
        {
            "name": "total_expenses_range",
            "sql": f"SELECT min(total_expenses), max(total_expenses) FROM {_quote_identifier(TARGET_TABLE)}",
            "check": lambda row: (
                (float(row[0]), float(row[1])) == EXPECTED_TOTAL_RANGE,
                f"{row[0]} .. {row[1]} (expected {EXPECTED_TOTAL_RANGE[0]} .. {EXPECTED_TOTAL_RANGE[1]})",
            ),
        },
        {
            "name": "no_positive_savings",
            "sql": f"SELECT count(*) FROM {_quote_identifier(TARGET_TABLE)} WHERE savings >= 0",
            "check": lambda row: (row[0] == 0, f"{row[0]} row(s) with savings >= 0 (expected 0)"),
        },
        {
            "name": "generated_columns_consistent",
            "sql": (
                f"SELECT count(*) FROM {_quote_identifier(TARGET_TABLE)} "
                "WHERE total_expenses <> food_expenses + transport_expenses "
                "+ academic_expenses + other_expenses "
                "OR savings <> allowance - total_expenses"
            ),
            "check": lambda row: (
                row[0] == 0,
                f"{row[0]} row(s) disagree with the GENERATED expressions (expected 0)",
            ),
        },
    ]


def verify_load(connection: Any, table: str = TARGET_TABLE) -> List[Dict[str, Any]]:
    """Run every post-load check and return a result per check.

    Returns
    -------
    list of dict
        Each with ``name``, ``ok``, ``detail`` and ``sql`` keys. Nothing is
        raised on a failed check: a full report is more useful than the first
        exception, and the caller decides the exit code.
    """
    results: List[Dict[str, Any]] = []
    for check in verification_checks():
        sql = check["sql"] if table == TARGET_TABLE else check["sql"].replace(TARGET_TABLE, _quote_identifier(table))
        row = fetch_one(connection, sql)
        if row is None:
            results.append(
                {"name": check["name"], "ok": False, "detail": "query returned no rows", "sql": sql}
            )
            continue
        try:
            ok, detail = check["check"](row)
        except Exception as exc:  # pragma: no cover - defensive
            ok, detail = False, f"could not evaluate: {type(exc).__name__}: {exc}"
        results.append({"name": check["name"], "ok": bool(ok), "detail": detail, "sql": sql})
    return results


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Apply the schema to a database and report the result.

    This is a thin convenience wrapper for manual use. The pipeline calls
    :func:`apply_schema` and :func:`verify_load` directly.
    """
    import argparse

    parser = argparse.ArgumentParser(description="Apply and verify the Stage 5 schema.")
    parser.add_argument("--dsn", default=None, help="Connection string. Defaults to $DATABASE_URL then libpq PG* variables.")
    parser.add_argument("--schema", type=Path, default=None, help="Schema file. Defaults to sql/schema.sql.")
    parser.add_argument("--verify-only", action="store_true", help="Skip the DDL and only run the checks.")
    arguments = parser.parse_args(argv)

    print(f"[stage 5] target      : {describe_dsn(arguments.dsn)}")

    try:
        with connect(arguments.dsn) as connection:
            if not arguments.verify_only:
                script = read_schema(arguments.schema)
                if schema_drops_table(script):
                    print("[stage 5] WARNING    : schema.sql contains DROP TABLE; existing data will be destroyed")
                applied = apply_schema(connection, arguments.schema)
                print(f"[stage 5] applied    : {applied} characters of DDL from {arguments.schema or SCHEMA_PATH}")

            results = verify_load(connection)
            failures = 0
            for result in results:
                status = "PASS" if result["ok"] else "FAIL"
                failures += 0 if result["ok"] else 1
                print(f"[stage 5] {status}      : {result['name']}: {result['detail']}")
            return 1 if failures else 0
    except Exception as exc:
        print(f"[stage 5] ERROR      : {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
