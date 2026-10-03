"""Stage 6 - Load the transformed dataset into PostgreSQL. 

The Stage 4 CSV uses short Title Case headers for presentation (``Food``,
``Budget Usage %``, ``Work/Internship``). Those are not valid bare SQL
identifiers, and two of them (``total_expenses``, ``savings``) are ``GENERATED``
columns in the target table that PostgreSQL will refuse an explicit value for.
So the load is a three-step *staged* copy rather than a direct insert:

1. ``CREATE TEMP TABLE`` whose columns are named after the CSV headers verbatim
   and all typed permissively (``TEXT``/``NUMERIC``).
2. ``COPY`` the file into that staging table. ``COPY`` is positional, which is
   safe *only* because the staging table is generated from the same column list
   as the CSV, so the two cannot drift.
3. ``INSERT ... SELECT`` the staging rows into ``students_budget``, renaming the
   headers to snake_case, dropping the generated columns, and pinning the naive
   survey timestamps to ``Asia/Kolkata`` on the way in.

The staging table is what lets the CSV keep its presentation headers and the
table keep sane identifiers at the same time, with the mapping written down in
one place (:data:`CSV_TO_COLUMN`) instead of spread across the SQL.

The load is also *validated before it is attempted*. Every ``CHECK`` constraint
in the schema is mirrored in :func:`validate_source_frame`, so a bad row produces
a clear local error naming the offending values instead of an opaque
``psycopg2.errors.CheckViolation`` halfway through a COPY. The checks are
duplicated on purpose: the database is the last line of defence, not the first
line of diagnosis.

Usage::

    python src/data_loader.py --dsn postgresql://user@localhost:5432/student_budget
    python src/data_loader.py --dry-run
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]

PROCESSED_DATA_PATH = PROJECT_ROOT / "data" / "processed" / "transformed_student_budget.csv"

TARGET_TABLE = "students_budget"
STAGING_TABLE = "stage_6_transformed"

#: The survey export carries naive local timestamps. PostgreSQL would otherwise
#: read them as UTC and shift every row by 5:30.
SOURCE_TIMEZONE = "Asia/Kolkata"

# --------------------------------------------------------------------------- #
# Column mapping
# --------------------------------------------------------------------------- #

#: Stage 4 CSV headers, in file order. This is the contract with
#: ``transformer.DISPLAY_NAMES``; :func:`validate_source_frame` checks the two
#: agree, and :func:`staging_table_sql` derives the staging DDL from this list so
#: the staging table always matches the file positionally.
CSV_COLUMNS: Tuple[str, ...] = (
    "Timestamp",
    "Student Name",
    "Gender",
    "Degree",
    "Year of Study",
    "Accommodation",
    "Background",
    "Work/Internship",
    "Specialization",
    "Food",
    "Transport",
    "Academic",
    "Other",
    "Total Expenses",
    "Allowance",
    "Savings",
    "Budget Usage %",
    "Preferred Payment",
    "UPI App",
    "Track Digitally",
    "Budget Plan",
    "Study Focus",
)

#: CSV header -> ``students_budget`` column. ``submitted_at`` is renamed because
#: ``timestamp`` is a type name in PostgreSQL and reads badly as a column.
CSV_TO_COLUMN: Dict[str, str] = {
    "Timestamp": "submitted_at",
    "Student Name": "student_name",
    "Gender": "gender",
    "Degree": "degree",
    "Year of Study": "year_of_study",
    "Accommodation": "accommodation",
    "Background": "background",
    "Work/Internship": "work_internship",
    "Specialization": "specialization",
    "Food": "food_expenses",
    "Transport": "transport_expenses",
    "Academic": "academic_expenses",
    "Other": "other_expenses",
    "Total Expenses": "total_expenses",
    "Allowance": "allowance",
    "Savings": "savings",
    "Budget Usage %": "budget_usage_pct",
    "Preferred Payment": "preferred_payment",
    "UPI App": "upi_app",
    "Track Digitally": "track_digitally",
    "Budget Plan": "budget_plan",
    "Study Focus": "study_focus",
}

#: Computed by the database. PostgreSQL raises
#: ``cannot insert a non-DEFAULT value into column "total_expenses"`` if a
#: generated column appears in the INSERT list, so these are staged and then
#: deliberately dropped. They are still copied into the staging table so the
#: positional COPY sees all 22 headers, and so
#: :func:`validate_source_frame` can cross-check the pipeline's arithmetic
#: against the database's.
GENERATED_COLUMNS: Tuple[str, ...] = ("total_expenses", "savings")

#: Staging columns carrying numbers; everything else is staged as ``TEXT`` and
#: cast by the target column on insert. The permissive staging types mean a
#: malformed number fails on INSERT with a column-qualified error rather than
#: silently landing as text in a ``TEXT`` column.
NUMERIC_CSV_COLUMNS: Tuple[str, ...] = (
    "Food",
    "Transport",
    "Academic",
    "Other",
    "Total Expenses",
    "Allowance",
    "Savings",
    "Budget Usage %",
)

#: Money headers that map to a target column named in :data:`GENERATED_COLUMNS`.
GENERATED_SOURCE_HEADERS: Tuple[str, ...] = ("Total Expenses", "Savings")

#: Target column order for the INSERT. Matches the CREATE TABLE in
#: ``sql/schema.sql`` so the generated columns sit in their natural position
#: among the others when the statement is read.
INSERT_COLUMNS: Tuple[str, ...] = (
    "submitted_at",
    "student_name",
    "gender",
    "degree",
    "year_of_study",
    "accommodation",
    "background",
    "work_internship",
    "specialization",
    "food_expenses",
    "transport_expenses",
    "academic_expenses",
    "other_expenses",
    "allowance",
    "budget_usage_pct",
    "preferred_payment",
    "upi_app",
    "track_digitally",
    "budget_plan",
    "study_focus",
)

# --------------------------------------------------------------------------- #
# Validation mirrors of the schema constraints
# --------------------------------------------------------------------------- #

#: Closed categorical domains. These must match the ``ck_students_budget_*``
#: CHECK constraints in ``sql/schema.sql`` exactly. The two can be verified
#: against each other without a database, which is how they are kept in step.
CATEGORICAL_DOMAINS: Dict[str, Tuple[str, ...]] = {
    "Gender": ("Male", "Female"),
    "Degree": ("B.Com", "BE / B.Tech", "BSc", "MBA", "ME / M.Tech", "Other"),
    "Year of Study": ("1st Year", "2nd Year", "3rd Year", "4th Year"),
    "Accommodation": ("Day Scholar", "Hostel", "PG"),
    "Background": ("Rural", "Urban"),
    "Work/Internship": ("Yes", "No"),
    "Track Digitally": ("Yes", "No"),
    "Budget Plan": ("Yes", "No"),
    "Preferred Payment": ("UPI", "Cash", "Debit/Credit Card", "Other"),
    "UPI App": (
        "GPay",
        "PhonePe",
        "SBI Pay",
        "Amazon Pay",
        "BHIM UPI",
        "Airtel Thanks App",
        "Other",
        "Not Applicable",
    ),
}

#: Money headers that must be non-negative, mirroring the five
#: ``*_non_negative`` CHECK constraints.
NON_NEGATIVE_COLUMNS: Tuple[str, ...] = ("Food", "Transport", "Academic", "Other", "Allowance")

#: Mirrors ``ck_students_budget_usage_range``. The ceiling of 10000 is the
#: arithmetic maximum of the survey's own bucket scheme (25000 / 250 * 100), not
#: a round number: budget usage is a ratio, and every respondent exceeds 100%.
BUDGET_USAGE_MIN = 0.0
BUDGET_USAGE_MAX = 10000.0

#: Mirrors ``ck_students_budget_name_not_blank``.
NAME_COLUMN = "Student Name"

#: Every target column is ``NOT NULL``, so no cell may be missing.
NULLABLE_COLUMNS: Tuple[str, ...] = ()


# --------------------------------------------------------------------------- #
# SQL generation
# --------------------------------------------------------------------------- #


def _quote(name: str) -> str:
    """Double-quote an SQL identifier, escaping any embedded double quotes."""
    return '"' + name.replace('"', '""') + '"'


def staging_table_sql() -> str:
    """Return the ``CREATE TEMP TABLE`` statement for the staging layer.

    Built from :data:`CSV_COLUMNS`, so the staging column order is by
    construction the CSV's column order and the positional ``COPY`` is correct.
    """
    lines = []
    for header in CSV_COLUMNS:
        column_type = "NUMERIC" if header in NUMERIC_CSV_COLUMNS else "TEXT"
        lines.append(f"    {_quote(header):24}{column_type},")
    body = "\n".join(lines)
    return f"CREATE TEMP TABLE {_quote(STAGING_TABLE)} (\n{body}\n) ON COMMIT DROP;"


def _select_expression(header: str) -> str:
    """Return the SELECT expression that maps one CSV header to its target column.

    Only the timestamp needs a cast. It is pinned to :data:`SOURCE_TIMEZONE`
    because the export is naive local time; without this every row would be
    stored as if it were UTC and read back 5:30 early.
    """
    if header == "Timestamp":
        return f"({_quote(header)}::timestamp AT TIME ZONE '{SOURCE_TIMEZONE}')"
    return _quote(header)


def insert_sql() -> str:
    """Return the ``INSERT ... SELECT`` that moves staged rows into the table.

    The target column list is :data:`INSERT_COLUMNS`, which excludes the
    generated columns, so PostgreSQL computes ``total_expenses`` and ``savings``
    itself and they cannot drift from their components.
    """
    headers_by_column = {column: header for header, column in CSV_TO_COLUMN.items()}
    unknown = [column for column in INSERT_COLUMNS if column not in headers_by_column]
    if unknown:
        raise KeyError(f"INSERT_COLUMNS has no CSV header mapping: {unknown}")

    targets = ",\n    ".join(_quote(column) for column in INSERT_COLUMNS)
    sources = ",\n    ".join(
        _select_expression(headers_by_column[column]) for column in INSERT_COLUMNS
    )
    return (
        f"INSERT INTO {_quote(TARGET_TABLE)} (\n    {targets})\n"
        f"SELECT\n    {sources}\n"
        f"FROM {_quote(STAGING_TABLE)};"
    )


def drop_staging_sql() -> str:
    """Return the ``DROP TABLE`` for the staging layer, ignoring absence."""
    return f"DROP TABLE IF EXISTS {_quote(STAGING_TABLE)};"


# --------------------------------------------------------------------------- #
# Source validation
# --------------------------------------------------------------------------- #


def resolve_source_path(path: Optional[Path] = None) -> Path:
    """Return the Stage 4 CSV to load, preferring an explicit path.

    ``data/processed/`` is the pipeline's canonical location but the repository
    currently tracks ``Data/processed/``, so the capitalised variant is probed
    too. This mirrors ``cleaner.resolve_raw_data_path``.
    """
    candidates = (
        (Path(path),)
        if path is not None
        else (
            PROCESSED_DATA_PATH,
            PROJECT_ROOT / "Data" / "processed" / "transformed_student_budget.csv",
        )
    )
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    probed = "\n  - ".join(str(candidate) for candidate in candidates)
    raise FileNotFoundError(f"Could not locate the Stage 4 output. Probed:\n  - {probed}")


def read_source_csv(path: Optional[Path] = None) -> pd.DataFrame:
    """Read the Stage 4 CSV as strings for every column.

    Every column is read as ``str`` deliberately. Letting pandas infer types
    would silently turn an unexpected value into ``NaN`` before validation can
    see it, and a missing categorical value must be reported as a bad value, not
    as a null that quietly satisfies ``IS NOT NULL``.
    """
    source = resolve_source_path(path)
    return pd.read_csv(source, dtype=str, keep_default_na=False)


def validate_source_frame(df: pd.DataFrame) -> List[str]:
    """Check a source frame against everything the target table will enforce.

    Runs every rule locally so a malformed row is reported with its offending
    values before any data is sent to the server.

    Parameters
    ----------
    df:
        The Stage 4 CSV, as read by :func:`read_source_csv`.

    Returns
    -------
    list of str
        One message per problem found, empty when the frame is loadable.
    """
    problems: List[str] = []

    missing = [column for column in CSV_COLUMNS if column not in df.columns]
    if missing:
        problems.append(f"missing columns: {missing}")
        return problems

    extra = [column for column in df.columns if column not in CSV_COLUMNS]
    if extra:
        problems.append(
            f"unexpected columns: {extra}. The staging COPY is positional, so an "
            f"extra column would shift every value into the wrong target column."
        )

    reordered = [column for column in df.columns if column in CSV_COLUMNS]
    if reordered != list(CSV_COLUMNS):
        problems.append(
            "column order does not match the expected Stage 4 layout: "
            f"expected {list(CSV_COLUMNS)}, found {reordered}"
        )

    if df.empty:
        problems.append("source frame has no rows")
        return problems

    for column in CSV_COLUMNS:
        blanks = df.loc[df[column].astype(str).str.strip().eq(""), column]
        if not blanks.empty and column not in NULLABLE_COLUMNS:
            shown = sorted(blanks.unique())[:5]
            problems.append(f"{column!r} has {len(blanks)} blank value(s), e.g. {shown}")

    for column, allowed in CATEGORICAL_DOMAINS.items():
        if column not in df.columns:
            continue
        offenders = sorted(set(df[column]) - set(allowed))
        if offenders:
            problems.append(
                f"{column!r} has {len(offenders)} value(s) outside its CHECK domain: {offenders[:5]}. "
                f"Allowed: {list(allowed)}"
            )

    for column in NON_NEGATIVE_COLUMNS:
        if column not in df.columns:
            continue
        values = pd.to_numeric(df[column], errors="coerce")
        unparseable = df.loc[values.isna() & df[column].str.strip().ne(""), column]
        if not unparseable.empty:
            problems.append(f"{column!r} has non-numeric value(s): {sorted(unparseable.unique())[:5]}")
            continue
        negative = df.loc[values < 0, column]
        if not negative.empty:
            problems.append(f"{column!r} has {len(negative)} negative value(s) (CHECK requires >= 0)")

    usage = pd.to_numeric(df["Budget Usage %"], errors="coerce")
    if usage.isna().any():
        problems.append(f"'Budget Usage %' has {int(usage.isna().sum())} non-numeric value(s)")
    else:
        out_of_range = usage[(usage < BUDGET_USAGE_MIN) | (usage > BUDGET_USAGE_MAX)]
        if not out_of_range.empty:
            problems.append(
                f"'Budget Usage %' has {len(out_of_range)} value(s) outside "
                f"[{BUDGET_USAGE_MIN}, {BUDGET_USAGE_MAX}] (CHECK): "
                f"{sorted(out_of_range.unique())[:5]}"
            )

    if NAME_COLUMN in df.columns:
        blank_names = df.loc[df[NAME_COLUMN].astype(str).str.strip().eq(""), NAME_COLUMN]
        if not blank_names.empty:
            problems.append(f"{NAME_COLUMN!r} has {len(blank_names)} blank value(s)")

    problems.extend(_check_derived_arithmetic(df))
    return problems


def _check_derived_arithmetic(df: pd.DataFrame) -> List[str]:
    """Cross-check the CSV's derived columns against their own components.

    The database recomputes ``total_expenses`` and ``savings`` on insert and
    ignores whatever the CSV says, so a disagreement here will not surface as an
    error - the stored values will simply differ from the file. Comparing before
    the load turns that silent difference into an explicit one, and it also
    catches the case where the schema's generated expression and the pipeline's
    definition have drifted apart.
    """
    problems: List[str] = []
    components = ["Food", "Transport", "Academic", "Other"]
    if not all(column in df.columns for column in components + ["Allowance", "Total Expenses", "Savings"]):
        return problems

    numeric = {column: pd.to_numeric(df[column], errors="coerce") for column in components + ["Allowance"]}
    if any(values.isna().any() for values in numeric.values()):
        return problems

    expected_total = sum(numeric[column] for column in components)
    expected_savings = numeric["Allowance"] - expected_total

    for label, actual_column, expected in (
        ("Total Expenses", "Total Expenses", expected_total),
        ("Savings", "Savings", expected_savings),
    ):
        if actual_column not in df.columns:
            continue
        actual = pd.to_numeric(df[actual_column], errors="coerce")
        mismatch = (actual - expected).abs() > 0.005
        if mismatch.any():
            examples = df.loc[mismatch, [actual_column]].head(3)
            problems.append(
                f"{label!r} disagrees with its components in {int(mismatch.sum())} row(s); "
                f"the database will recompute it, so the stored value will differ from the CSV. "
                f"Examples: {examples.to_dict('records')}"
            )
    return problems


# --------------------------------------------------------------------------- #
# Load
# --------------------------------------------------------------------------- #


def load_csv(
    connection: Any,
    path: Optional[Path] = None,
    validate: bool = True,
) -> int:
    """Stage and load the Stage 4 CSV into ``students_budget``.

    Parameters
    ----------
    connection:
        An open ``psycopg2`` connection in autocommit mode.
    path:
        CSV to load. Defaults to the Stage 4 output.
    validate:
        Run :func:`validate_source_frame` first. On by default; disabling it
        trades a clear local error for a server-side one.

    Returns
    -------
    int
        Number of rows inserted.

    Raises
    ------
    ValueError
        If validation fails, or if the target table does not exist.
    """
    source = resolve_source_path(path)
    if validate:
        frame = read_source_csv(source)
        problems = validate_source_frame(frame)
        if problems:
            listing = "\n  - ".join(problems)
            raise ValueError(f"{source.name} failed pre-load validation:\n  - {listing}")

    # Sibling module. The repository has no src/__init__.py, so the import works
    # both when this file is run as a script (its own directory is on the path)
    # and when it is imported as part of a package.
    try:
        from database import table_exists
    except ImportError:  # pragma: no cover - depends on invocation style
        from src.database import table_exists

    if not table_exists(connection, TARGET_TABLE):
        raise ValueError(
            f"Target table {TARGET_TABLE!r} does not exist. Run Stage 5 "
            f"(python src/database.py) to apply sql/schema.sql first."
        )

    staging = staging_table_sql()
    statement = insert_sql()

    with connection.cursor() as cursor:
        cursor.execute(drop_staging_sql())
        cursor.execute(staging)
        with source.open("r", encoding="utf-8", newline="") as handle:
            # Positional COPY: correct because staging_table_sql() derives its
            # column order from the same CSV_COLUMNS the file was written with.
            cursor.copy_expert(
                f"COPY {_quote(STAGING_TABLE)} FROM STDIN WITH (FORMAT csv, HEADER true)",
                handle,
            )
            staged = cursor.rowcount
            cursor.execute(statement)
            inserted = cursor.rowcount

    if staged != inserted:
        raise ValueError(f"staged {staged} row(s) but inserted {inserted} - the two must match")
    return inserted


def load_dataframe(
    connection: Any,
    df: pd.DataFrame,
    validate: bool = True,
) -> int:
    """Load an in-memory Stage 4 frame rather than the CSV on disk.

    Equivalent to :func:`load_csv` but writes the frame to a temporary file
    first, because ``COPY FROM STDIN`` takes a byte stream. Useful when a caller
    has already produced the frame in memory - for example a test, or a
    pipeline run that chose not to persist the CSV.
    """
    import tempfile

    if validate:
        problems = validate_source_frame(df.astype(str))
        if problems:
            listing = "\n  - ".join(problems)
            raise ValueError(f"frame failed pre-load validation:\n  - {listing}")

    frame = df.copy()
    for column in CSV_COLUMNS:
        if column not in frame.columns:
            frame[column] = ""
    frame = frame[list(CSV_COLUMNS)]

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", encoding="utf-8", newline="", delete=False
    ) as handle:
        temp_path = Path(handle.name)
        frame.to_csv(handle, index=False)

    try:
        return load_csv(connection, path=temp_path, validate=False)
    finally:
        temp_path.unlink(missing_ok=True)


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Load the CSV into PostgreSQL, or validate it with ``--dry-run``.

    ``--dry-run`` runs every check that does not need a server, so the file can
    be validated on a machine with no database and no ``psycopg2`` installed.
    """
    import argparse

    parser = argparse.ArgumentParser(description="Load the Stage 4 CSV into PostgreSQL.")
    parser.add_argument("--dsn", default=None, help="Connection string. Defaults to $DATABASE_URL then libpq PG* variables.")
    parser.add_argument("--csv", type=Path, default=None, help="Source CSV. Defaults to the Stage 4 output.")
    parser.add_argument("--dry-run", action="store_true", help="Validate the CSV and print the SQL without a database.")
    parser.add_argument("--skip-validation", action="store_true", help="Send the data without pre-load checks.")
    arguments = parser.parse_args(argv)

    source = resolve_source_path(arguments.csv)
    frame = read_source_csv(source)
    print(f"[stage 6] source      : {source}")
    print(f"[stage 6] shape       : {frame.shape[0]} rows x {frame.shape[1]} columns")

    problems = [] if arguments.skip_validation else validate_source_frame(frame)
    if problems:
        print(f"[stage 6] VALIDATION  : {len(problems)} problem(s)")
        for problem in problems:
            print(f"[stage 6]   - {problem}")
        return 1
    print(f"[stage 6] validation  : passed ({len(validate_source_frame(frame))} issues)")

    if arguments.dry_run:
        print(f"[stage 6] would create staging table with {len(CSV_COLUMNS)} columns")
        print("[stage 6] would COPY the CSV, then run:")
        for line in insert_sql().splitlines():
            print(f"[stage 6]   {line}")
        return 0

    from database import connect, describe_dsn, verify_load

    print(f"[stage 6] target      : {describe_dsn(arguments.dsn)}")
    with connect(arguments.dsn) as connection:
        inserted = load_csv(connection, source, validate=not arguments.skip_validation)
        print(f"[stage 6] inserted    : {inserted} row(s)")

        results = verify_load(connection)
        failures = sum(1 for result in results if not result["ok"])
        for result in results:
            status = "PASS" if result["ok"] else "FAIL"
            print(f"[stage 6] {status}      : {result['name']}: {result['detail']}")
        return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
