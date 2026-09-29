"""Stage 6 - End-to-end pipeline runner for Student Budget Analytics.

Drives the whole chain in order, so the stages can be reproduced in one command
instead of by remembering four script invocations:

    Stage 3  clean the raw Google-Forms export      -> cleaned_student_budget.csv
    Stage 4  transform to short display schema      -> transformed_student_budget.csv
    Stage 5  create the PostgreSQL schema           -> table students_budget
    Stage 6  load the CSV into that table           -> 213 rows, verified

Stages 3 and 4 are pure and always runnable. Stages 5 and 6 need a PostgreSQL
server, so under ``--dry-run`` they report exactly what they would send to the
database and stop there. That makes the dry run a genuine pre-flight check: it
validates the CSV against every constraint the schema will enforce and prints
the generated SQL, without needing a driver or a server.

Usage::

    python src/pipeline.py --dry-run
    python src/pipeline.py --stages 3,4
    python src/pipeline.py --dsn postgresql://user@localhost:5432/student_budget --allow-drop
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = Path(__file__).resolve().parent

# The stage modules are plain scripts run as ``python src/<name>.py`` and the
# repository has no ``src/__init__.py``, so they are importable as top-level
# modules only once this directory is on the path. Inserting it here keeps
# ``python src/pipeline.py`` and ``python -m src.pipeline`` equivalent.
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import cleaner  # noqa: E402  (path setup must precede these imports)
import data_loader  # noqa: E402
import database  # noqa: E402
import transformer  # noqa: E402

STAGE_DESCRIPTIONS: Dict[int, str] = {
    3: "clean the raw survey export",
    4: "transform into the display schema",
    5: "create the PostgreSQL schema",
    6: "load the CSV into PostgreSQL",
}

#: Stage 6 depends on the table created by stage 5. Running it alone against a
#: database with no table would fail deep inside the load, so the dependency is
#: checked up front and reported as a skip.
STAGE_DEPENDENCIES: Dict[int, int] = {6: 5}


class StageError(RuntimeError):
    """Raised when a stage fails in a way the runner should report, not re-raise."""


def _report(stage: int, message: str) -> None:
    print(f"[stage {stage}] {message}")


# --------------------------------------------------------------------------- #
# Stages
# --------------------------------------------------------------------------- #


def run_stage_3(context: Dict[str, Any]) -> None:
    """Clean the raw export and write ``cleaned_student_budget.csv``."""
    raw_path = cleaner.resolve_raw_data_path()
    raw = cleaner.load_raw_data(raw_path)
    _report(3, f"source      : {raw_path.relative_to(PROJECT_ROOT)}")
    _report(3, f"raw shape   : {raw.shape[0]} rows x {raw.shape[1]} columns")

    cleaned = cleaner.clean_data(raw)
    destination = cleaner.export_cleaned_data(cleaned)
    _report(3, f"dropped     : {raw.shape[0] - cleaned.shape[0]} corrupted/empty row(s)")
    _report(3, f"clean shape : {cleaned.shape[0]} rows x {cleaned.shape[1]} columns")
    _report(3, f"wrote       : {destination.relative_to(PROJECT_ROOT)}")
    context["stage3_rows"] = cleaned.shape[0]


def run_stage_4(context: Dict[str, Any]) -> None:
    """Transform the cleaned CSV and write ``transformed_student_budget.csv``."""
    cleaned = transformer.load_cleaned_data()
    transformed = transformer.transform_data(cleaned)
    destination = transformer.export_transformed_data(transformed)
    _report(4, f"in shape    : {cleaned.shape[0]} rows x {cleaned.shape[1]} columns")
    _report(4, f"out shape   : {transformed.shape[0]} rows x {transformed.shape[1]} columns")
    _report(4, f"usage %     : {transformed[transformer.BUDGET_USAGE_COLUMN].min():.1f} - "
               f"{transformed[transformer.BUDGET_USAGE_COLUMN].max():.1f}")
    _report(4, f"wrote       : {destination.relative_to(PROJECT_ROOT)}")
    context["stage4_rows"] = transformed.shape[0]


def run_stage_5(context: Dict[str, Any]) -> None:
    """Apply ``sql/schema.sql``, which drops and recreates the table.

    Guarded rather than free to destroy data: the script opens with
    ``DROP TABLE IF EXISTS``, so running it against a populated database throws
    away everything loaded so far. ``--allow-drop`` is the explicit opt-in.
    """
    script = database.read_schema()
    if database.schema_drops_table(script):
        _report(5, "WARNING    : schema.sql contains DROP TABLE; existing data will be destroyed")
        if not context["allow_drop"]:
            raise StageError(
                "refusing to drop an existing table without --allow-drop "
                "(or use --dry-run to inspect without touching the database)"
            )

    applied = database.apply_schema(context["connection"], context.get("schema_path"))
    _report(5, f"applied    : {applied} characters of DDL")
    _report(5, f"table      : {database.TARGET_TABLE} created")


def run_stage_6(context: Dict[str, Any]) -> None:
    """Load the Stage 4 CSV and verify the result."""
    source = data_loader.resolve_source_path()
    _report(6, f"source     : {source}")

    frame = data_loader.read_source_csv(source)
    problems = [] if context["skip_validation"] else data_loader.validate_source_frame(frame)
    if problems:
        for problem in problems:
            _report(6, f"problem    : {problem}")
        raise StageError(f"{len(problems)} pre-load validation problem(s); nothing was sent to the database")
    _report(6, f"validation : passed ({frame.shape[0]} rows x {frame.shape[1]} columns)")

    inserted = data_loader.load_csv(
        context["connection"], source, validate=not context["skip_validation"]
    )
    _report(6, f"inserted   : {inserted} row(s)")

    results = database.verify_load(context["connection"])
    failures = 0
    for result in results:
        failures += 0 if result["ok"] else 1
        _report(6, f"{'PASS' if result['ok'] else 'FAIL'}       : {result['name']}: {result['detail']}")
    if failures:
        raise StageError(f"{failures} verification check(s) failed")


STAGE_RUNNERS: Dict[int, Callable[[Dict[str, Any]], None]] = {
    3: run_stage_3,
    4: run_stage_4,
    5: run_stage_5,
    6: run_stage_6,
}

#: Stages that need a database connection, in the order they must run.
DATABASE_STAGES = (5, 6)


# --------------------------------------------------------------------------- #
# Dry run
# --------------------------------------------------------------------------- #


def dry_run(stages: Sequence[int]) -> int:
    """Run the pure stages, then report what the database stages would do.

    Returns a process exit code. Non-zero means the dry run found a problem, so
    it can gate a deployment step in CI.
    """
    context: Dict[str, Any] = {"allow_drop": True, "skip_validation": False}

    for stage in stages:
        if stage in DATABASE_STAGES:
            _report(stage, f"would      : {STAGE_DESCRIPTIONS[stage]}")
        else:
            STAGE_RUNNERS[stage](context)

    if any(stage in DATABASE_STAGES for stage in stages):
        _report(5, "would apply sql/schema.sql (drops and recreates students_budget)")
        _report(6, "would create a TEMP staging table and COPY the CSV into it")
        for line in data_loader.insert_sql().splitlines():
            _report(6, f"would run  : {line.strip()}")

        frame = data_loader.read_source_csv()
        problems = data_loader.validate_source_frame(frame)
        _report(6, f"validation : {'passed' if not problems else str(len(problems)) + ' problem(s)'}")
        for problem in problems:
            _report(6, f"problem    : {problem}")
        if problems:
            return 1

    print("[pipeline ] dry run complete; no database was contacted")
    return 0


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #


def parse_stages(value: str) -> List[int]:
    """Turn ``"3,4"`` or ``"all"`` into an ordered list of stage numbers.

    Raises
    ------
    ValueError
        If a stage is not an integer in the supported range.
    """
    if value.strip().lower() == "all":
        return sorted(STAGE_RUNNERS)
    try:
        requested = [int(part.strip()) for part in value.split(",") if part.strip()]
    except ValueError:
        raise ValueError(f"stage list must be integers or 'all', got {value!r}") from None
    unknown = sorted(set(requested) - set(STAGE_RUNNERS))
    if unknown:
        supported = ", ".join(str(stage) for stage in sorted(STAGE_RUNNERS))
        raise ValueError(f"unknown stage(s) {unknown}; supported stages are {supported}")
    return sorted(set(requested))


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Parse arguments, run the selected stages in order, and report the outcome."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Run the Student Budget Analytics pipeline (stages 3-6).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--stages",
        default="all",
        help="Comma-separated stage numbers, or 'all' for 3,4,5,6. Default: all",
    )
    parser.add_argument(
        "--dsn",
        default=None,
        help="PostgreSQL connection string. Defaults to $DATABASE_URL, then the libpq PG* variables.",
    )
    parser.add_argument("--schema", type=Path, default=None, help="Schema file. Defaults to sql/schema.sql.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run the offline stages and report what the database stages would do, without a server.",
    )
    parser.add_argument(
        "--allow-drop",
        action="store_true",
        help="Required to run stage 5, because sql/schema.sql drops and recreates the table.",
    )
    parser.add_argument(
        "--skip-validation",
        action="store_true",
        help="Load without the pre-flight CSV checks (not recommended).",
    )
    arguments = parser.parse_args(argv)

    try:
        stages = parse_stages(arguments.stages)
    except ValueError as exc:
        parser.error(str(exc))
        return 2

    print("[pipeline ] stages     : " + ", ".join(f"{s} ({STAGE_DESCRIPTIONS[s]})" for s in stages))
    print("[pipeline ] dry run    : " + str(arguments.dry_run).lower())

    if arguments.dry_run:
        return dry_run(stages)

    needs_database = any(stage in DATABASE_STAGES for stage in stages)
    if needs_database:
        print(f"[pipeline ] target     : {database.describe_dsn(arguments.dsn)}")

    context: Dict[str, Any] = {
        "allow_drop": arguments.allow_drop,
        "skip_validation": arguments.skip_validation,
        "schema_path": arguments.schema,
    }

    if not needs_database:
        for stage in stages:
            STAGE_RUNNERS[stage](context)
        print("[pipeline ] done")
        return 0

    try:
        with database.connect(arguments.dsn) as connection:
            context["connection"] = connection
            for stage in stages:
                dependency = STAGE_DEPENDENCIES.get(stage)
                if dependency is not None and dependency not in stages:
                    _report(stage, f"NOTE      : stage {stage} normally follows stage {dependency}; "
                                   f"assuming the table already exists")
                STAGE_RUNNERS[stage](context)
    except StageError as exc:
        print(f"[pipeline ] FAILED    : {exc}", file=sys.stderr)
        return 1
    except ImportError as exc:
        print(f"[pipeline ] FAILED    : {exc}", file=sys.stderr)
        return 2
    except Exception as exc:
        print(f"[pipeline ] FAILED    : {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1

    print("[pipeline ] done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
