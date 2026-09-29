"""Stage 7 - Automated data quality verification for the Student Budget Analytics pipeline.

Runs a set of named, severity-ranked assertions over the transformed dataset
*before* anything reaches PostgreSQL, so a bad batch is rejected with a readable
reason instead of failing halfway through a ``COPY`` with a bare
``CheckViolation``.

Why severities
--------------
The single most important design decision in a quality framework is which
findings are allowed to block a load. Every assertion here was calibrated against
the seed data, and two findings that look like defects are actually the dataset's
real shape:

* **Every one of the 213 respondents overspends**, so ``savings`` is negative for
  213/213 rows and ``budget_usage_pct`` exceeds 100 for 213/213 rows. The naive
  assertions ``savings >= 0`` and ``usage <= 100`` therefore fail on 100% of the
  data. They are *not* used. See :func:`check_budget_usage_range` and
  :func:`check_savings_consistency`.
* **One student name appears twice** (``HARINE SHREE G``), but the two rows
  differ in 17 of 22 fields - different degree, year, timestamp and amounts.
  Those are two different people who share a name, not a duplicated submission.
  A naive "unique student_name" assertion fails the load on valid data, so
  duplicate detection is deliberately three-tiered: see
  :func:`check_exact_duplicate_rows`, :func:`check_duplicate_name_and_timestamp`
  and :func:`check_repeated_student_name`.

The consequence is that ``CRITICAL`` is reserved for defects that genuinely
indicate corrupt input: a missing column, a null in a mandatory field, a
negative amount, arithmetic that does not reconcile. Those are the only findings
that stop a load by default.

Usage::

    python src/data_quality.py                    # report on the Stage 4 CSV
    python src/data_quality.py --verbose          # every check, passed ones too
    python src/data_quality.py --json report.json # machine-readable output
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]

PROCESSED_DATA_PATH = PROJECT_ROOT / "data" / "processed" / "transformed_student_budget.csv"

#: Categorical domains are owned by ``data_loader`` and the ``sql/schema.sql``
#: CHECK constraints; importing them here keeps one source of truth rather than a
#: third copy that can silently drift.
try:
    from data_loader import CATEGORICAL_DOMAINS as _CATEGORICAL_DOMAINS
except ImportError:  # pragma: no cover - depends on invocation style
    from src.data_loader import CATEGORICAL_DOMAINS as _CATEGORICAL_DOMAINS

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

#: Stage 4 column names. The brief for this stage referred to
#: ``total_monthly_expenses``; the Stage 3/4 rename map collapsed
#: ``total_expenses_monthly`` to the display label ``Total Expenses``, so that is
#: the name every check below uses.
TIMESTAMP_COLUMN = "Timestamp"
NAME_COLUMN = "Student Name"
TOTAL_EXPENSES_COLUMN = "Total Expenses"
ALLOWANCE_COLUMN = "Allowance"
SAVINGS_COLUMN = "Savings"
BUDGET_USAGE_COLUMN = "Budget Usage %"

EXPENSE_COLUMNS: Tuple[str, ...] = ("Food", "Transport", "Academic", "Other")
MONEY_COLUMNS: Tuple[str, ...] = EXPENSE_COLUMNS + (ALLOWANCE_COLUMN, TOTAL_EXPENSES_COLUMN)

#: Fields that must be present and populated. A null in any of these makes a row
#: unusable, so they are CRITICAL rather than merely suspicious.
MANDATORY_COLUMNS: Tuple[str, ...] = (
    TIMESTAMP_COLUMN,
    NAME_COLUMN,
    "Gender",
    "Degree",
    "Year of Study",
    "Accommodation",
    "Preferred Payment",
) + MONEY_COLUMNS

#: Non-empty is a hard requirement. Below this the batch is more likely to be a
#: truncated export than a genuine survey, so it is reported as a warning.
MIN_REASONABLE_ROWS = 100

#: Budget usage is a ratio, not a share. The ceiling is the arithmetic maximum of
#: the survey's own bucket scheme (25000 / 250 * 100), not a round number.
BUDGET_USAGE_MIN = 0.0
BUDGET_USAGE_MAX = 10000.0

#: Money is stored as NUMERIC, so fractions are permitted by the schema, but the
#: pipeline only ever produces integral bucket midpoints. A fraction means an
#: upstream rule changed without this module being told.
MONEY_INTEGRAL_TOLERANCE = 1e-9

#: Tukey fence multiplier for the statistical outlier screen.
IQR_MULTIPLIER = 1.5

#: The survey's open-ended top buckets. These are *right-censored* observations:
#: "more than 5000" was recorded as the midpoint 6250, so the true figure is
#: unknown and higher. Every total that includes one is a lower bound, not an
#: estimate. Sourced from ``transformer.OPEN_HIGH_UPPER_MULTIPLIER``.
CENSORED_EXPENSE_VALUE = 6250.0
CENSORED_ALLOWANCE_VALUE = 3750.0

#: Plausible span of a single survey collection window.
MAX_TIMESTAMP_SPAN_DAYS = 365

#: UPI was asked of every respondent, not only those who pay by UPI, so naming an
#: app and paying by UPI are independent answers. See the module docstring.
UPI_PAYMENT_VALUE = "UPI"
UPI_NOT_APPLICABLE = "Not Applicable"

#: Free-text course answer; 44 spellings in the seed data, so it is reported
#: rather than policed.
SPECIALIZATION_COLUMN = "Specialization"

MAX_EXAMPLES = 5

# --------------------------------------------------------------------------- #
# Result model
# --------------------------------------------------------------------------- #

#: Ordered most to least severe. ``CRITICAL`` is the only tier that blocks a load
#: by default; ``WARNING`` blocks only under ``--quality strict``.
SEVERITIES: Tuple[str, ...] = ("CRITICAL", "WARNING", "INFO")

SEVERITY_RANK: Dict[str, int] = {name: index for index, name in enumerate(SEVERITIES)}


@dataclass
class CheckResult:
    """The outcome of one assertion."""

    name: str
    category: str
    severity: str
    passed: bool
    message: str
    offending: int = 0
    examples: List[Any] = field(default_factory=list)
    detail: str = ""

    @property
    def skipped(self) -> bool:
        """A check that could not run is not a failure."""
        return self.severity == "SKIPPED"

    @property
    def severity_rank(self) -> int:
        return SEVERITY_RANK.get(self.severity, len(SEVERITIES))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "category": self.category,
            "severity": self.severity,
            "passed": self.passed,
            "skipped": self.skipped,
            "offending": self.offending,
            "message": self.message,
            "examples": [str(example) for example in self.examples],
            "detail": self.detail,
        }


@dataclass
class QualityReport:
    """The aggregate outcome of a quality run."""

    results: List[CheckResult] = field(default_factory=list)
    row_count: int = 0

    def _count(self, severity: str, failed_only: bool = True) -> int:
        return sum(
            1
            for result in self.results
            if result.severity == severity and (not failed_only or not result.passed)
        )

    @property
    def critical_failures(self) -> List[CheckResult]:
        return [r for r in self.results if r.severity == "CRITICAL" and not r.passed]

    @property
    def warning_failures(self) -> List[CheckResult]:
        return [r for r in self.results if r.severity == "WARNING" and not r.passed]

    @property
    def info_failures(self) -> List[CheckResult]:
        return [r for r in self.results if r.severity == "INFO" and not r.passed]

    @property
    def passed(self) -> bool:
        """True when nothing CRITICAL failed. Warnings do not fail the report."""
        return not self.critical_failures

    @property
    def problems(self) -> List[CheckResult]:
        return [r for r in self.results if not r.passed and not r.skipped]

    def blocking(self, strict: bool = False) -> List[CheckResult]:
        """Failures that should stop a load.

        Parameters
        ----------
        strict:
            Also treat WARNING failures as blocking.
        """
        return self.critical_failures + (self.warning_failures if strict else [])

    def raise_if_failed(self, strict: bool = False) -> None:
        """Raise :class:`QualityCheckFailed` if anything blocking failed."""
        blocking = self.blocking(strict)
        if blocking:
            raise QualityCheckFailed(self, strict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "row_count": self.row_count,
            "passed": self.passed,
            "summary": {
                "total": len(self.results),
                "passed": sum(1 for r in self.results if r.passed),
                "skipped": sum(1 for r in self.results if r.skipped),
                "critical": self._count("CRITICAL"),
                "warning": self._count("WARNING"),
                "info": self._count("INFO"),
            },
            "results": [result.to_dict() for result in self.results],
        }

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent)

    def render(self, verbose: bool = False) -> str:
        """Return a human-readable report.

        Parameters
        ----------
        verbose:
            Include checks that passed. By default only problems and the summary
            are shown, so a clean run stays readable.
        """
        lines: List[str] = []
        shown = self.results if verbose else self.problems
        for result in shown:
            if result.skipped:
                tag = "SKIP"
            elif result.passed:
                tag = "PASS"
            else:
                tag = result.severity
            lines.append(f"  [{tag:8}] {result.name}")
            lines.append(f"             {result.message}")
            if result.examples:
                lines.append(f"             examples: {result.examples[:MAX_EXAMPLES]}")
            if result.detail:
                lines.append(f"             {result.detail}")

        counts = self._count("CRITICAL"), self._count("WARNING"), self._count("INFO")
        lines.append(
            f"  {len(self.results)} check(s): "
            f"{counts[0]} critical, {counts[1]} warning, {counts[2]} info, "
            f"{sum(1 for r in self.results if r.passed)} passed, "
            f"{sum(1 for r in self.results if r.skipped)} skipped"
        )
        lines.append(
            f"  RESULT: {'PASS' if self.passed else 'FAIL'}"
            + (f" (blocking: {len(self.critical_failures)})" if not self.passed else "")
        )
        return "\n".join(lines)


class QualityCheckFailed(RuntimeError):
    """Raised when quality findings are severe enough to block a load.

    Carries the whole :class:`QualityReport` so a caller can inspect, log or
    re-render the evidence rather than parsing the message.
    """

    def __init__(self, report: QualityReport, strict: bool = False) -> None:
        self.report = report
        self.strict = strict
        blocking = report.blocking(strict)
        names = ", ".join(result.name for result in blocking)
        tier = "CRITICAL and WARNING" if strict else "CRITICAL"
        super().__init__(f"{tier} data quality check(s) failed: {names}")


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _result(
    name: str,
    category: str,
    severity: str,
    passed: bool,
    message: str,
    offending: int = 0,
    examples: Optional[Sequence[Any]] = None,
    detail: str = "",
) -> CheckResult:
    return CheckResult(
        name=name,
        category=category,
        severity=severity,
        passed=passed,
        message=message,
        offending=offending,
        examples=list(examples or []),
        detail=detail,
    )


def _skipped(name: str, category: str, reason: str) -> CheckResult:
    return _result(name, category, "SKIPPED", True, f"skipped: {reason}")


def _absent(df: pd.DataFrame, columns: Sequence[str]) -> List[str]:
    return [column for column in columns if column not in df.columns]


def _numeric(df: pd.DataFrame, columns: Sequence[str]) -> Dict[str, pd.Series]:
    return {column: pd.to_numeric(df[column], errors="coerce") for column in columns}


def _unparseable(df: pd.DataFrame, columns: Sequence[str]) -> List[Tuple[str, int, List[Any]]]:
    """Return ``(column, count, examples)`` for cells that are not numbers."""
    out: List[Tuple[str, int, List[Any]]] = []
    for column, values in _numeric(df, columns).items():
        if column not in df.columns:
            continue
        bad = values.isna() & df[column].astype(str).str.strip().ne("")
        if bad.any():
            out.append((column, int(bad.sum()), sorted(set(df.loc[bad, column]))[:MAX_EXAMPLES]))
    return out


# --------------------------------------------------------------------------- #
# Completeness
# --------------------------------------------------------------------------- #


def check_row_count(df: pd.DataFrame) -> List[CheckResult]:
    """The frame must contain data, and ideally a plausible amount of it."""
    results = [
        _result(
            "row_count_not_empty",
            "completeness",
            "CRITICAL",
            len(df) > 0,
            f"{len(df)} row(s) in the frame",
        )
    ]
    if 0 < len(df) < MIN_REASONABLE_ROWS:
        results.append(
            _result(
                "row_count_plausible",
                "completeness",
                "WARNING",
                False,
                f"only {len(df)} row(s), below the {MIN_REASONABLE_ROWS} expected for a full survey",
                detail="a truncated export looks exactly like this",
            )
        )
    return results


def check_mandatory_columns(df: pd.DataFrame) -> List[CheckResult]:
    """Every column the pipeline and schema depend on must be present."""
    missing = _absent(df, MANDATORY_COLUMNS)
    if not missing:
        return [
            _result(
                "mandatory_columns_present",
                "completeness",
                "CRITICAL",
                True,
                f"all {len(MANDATORY_COLUMNS)} mandatory columns present",
            )
        ]
    return [
        _result(
            "mandatory_columns_present",
            "completeness",
            "CRITICAL",
            False,
            f"{len(missing)} mandatory column(s) missing",
            offending=len(missing),
            examples=missing,
            detail="the load cannot proceed without these",
        )
    ]


# --------------------------------------------------------------------------- #
# Validity: nulls and blanks
# --------------------------------------------------------------------------- #


def check_mandatory_field_nulls(df: pd.DataFrame) -> List[CheckResult]:
    """No mandatory field may be null or whitespace-only."""
    results: List[CheckResult] = []
    for column in MANDATORY_COLUMNS:
        if column not in df.columns:
            results.append(_skipped(f"no_nulls::{column}", "validity", f"column {column!r} absent"))
            continue
        nulls = int(df[column].isna().sum())
        blanks = int(df[column].astype(str).str.strip().eq("").sum())
        blanks_only = int((df[column].isna() | df[column].astype(str).str.strip().eq("")).sum())
        failed = blanks_only > 0
        results.append(
            _result(
                f"no_nulls::{column}",
                "validity",
                "CRITICAL",
                not failed,
                f"{column}: {nulls} null, {blanks} whitespace-only ({blanks_only} unusable)",
                offending=blanks_only,
                examples=(
                    sorted(set(df.loc[df[column].isna() | df[column].astype(str).str.strip().eq(""), column].astype(str)))[:MAX_EXAMPLES]
                    if failed
                    else []
                ),
            )
        )
    return results


def check_optional_field_nulls(df: pd.DataFrame) -> List[CheckResult]:
    """Non-mandatory text fields should still be populated.

    Warning rather than critical: these are descriptive dimensions, and a missing
    specialization does not stop a load, it just weakens a per-course breakdown.
    """
    optional = ("Specialization", "Background", "Work/Internship", "Track Digitally",
                "Budget Plan", "Study Focus", "UPI App")
    results: List[CheckResult] = []
    for column in optional:
        if column not in df.columns:
            results.append(_skipped(f"populated::{column}", "validity", f"column {column!r} absent"))
            continue
        missing = int(df[column].isna().sum() + df[column].astype(str).str.strip().eq("").sum())
        results.append(
            _result(
                f"populated::{column}",
                "validity",
                "WARNING",
                missing == 0,
                f"{column}: {missing} missing value(s)",
                offending=missing,
            )
        )
    return results


# --------------------------------------------------------------------------- #
# Numeric constraints
# --------------------------------------------------------------------------- #


def check_money_numeric(df: pd.DataFrame) -> List[CheckResult]:
    """Every money field must be parseable as a number."""
    present = [column for column in MONEY_COLUMNS if column in df.columns]
    if not present:
        return [_skipped("money_numeric", "numeric", "no money columns present")]
    bad = _unparseable(df, present)
    return [
        _result(
            "money_numeric",
            "numeric",
            "CRITICAL",
            not bad,
            "all money fields are numeric" if not bad
            else f"{len(bad)} money field(s) contain non-numeric text",
            offending=sum(count for _, count, _ in bad),
            examples=[f"{column}: {values}" for column, _, values in bad],
        )
    ]


def check_money_non_negative(df: pd.DataFrame) -> List[CheckResult]:
    """Amounts cannot be negative.

    This is the assertion the brief calls ``total_monthly_expenses >= 0``, applied
    to every money column rather than just the total, because a negative
    component would silently inflate the derived total. It is safe for all six
    columns here: the observed minimum is 1250.
    """
    present = [column for column in MONEY_COLUMNS if column in df.columns]
    if not present:
        return [_skipped("money_non_negative", "numeric", "no money columns present")]

    results: List[CheckResult] = []
    for column in present:
        values = pd.to_numeric(df[column], errors="coerce")
        negative = values < 0
        count = int(negative.sum())
        results.append(
            _result(
                f"non_negative::{column}",
                "numeric",
                "CRITICAL",
                count == 0,
                f"{column}: min {values.min()}, {count} negative value(s)",
                offending=count,
                examples=sorted(set(df.loc[negative, column]))[:MAX_EXAMPLES] if count else [],
            )
        )
    return results


def check_money_integral(df: pd.DataFrame) -> List[CheckResult]:
    """Money should be whole rupees; the pipeline only produces bucket midpoints."""
    present = [column for column in MONEY_COLUMNS if column in df.columns]
    if not present:
        return [_skipped("money_integral", "numeric", "no money columns present")]

    results: List[CheckResult] = []
    for column in present:
        values = pd.to_numeric(df[column], errors="coerce")
        fractional = (values % 1 != 0) & values.notna()
        count = int(fractional.sum())
        results.append(
            _result(
                f"integral::{column}",
                "numeric",
                "WARNING",
                count == 0,
                f"{column}: {count} fractional value(s)",
                offending=count,
                examples=sorted(set(values[fractional]))[:MAX_EXAMPLES] if count else [],
                detail="expected: all money derives from integral survey-bucket midpoints",
            )
        )
    return results


def check_total_expenses_reconciles(df: pd.DataFrame) -> List[CheckResult]:
    """``Total Expenses`` must equal the sum of its four components."""
    needed = list(EXPENSE_COLUMNS) + [TOTAL_EXPENSES_COLUMN]
    if _absent(df, needed):
        return [_skipped("total_expenses_reconciles", "numeric", f"missing {_absent(df, needed)}")]

    components = (
        df[list(EXPENSE_COLUMNS)]
        .apply(pd.to_numeric, errors="coerce")
        .sum(axis=1, min_count=len(EXPENSE_COLUMNS))
    )
    total = pd.to_numeric(df[TOTAL_EXPENSES_COLUMN], errors="coerce")
    mismatch = (components - total).abs() > 0.005
    count = int(mismatch.sum())
    return [
        _result(
            "total_expenses_reconciles",
            "numeric",
            "CRITICAL",
            count == 0,
            f"{count} row(s) where the total does not equal the sum of the parts",
            offending=count,
            detail="the database recomputes this column, so a mismatch means the two definitions have drifted",
        )
    ]


def check_savings_consistency(df: pd.DataFrame) -> List[CheckResult]:
    """``Savings`` must equal ``Allowance - Total Expenses``.

    Deliberately *not* ``savings >= 0``. All 213 respondents overspend, so the
    observed range is -24000..-250; asserting non-negativity would fail every
    row and teach operators to ignore the check. The realistic failure this
    catches is an inconsistent *definition* between the pipeline and the schema,
    which is why the assertion is on the arithmetic rather than the sign.
    """
    needed = [ALLOWANCE_COLUMN, TOTAL_EXPENSES_COLUMN, SAVINGS_COLUMN]
    if _absent(df, needed):
        return [_skipped("savings_consistency", "numeric", f"missing {_absent(df, needed)}")]

    allowance = pd.to_numeric(df[ALLOWANCE_COLUMN], errors="coerce")
    total = pd.to_numeric(df[TOTAL_EXPENSES_COLUMN], errors="coerce")
    savings = pd.to_numeric(df[SAVINGS_COLUMN], errors="coerce")
    mismatch = ((allowance - total) - savings).abs() > 0.005
    count = int(mismatch.sum())
    negative = int((savings < 0).sum())
    return [
        _result(
            "savings_consistency",
            "numeric",
            "CRITICAL",
            count == 0,
            f"{count} row(s) where savings != allowance - total",
            offending=count,
            detail=(
                f"note: {negative}/{len(df)} rows have negative savings; that is the real "
                f"finding in this dataset, not an error"
            ),
        )
    ]


def check_budget_usage_range(df: pd.DataFrame) -> List[CheckResult]:
    """Budget usage must be non-negative and below the scheme's arithmetic ceiling.

    Deliberately *not* ``BETWEEN 0 AND 100``. The observed range is 125.0..8600.0
    and no row is at or below 100, because the survey's allowance buckets are much
    coarser and lower than its expense buckets. The column is a ratio, not a
    share. See the Stage 5 header in ``sql/schema.sql``.
    """
    if BUDGET_USAGE_COLUMN not in df.columns:
        return [_skipped("budget_usage_range", "numeric", f"column {BUDGET_USAGE_COLUMN!r} absent")]

    values = pd.to_numeric(df[BUDGET_USAGE_COLUMN], errors="coerce")
    unparseable = int((values.isna() & df[BUDGET_USAGE_COLUMN].astype(str).str.strip().ne("")).sum())
    out_of_range = (values < BUDGET_USAGE_MIN) | (values > BUDGET_USAGE_MAX)
    count = int(out_of_range.fillna(False).sum())
    over_100 = int((values > 100).sum())
    return [
        _result(
            "budget_usage_range",
            "numeric",
            "CRITICAL",
            count == 0 and unparseable == 0,
            f"range {values.min()}..{values.max()}, {count} outside "
            f"[{BUDGET_USAGE_MIN}, {BUDGET_USAGE_MAX}], {unparseable} non-numeric",
            offending=count + unparseable,
            detail=(
                f"note: {over_100}/{len(df)} rows exceed 100%, which is the expected shape of "
                f"this survey; the ceiling is the bucket scheme's maximum, not a share cap"
            ),
        )
    ]


# --------------------------------------------------------------------------- #
# Uniqueness
# --------------------------------------------------------------------------- #


def check_exact_duplicate_rows(df: pd.DataFrame) -> List[CheckResult]:
    """No two rows may be identical across every column.

    This is the only duplicate signal that reliably means "the same record was
    loaded twice", so it is the only one treated as CRITICAL.
    """
    if df.empty:
        return [_skipped("exact_duplicate_rows", "uniqueness", "frame is empty")]
    duplicated = df.duplicated(keep=False)
    count = int(duplicated.sum())
    return [
        _result(
            "exact_duplicate_rows",
            "uniqueness",
            "CRITICAL",
            count == 0,
            f"{count} row(s) are exact duplicates across all {df.shape[1]} columns",
            offending=count,
            detail="a repeated record means a re-ingestion or a double-pasted export",
        )
    ]


def check_duplicate_name_and_timestamp(df: pd.DataFrame) -> List[CheckResult]:
    """The same name at the same instant twice is impossible for a real respondent.

    Stronger evidence than a name collision and treated as critical: a form
    cannot produce two submissions from one person at one timestamp, so this
    indicates a duplicated row whose other answers were edited, or a merge fault.
    """
    keys = [NAME_COLUMN, TIMESTAMP_COLUMN]
    if _absent(df, keys):
        return [_skipped("duplicate_name_and_timestamp", "uniqueness", f"missing {_absent(df, keys)}")]
    duplicated = df.duplicated(subset=keys, keep=False)
    count = int(duplicated.sum())
    return [
        _result(
            "duplicate_name_and_timestamp",
            "uniqueness",
            "CRITICAL",
            count == 0,
            f"{count} row(s) share both a name and a timestamp",
            offending=count,
            examples=sorted(set(df.loc[duplicated, NAME_COLUMN]))[:MAX_EXAMPLES] if count else [],
        )
    ]


def check_repeated_student_name(df: pd.DataFrame) -> List[CheckResult]:
    """Repeated names are a warning, never a failure.

    The seed data contains exactly one such pair: ``HARINE SHREE G`` appears
    twice, but the rows differ in 17 of 22 fields - different degree, year of
    study, timestamp and amounts. Those are two different people who share a
    name, and the same is true of any real survey. ``student_name`` is therefore
    indexed but never unique, here and in ``sql/schema.sql``. Making this
    CRITICAL would block every future load of valid data.
    """
    if NAME_COLUMN not in df.columns:
        return [_skipped("repeated_student_name", "uniqueness", f"column {NAME_COLUMN!r} absent")]

    duplicated = df.duplicated(subset=[NAME_COLUMN], keep=False)
    count = int(duplicated.sum())
    if not count:
        return [
            _result(
                "repeated_student_name",
                "uniqueness",
                "WARNING",
                True,
                f"all {df[NAME_COLUMN].nunique()} student names are unique",
            )
        ]

    # Separate genuine repeats from name collisions by looking at whether the
    # rest of the record agrees.
    collisions: List[str] = []
    repeats: List[str] = []
    for name, group in df.loc[duplicated].groupby(NAME_COLUMN):
        differing = [column for column in df.columns if group[column].nunique(dropna=False) > 1]
        (collisions if differing else repeats).append(name)

    severity_note = (
        f"{len(collisions)} name collision(s) with differing answers "
        f"(different people), {len(repeats)} identical-name repeat(s)"
    )
    return [
        _result(
            "repeated_student_name",
            "uniqueness",
            "WARNING",
            not repeats,
            f"{count} row(s) share a name across {df[NAME_COLUMN].nunique()} distinct name(s)",
            offending=len(repeats),
            examples=repeats[:MAX_EXAMPLES],
            detail=(
                f"{severity_note}; collisions are expected and are not duplicates. "
                f"Names seen: {(collisions + repeats)[:MAX_EXAMPLES]}"
            ),
        )
    ]


# --------------------------------------------------------------------------- #
# Anomalies
# --------------------------------------------------------------------------- #


def check_statistical_outliers(df: pd.DataFrame) -> List[CheckResult]:
    """Tukey-fence screen on each money field.

    Warning only. In the seed data this flags nothing, because the values are
    coarse bucket midpoints with a handful of distinct levels, so the interquartile
    range is wide and the fences land outside the real support. It is kept because
    it becomes meaningful the moment the data is sourced from something other than
    a fixed-choice survey.
    """
    present = [column for column in MONEY_COLUMNS if column in df.columns]
    if not present:
        return [_skipped("statistical_outliers", "anomaly", "no money columns present")]

    results: List[CheckResult] = []
    for column in present:
        values = pd.to_numeric(df[column], errors="coerce").dropna()
        if len(values) < 4:
            results.append(_skipped(f"outliers::{column}", "anomaly", "too few values"))
            continue
        q1, q3 = values.quantile([0.25, 0.75])
        iqr = q3 - q1
        if iqr <= 0:
            results.append(_skipped(f"outliers::{column}", "anomaly", "zero interquartile range"))
            continue
        low, high = q1 - IQR_MULTIPLIER * iqr, q3 + IQR_MULTIPLIER * iqr
        flagged = (values < low) | (values > high)
        count = int(flagged.sum())
        results.append(
            _result(
                f"outliers::{column}",
                "anomaly",
                "WARNING",
                count == 0,
                f"{column}: {count} value(s) outside [{low:.0f}, {high:.0f}]",
                offending=count,
                detail=f"distinct values: {values.nunique()}; Tukey fences at {IQR_MULTIPLIER}x IQR",
            )
        )
    return results


def check_censored_values(df: pd.DataFrame) -> List[CheckResult]:
    """Report right-censored money cells.

    A respondent answering "more than 5000" is stored as the midpoint 6250. The
    real figure is unknown and higher, so every total and average built from these
    columns is a *lower bound*. 13.4% of expense cells in the seed data are
    censored, which is large enough to bias any mean. This is informational by
    design: it is a property of the survey instrument, not a defect, and it must
    not block a load.
    """
    present = [column for column in EXPENSE_COLUMNS if column in df.columns]
    if not present or df.empty:
        # An empty frame is already reported by check_row_count; computing a
        # percentage here would divide by zero and misreport a check defect as
        # a data problem.
        return [_skipped("censored_values", "anomaly", "no rows to measure")]

    total_cells = len(df) * len(present)
    censored = 0
    by_column: List[str] = []
    for column in present:
        count = int((pd.to_numeric(df[column], errors="coerce") == CENSORED_EXPENSE_VALUE).sum())
        censored += count
        if count:
            by_column.append(f"{column}={count}")

    # pd.to_numeric rejects a DataFrame, so build the boolean mask column by column.
    censored_mask = pd.DataFrame(
        {column: pd.to_numeric(df[column], errors="coerce") == CENSORED_EXPENSE_VALUE for column in present}
    )
    affected = int(censored_mask.any(axis=1).sum())
    return [
        _result(
            "censored_values",
            "anomaly",
            "INFO",
            True,
            f"{censored}/{total_cells} expense cells are right-censored "
            f"({censored / total_cells * 100:.1f}%), affecting {affected} row(s)",
            detail=(
                f"cells at the open-ended '> 5000' midpoint ({CENSORED_EXPENSE_VALUE:.0f}): "
                f"{', '.join(by_column) or 'none'}. Totals including these are lower bounds."
            ),
        )
    ]


def check_timestamp_sanity(df: pd.DataFrame) -> List[CheckResult]:
    """Timestamps must parse, exist, not be in the future, and span a sane window."""
    if TIMESTAMP_COLUMN not in df.columns:
        return [_skipped("timestamp_sanity", "anomaly", f"column {TIMESTAMP_COLUMN!r} absent")]

    parsed = pd.to_datetime(df[TIMESTAMP_COLUMN], errors="coerce", format="mixed")
    unparseable = int((parsed.isna() & df[TIMESTAMP_COLUMN].astype(str).str.strip().ne("")).sum())
    future = int((parsed > pd.Timestamp.now()).sum())
    span = (parsed.max() - parsed.min()).days if parsed.notna().any() else 0

    return [
        _result(
            "timestamp_parseable",
            "anomaly",
            "CRITICAL",
            unparseable == 0,
            f"{unparseable} unparseable timestamp(s), {parsed.min()} .. {parsed.max()}",
            offending=unparseable,
            detail="loaded into a TIMESTAMPTZ column, so a bad value cannot be stored",
        ),
        _result(
            "timestamp_not_future",
            "anomaly",
            "CRITICAL",
            future == 0,
            f"{future} timestamp(s) in the future",
            offending=future,
        ),
        _result(
            "timestamp_span_sane",
            "anomaly",
            "WARNING",
            span <= MAX_TIMESTAMP_SPAN_DAYS,
            f"collection window spans {span} day(s)",
            detail=f"expected at most {MAX_TIMESTAMP_SPAN_DAYS} days for one survey",
        ),
    ]


def check_cross_field_consistency(df: pd.DataFrame) -> List[CheckResult]:
    """Cross-field business rules.

    The one implemented is the UPI relationship. It fires on 112 of 213 rows, and
    that is a genuine finding rather than a defect: the questionnaire asked which
    UPI app the respondent uses without gating it on paying by UPI, so the two
    answers are independent. It is reported so the oddity is visible, and kept
    non-blocking so it does not stop every load.
    """
    payment, app = "Preferred Payment", "UPI App"
    if _absent(df, (payment, app)):
        return [_skipped("cross_field_consistency", "anomaly", f"missing {_absent(df, (payment, app))}")]

    names_app_without_upi = df[(df[payment] != UPI_PAYMENT_VALUE) & (df[app] != UPI_NOT_APPLICABLE)]
    upi_without_app = df[(df[payment] == UPI_PAYMENT_VALUE) & (df[app] == UPI_NOT_APPLICABLE)]
    return [
        _result(
            "cross_field_consistency::upi_app_matches_payment",
            "anomaly",
            "WARNING",
            len(names_app_without_upi) == 0,
            f"{len(names_app_without_upi)} row(s) name a UPI app but do not pay by UPI",
            offending=len(names_app_without_upi),
            detail=(
                f"{len(upi_without_app)} row(s) pay by UPI but report '{UPI_NOT_APPLICABLE}'. "
                f"Expected behaviour of an ungated question, not a data error."
            ),
        )
    ]


def check_categorical_domains(df: pd.DataFrame) -> List[CheckResult]:
    """Categorical values must fall inside the CHECK domains in ``sql/schema.sql``.

    Critical, because PostgreSQL enforces the same lists and a value outside them
    aborts the load. The domains are imported from ``data_loader`` rather than
    restated, so the three definitions cannot drift apart.
    """
    results: List[CheckResult] = []
    for column, allowed in sorted(_CATEGORICAL_DOMAINS.items()):
        if column not in df.columns:
            results.append(_skipped(f"domain::{column}", "validity", f"column {column!r} absent"))
            continue
        offenders = sorted(set(df[column]) - set(allowed))
        results.append(
            _result(
                f"domain::{column}",
                "validity",
                "CRITICAL",
                not offenders,
                f"{column}: {len(offenders)} value(s) outside the allowed domain",
                offending=len(offenders),
                examples=offenders[:MAX_EXAMPLES],
                detail=f"allowed: {list(allowed)}",
            )
        )
    return results


def check_specialization_cardinality(df: pd.DataFrame) -> List[CheckResult]:
    """Report, but do not police, the free-text course column.

    44 distinct spellings including ``BACHELOR OF BUSINESS ADMINISTRATION`` is
    the reason ``specialization`` carries no CHECK constraint. Reported so the
    lack of canonicalisation stays visible instead of being rediscovered.
    """
    if SPECIALIZATION_COLUMN not in df.columns:
        return [_skipped("specialization_cardinality", "anomaly", f"column {SPECIALIZATION_COLUMN!r} absent")]

    distinct = df[SPECIALIZATION_COLUMN].nunique()
    ratio = distinct / len(df) if len(df) else 0
    return [
        _result(
            "specialization_cardinality",
            "anomaly",
            "INFO",
            True,
            f"{distinct} distinct course strings across {len(df)} rows ({ratio:.0%})",
            detail=(
                "free text, deliberately unconstrained. Normalising it would allow a CHECK "
                "constraint and per-course grouping."
            ),
        )
    ]


# --------------------------------------------------------------------------- #
# Registry and runner
# --------------------------------------------------------------------------- #

#: Ordered so the report reads from structural problems to subtle ones. A check
#: that needs a missing column skips itself rather than cascading.
CheckFunction = Callable[[pd.DataFrame], List[CheckResult]]

DEFAULT_CHECKS: Tuple[CheckFunction, ...] = (
    check_row_count,
    check_mandatory_columns,
    check_mandatory_field_nulls,
    check_optional_field_nulls,
    check_money_numeric,
    check_money_non_negative,
    check_money_integral,
    check_total_expenses_reconciles,
    check_savings_consistency,
    check_budget_usage_range,
    check_exact_duplicate_rows,
    check_duplicate_name_and_timestamp,
    check_repeated_student_name,
    check_statistical_outliers,
    check_censored_values,
    check_timestamp_sanity,
    check_cross_field_consistency,
    check_categorical_domains,
    check_specialization_cardinality,
)


def run_quality_checks(
    df: pd.DataFrame,
    checks: Optional[Sequence[CheckFunction]] = None,
) -> QualityReport:
    """Run every check and return the aggregate report.

    Checks never raise: a failure inside one assertion is captured as a failed
    result so the remaining checks still run and the operator sees the full
    picture in one pass.

    Parameters
    ----------
    df:
        The transformed dataset to verify.
    checks:
        Checks to run. Defaults to :data:`DEFAULT_CHECKS`.

    Returns
    -------
    QualityReport
    """
    report = QualityReport(row_count=len(df))
    for check in checks or DEFAULT_CHECKS:
        try:
            report.results.extend(check(df))
        except Exception as exc:  # a broken check must not mask the others
            report.results.append(
                _result(
                    getattr(check, "__name__", str(check)),
                    "internal",
                    "CRITICAL",
                    False,
                    f"check raised {type(exc).__name__}: {exc}",
                    detail="this is a defect in the check itself, not in the data",
                )
            )
    return report


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #


def resolve_source_path(path: Optional[Path] = None) -> Path:
    """Locate the Stage 4 CSV, tolerating the ``data/`` versus ``Data/`` split."""
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


def verify_file(path: Optional[Path] = None) -> QualityReport:
    """Verify the Stage 4 CSV on disk and return the report.

    This is the entry point the pipeline uses. It reads the file rather than
    accepting an in-memory frame so the quality gate inspects *the exact artifact
    that is about to be loaded*, not a possibly different object still held in
    memory. A gate that verifies one thing and loads another is worthless.
    """
    return run_quality_checks(pd.read_csv(resolve_source_path(path)))


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Verify the Stage 4 CSV and report. Exits non-zero on CRITICAL failures."""
    import argparse

    parser = argparse.ArgumentParser(description="Stage 7 - data quality verification.")
    parser.add_argument("--csv", type=Path, default=None, help="CSV to verify. Defaults to the Stage 4 output.")
    parser.add_argument("--verbose", action="store_true", help="Show passing checks too.")
    parser.add_argument("--strict", action="store_true", help="Treat WARNING failures as blocking.")
    parser.add_argument("--json", type=Path, default=None, help="Also write the report to this JSON file.")
    arguments = parser.parse_args(argv)

    source = resolve_source_path(arguments.csv)
    print(f"[stage 7] source      : {source}")

    report = verify_file(arguments.csv)
    print(f"[stage 7] {report.render(verbose=arguments.verbose)}")

    if arguments.json:
        arguments.json.parent.mkdir(parents=True, exist_ok=True)
        arguments.json.write_text(report.to_json(), encoding="utf-8")
        print(f"[stage 7] wrote       : {arguments.json}")

    if report.passed:
        return 0
    try:
        report.raise_if_failed(strict=arguments.strict)
    except QualityCheckFailed as exc:
        print(f"[stage 7] BLOCKING    : {exc}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
