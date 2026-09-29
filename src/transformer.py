"""Stage 4 - Data transformation for the Student Budget Analytics pipeline.

Reshapes the cleaned dataset into a compact, display-oriented schema: short
column names, numeric money fields and derived financial metrics.

Usage::

    python src/transformer.py

Two column-naming layers are used deliberately:

* ``CANONICAL_COLUMNS`` - short ``snake_case`` identifiers. These are the
  internal contract and the only thing the transformation functions address.
* ``DISPLAY_NAMES`` - the short Title Case labels required for presentation
  (``Food``, ``Total Expenses``, ``Budget Usage %``). Labels such as
  ``Work/Internship`` and ``Budget Usage %`` are *not* valid Python identifiers
  and need quoting in SQL, which is exactly why the canonical layer exists.

Stage 3 leaves the money fields as ordinal range labels (``"1000 - 2000 ₹"``),
so **Stage 4 owns the label-to-number conversion outright** - it is
self-contained and does not import from ``cleaner.py``. That keeps the open-ended
midpoint rule defined in exactly one place.
"""

from __future__ import annotations

import re
import sys
import unicodedata
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]

CLEANED_DATA_PATH = PROJECT_ROOT / "data" / "processed" / "cleaned_student_budget.csv"
PROCESSED_DATA_PATH = PROJECT_ROOT / "data" / "processed" / "transformed_student_budget.csv"

# --------------------------------------------------------------------------- #
# Schema
# --------------------------------------------------------------------------- #

#: Long Stage-3 column -> short canonical identifier.
COLUMN_SHORTEN_MAP: Dict[str, str] = {
    "food_expenses_monthly": "food",
    "transport_expenses_monthly": "transport",
    "academic_expenses_monthly": "academic",
    "other_expenses_monthly": "other",
    "monthly_allowance": "allowance",
    "part_time_work_internship": "work_internship",
    "residential_background": "background",
    "accommodation_type": "accommodation",
    "top_academic_spending_area": "study_focus",
    "track_expenses_digitally": "track_digitally",
    "maintain_budget_plan": "budget_plan",
    "total_expenses_monthly": "total_expenses",
}

#: Canonical identifier -> Title Case label used in the exported CSV.
DISPLAY_NAMES: Dict[str, str] = {
    "timestamp": "Timestamp",
    "student_name": "Student Name",
    "gender": "Gender",
    "degree": "Degree",
    "year_of_study": "Year of Study",
    "accommodation": "Accommodation",
    "background": "Background",
    "work_internship": "Work/Internship",
    "specialization": "Specialization",
    "food": "Food",
    "transport": "Transport",
    "academic": "Academic",
    "other": "Other",
    "total_expenses": "Total Expenses",
    "allowance": "Allowance",
    "savings": "Savings",
    "budget_usage_pct": "Budget Usage %",
    "preferred_payment": "Preferred Payment",
    "upi_app": "UPI App",
    "track_digitally": "Track Digitally",
    "budget_plan": "Budget Plan",
    "study_focus": "Study Focus",
}

#: Canonical columns Stage 4 *derives* rather than reads. ``total_expenses`` is
#: always recomputed from the four components, so it is an optional passthrough:
#: a Stage 3 frame may or may not already carry it, and a Stage 4 frame always
#: will. Only genuinely required inputs are validated in shorten_column_names().
DERIVED_COLUMNS: Tuple[str, ...] = ("total_expenses", "savings", "budget_usage_pct")

#: Financial fields moved next to the metrics derived from them, so the exported
#: file reads as one block for the dashboard. Descriptive fields follow.
CANONICAL_COLUMNS: Tuple[str, ...] = (
    "timestamp",
    "student_name",
    "gender",
    "degree",
    "year_of_study",
    "accommodation",
    "background",
    "work_internship",
    "specialization",
    "food",
    "transport",
    "academic",
    "other",
    "total_expenses",
    "allowance",
    "savings",
    "budget_usage_pct",
    "preferred_payment",
    "upi_app",
    "track_digitally",
    "budget_plan",
    "study_focus",
)

#: Numeric money fields after the short rename.
MONEY_COLUMNS: Tuple[str, ...] = ("food", "transport", "academic", "other", "allowance")

#: The Stage 3 source names of those same money fields, i.e. the columns that
#: arrive as range labels. ``total_expenses_monthly`` is deliberately absent -
#: Stage 3 v1 does not emit it and Stage 4 derives the total itself.
MONEY_SOURCE_COLUMNS: Tuple[str, ...] = (
    "food_expenses_monthly",
    "transport_expenses_monthly",
    "academic_expenses_monthly",
    "other_expenses_monthly",
    "monthly_allowance",
)

#: The four spending components that roll up into the totals.
EXPENSE_COLUMNS: Tuple[str, ...] = ("food", "transport", "academic", "other")

TOTAL_EXPENSES_COLUMN = "total_expenses"
SAVINGS_COLUMN = "savings"
BUDGET_USAGE_COLUMN = "budget_usage_pct"

BUDGET_USAGE_DECIMALS = 1

# --------------------------------------------------------------------------- #
# Money labels -> numbers
# --------------------------------------------------------------------------- #

#: Open-ended buckets. ``"< 500"`` -> 250 is the Stage-3 rule (midpoint of
#: [0, 500], i.e. 0.5x the bound). Stage 4 specifies ``"> 5000"`` -> 6250, which
#: is 1.25x the bound, so the two open-ended multipliers are deliberately NOT
#: symmetric. 13.9% of money cells are open-ended, so this is the single most
#: load-bearing constant in the pipeline - see the Stage 3 note in the README
#: handoff below. Kept as a named constant so it is a one-line change.
OPEN_LOW_UPPER_MULTIPLIER = 0.5
OPEN_HIGH_UPPER_MULTIPLIER = 1.25

#: Currency symbols and mojibake renderings of the rupee sign (UTF-8 bytes
#: E2 82 B9 mis-decoded as cp1252/latin-1). Artifact removal runs BEFORE NFKC
#: normalisation, because NFKC folds U+00B9 (SUPERSCRIPT ONE) into the ASCII
#: digit "1" and would rewrite the mojibake before it could be matched. Written
#: as escapes so the source stays pure ASCII.
CURRENCY_ARTIFACTS: Tuple[str, ...] = (
    "₹",  # ₹  U+20B9 INDIAN RUPEE SIGN
    "â‚¹",  # â‚¹  cp1252 mis-decode
    "â,¹",  # variant seen in hand-edited CSVs
    "â¹",  # â¹  latin-1 mis-decode
    "؋",  # ؋  U+060B ARABIC RUPEE SIGN
    "Rs.",
    "Rs",
    "INR",
    "$",
    "€",
    "£",
)

_MONEY_PATTERN = re.compile(
    r"^\s*(?:<\s*(?P<below>[\d.,]+)|>\s*(?P<above>[\d.,]+)|(?P<low>[\d.,]+)\s*-\s*(?P<high>[\d.,]+))\s*$"
)


def strip_currency_artifacts(value: object) -> str:
    """Remove currency symbols, mojibake and stray whitespace from a money label."""
    if not isinstance(value, str):
        return ""

    def drop_known(text: str) -> str:
        for artifact in CURRENCY_ARTIFACTS:
            text = text.replace(artifact, " ")
        return text

    text = drop_known(value)
    text = unicodedata.normalize("NFKC", text)
    text = drop_known(text)
    # Spreadsheet apps promote a hyphen to an en/em dash or a minus sign when
    # copying ranges, so fold every dash-like code point back to "-".
    text = re.sub(r"[‐-―−]", "-", text)
    # Keep only what a range label can legitimately contain: digits, thousands
    # separators, the comparison operators and the range dash.
    text = re.sub(r"[^\d\s<>.,\-]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _to_number(token: str) -> Optional[float]:
    cleaned = token.replace(",", "")
    try:
        return float(cleaned)
    except ValueError:
        return None


def parse_money_midpoint(value: object) -> Optional[float]:
    """Convert a money range label (or a number) into a single numeric midpoint.

    ``"1000 - 2000 ₹"`` -> ``1500.0``, ``"> 5000 ₹"`` -> ``6250.0``,
    ``"< 1000 ₹"`` -> ``500.0``. Numbers pass through unchanged, which makes the
    function safe to apply to an already-converted column.
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, (int, float)) and not pd.isna(value):
        return float(value)

    text = strip_currency_artifacts(value)
    if not text:
        return None

    match = _MONEY_PATTERN.match(text)
    if match is None:
        return None

    groups = match.groupdict()
    if groups["below"] is not None:
        bound = _to_number(groups["below"])
        return None if bound is None else bound * OPEN_LOW_UPPER_MULTIPLIER
    if groups["above"] is not None:
        bound = _to_number(groups["above"])
        return None if bound is None else bound * OPEN_HIGH_UPPER_MULTIPLIER

    low = _to_number(groups["low"])
    high = _to_number(groups["high"])
    if low is None or high is None:
        return None
    if high < low:
        low, high = high, low
    return (low + high) / 2.0


def to_numeric_money(df: pd.DataFrame, columns: Sequence[str] = MONEY_COLUMNS) -> pd.DataFrame:
    """Coerce money columns to numbers, accepting either labels or numerics.

    Stage 3 already emitted these as ``Int64``, so on the real pipeline this is
    a no-op pass-through. It is kept so the transformer is idempotent and still
    correct if it is ever pointed at a partially-cleaned frame. An unparseable
    cell raises rather than silently becoming 0, which would read as a genuine
    zero spend.
    """
    out = df.copy()
    for column in columns:
        if column not in out.columns:
            raise KeyError(f"Expected money column {column!r} in the frame.")
        if pd.api.types.is_numeric_dtype(out[column]):
            out[column] = out[column].astype("Int64")
            continue
        values = out[column].map(parse_money_midpoint)
        if values.isna().any():
            bad = out.loc[values.isna(), column].dropna().unique()
            raise ValueError(f"Unparseable money labels in {column!r}: {list(bad)[:5]}")
        if not (values.dropna() % 1 == 0).all():
            raise ValueError(f"Non-integral midpoint produced in {column!r}.")
        out[column] = values.astype("Int64")
    return out


# --------------------------------------------------------------------------- #
# Renaming
# --------------------------------------------------------------------------- #


def shorten_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Map the long Stage-3 names onto short canonical identifiers.

    Only columns that are actually present are renamed, so this is safe to
    re-apply to an already-transformed frame - re-running a stage on its own
    output should be a no-op, not an error.
    """
    applicable = {k: v for k, v in COLUMN_SHORTEN_MAP.items() if k in df.columns}
    renamed = df.rename(columns=applicable)

    expected = (set(COLUMN_SHORTEN_MAP.values()) - set(DERIVED_COLUMNS)) | set(MONEY_COLUMNS)
    required = sorted(expected - set(renamed.columns))
    if required:
        raise KeyError(
            f"Input is neither a Stage 3 nor a Stage 4 frame; missing columns: {required}. "
            f"transform_data() expects the Stage 3 output (see load_cleaned_data)."
        )
    return renamed


def apply_display_names(df: pd.DataFrame) -> pd.DataFrame:
    """Rename canonical columns to their Title Case display labels."""
    unknown = sorted(set(df.columns) - set(DISPLAY_NAMES))
    if unknown:
        raise KeyError(f"Columns without a display name: {unknown}")
    return df.rename(columns=DISPLAY_NAMES)


def order_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Reorder to the canonical layout, appending any unmapped extra columns."""
    ordered: List[str] = [col for col in CANONICAL_COLUMNS if col in df.columns]
    extras: List[str] = [col for col in df.columns if col not in ordered]
    return df[ordered + extras]


# --------------------------------------------------------------------------- #
# Derived financial metrics
# --------------------------------------------------------------------------- #


def add_total_expenses(
    df: pd.DataFrame,
    columns: Sequence[str] = EXPENSE_COLUMNS,
    total_column: str = TOTAL_EXPENSES_COLUMN,
) -> pd.DataFrame:
    """``Total Expenses`` = Food + Transport + Academic + Other.

    ``min_count`` makes a row with any missing component total to ``NA`` rather
    than a misleading partial sum.
    """
    missing = [column for column in columns if column not in df.columns]
    if missing:
        raise KeyError(f"Cannot total expenses, missing columns: {missing}")
    out = df.copy()
    out[total_column] = out[list(columns)].sum(axis=1, min_count=len(columns)).astype("Int64")
    return out


def add_savings(
    df: pd.DataFrame,
    allowance_column: str = "allowance",
    total_column: str = TOTAL_EXPENSES_COLUMN,
    savings_column: str = SAVINGS_COLUMN,
) -> pd.DataFrame:
    """``Savings`` = Allowance - Total Expenses.

    Negative values are **kept, not clamped**. In this dataset every respondent
    overspends, so a clamp at zero would silently erase the only signal the
    column carries.
    """
    for column in (allowance_column, total_column):
        if column not in df.columns:
            raise KeyError(f"Cannot compute savings, missing column {column!r}.")
    out = df.copy()
    out[savings_column] = (out[allowance_column] - out[total_column]).astype("Int64")
    return out


def add_budget_usage(
    df: pd.DataFrame,
    allowance_column: str = "allowance",
    total_column: str = TOTAL_EXPENSES_COLUMN,
    usage_column: str = BUDGET_USAGE_COLUMN,
    decimals: int = BUDGET_USAGE_DECIMALS,
) -> pd.DataFrame:
    """``Budget Usage %`` = (Total Expenses / Allowance) * 100.

    A zero allowance yields ``NA`` rather than ``inf`` or a divide-by-zero.
    Values are intentionally left uncapped: >100% is the actual finding here.
    """
    for column in (allowance_column, total_column):
        if column not in df.columns:
            raise KeyError(f"Cannot compute budget usage, missing column {column!r}.")

    out = df.copy()
    allowance = out[allowance_column].astype("Float64")
    total = out[total_column].astype("Float64")
    ratio = (total / allowance.where(allowance != 0)) * 100.0
    out[usage_column] = ratio.round(decimals).astype("Float64")
    return out


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #


def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """Transform the cleaned dataset into the short, display-ready schema.

    Steps, in order:

    1. Rename long columns to short canonical identifiers.
    2. Coerce the money columns to numbers (no-op when Stage 3 already did).
    3. Recompute ``total_expenses`` from the four components.
    4. Derive ``savings`` (Allowance - Total Expenses) and ``budget_usage_pct``.
    5. Reorder into the canonical layout.

    Returns canonical ``snake_case`` columns; use :func:`apply_display_names`
    for the Title Case presentation layer.

    Parameters
    ----------
    df:
        The Stage 3 output, as loaded from ``cleaned_student_budget.csv``.

    Returns
    -------
    pandas.DataFrame
        A new frame with short column names and a reset index. The input is not
        modified.
    """
    transformed = shorten_column_names(df)
    transformed = to_numeric_money(transformed)
    transformed = add_total_expenses(transformed)
    transformed = add_savings(transformed)
    transformed = add_budget_usage(transformed)
    transformed = order_columns(transformed)
    return transformed.reset_index(drop=True)


def load_cleaned_data(path: Optional[Path] = None) -> pd.DataFrame:
    """Load the Stage 3 output."""
    source = Path(path) if path is not None else CLEANED_DATA_PATH
    if not source.is_file():
        raise FileNotFoundError(f"Stage 3 output not found: {source}")
    return pd.read_csv(source)


def export_transformed_data(df: pd.DataFrame, path: Optional[Path] = None) -> Path:
    """Write the transformed frame with Title Case headers, creating dirs as needed."""
    destination = Path(path) if path is not None else PROCESSED_DATA_PATH
    destination.parent.mkdir(parents=True, exist_ok=True)
    apply_display_names(df).to_csv(destination, index=False, encoding="utf-8")
    return destination


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Load the cleaned data, transform it, and export the presentation CSV."""
    source = CLEANED_DATA_PATH
    cleaned = load_cleaned_data(source)
    print(f"[stage 4] source      : {source.relative_to(PROJECT_ROOT)}")
    print(f"[stage 4] in shape    : {cleaned.shape[0]} rows x {cleaned.shape[1]} columns")

    transformed = transform_data(cleaned)
    print(f"[stage 4] out shape   : {transformed.shape[0]} rows x {transformed.shape[1]} columns")
    print(f"[stage 4] total       : {transformed[TOTAL_EXPENSES_COLUMN].min()} - "
          f"{transformed[TOTAL_EXPENSES_COLUMN].max()} INR")
    print(f"[stage 4] savings     : {int((transformed[SAVINGS_COLUMN] < 0).sum())} negative row(s)")
    print(f"[stage 4] usage %     : {transformed[BUDGET_USAGE_COLUMN].min():.1f} - "
          f"{transformed[BUDGET_USAGE_COLUMN].max():.1f}")

    destination = export_transformed_data(transformed)
    print(f"[stage 4] wrote       : {destination.relative_to(PROJECT_ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
