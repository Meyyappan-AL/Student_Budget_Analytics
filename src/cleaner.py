"""Stage 3 - Data cleaning for the Student Budget Analytics pipeline.

Normalises the raw Google-Forms export into a database-standard, snake_case
schema and writes the result to ``data/processed/cleaned_student_budget.csv``.

Usage::

    python src/cleaner.py
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Dict, List, Optional, Sequence

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]

#: Candidate locations for the raw Google-Forms export, in priority order.
RAW_DATA_CANDIDATES: Sequence[Path] = (
    PROJECT_ROOT / "data" / "raw" / "Student Budget Analytics Raw Data.xlsx",
    PROJECT_ROOT / "Data" / "Student Budget Analytics Raw Data.xlsx",
    PROJECT_ROOT / "data" / "raw" / "Student Budget Analytics Raw Data.xls",
)

PROCESSED_DATA_PATH = PROJECT_ROOT / "data" / "processed" / "cleaned_student_budget.csv"

#: Raw form label -> database-standard column name. Keys are whitespace-normalised,
#: so ``clean_data`` must strip the frame's columns *before* applying this map
#: (e.g. the source stores ``"Residential Background "`` with a trailing space).
COLUMN_RENAME_MAP: Dict[str, str] = {
    "Timestamp": "timestamp",
    "Student Name": "student_name",
    "Gender": "gender",
    "Degree": "degree",
    "Year of Study": "year_of_study",
    "Accommodation Type": "accommodation_type",
    "Residential Background": "residential_background",
    "Part-time Work/Internship": "part_time_work_internship",
    "Food Expenses  (Monthly)": "food_expenses_monthly",
    "Transport Expenses  (Monthly)": "transport_expenses_monthly",
    "Academic Expenses  (Monthly)": "academic_expenses_monthly",
    "Other Expenses (Monthly)": "other_expenses_monthly",
    "Preferred Payment": "preferred_payment",
    "If UPI which app you use?": "upi_app",
    "Do you track your expenses digitally?": "track_expenses_digitally",
    "Monthly Allowance": "monthly_allowance",
    "Do you maintain a monthly budget plan?": "maintain_budget_plan",
    "In which study-related areas do you spend the most each month?": "top_academic_spending_area",
}

#: The form splits the student's course across up to six separate questions.
COURSE_TITLE_COLUMNS: Sequence[str] = (
    "Course Title",
    "Course Title 2",
    "Course Title 3",
    "Course Title 4",
    "Course Title 5",
    "Course Title 6",
)
SPECIALIZATION_COLUMN = "specialization"

#: A row is considered a corrupted/empty trailing row only if every key column is
#: blank. Single-field blanks elsewhere are legitimate partial responses.
ROW_IDENTITY_SUBSET: Sequence[str] = ("Timestamp", "Student Name")

UPI_NOT_APPLICABLE = "Not Applicable"

#: Target column order after cleaning. ``specialization`` keeps the position of the
#: original course-title block so the schema stays human-readable.
CLEANED_COLUMNS: Sequence[str] = (
    "timestamp",
    "student_name",
    "gender",
    "degree",
    "year_of_study",
    "accommodation_type",
    "residential_background",
    "part_time_work_internship",
    SPECIALIZATION_COLUMN,
    "food_expenses_monthly",
    "transport_expenses_monthly",
    "academic_expenses_monthly",
    "other_expenses_monthly",
    "preferred_payment",
    "upi_app",
    "track_expenses_digitally",
    "monthly_allowance",
    "maintain_budget_plan",
    "top_academic_spending_area",
)


def resolve_raw_data_path(candidates: Optional[Sequence[Path]] = None) -> Path:
    """Return the first existing raw-data candidate.

    The repository currently ships the export at ``Data/`` rather than the
    ``data/raw/`` layout used by the pipeline, so several candidates are probed
    rather than hard-coding a single path.
    """
    for candidate in candidates or RAW_DATA_CANDIDATES:
        if candidate.is_file():
            return candidate
    probed = "\n  - ".join(str(c) for c in (candidates or RAW_DATA_CANDIDATES))
    raise FileNotFoundError(f"Could not locate the raw data file. Probed:\n  - {probed}")


def load_raw_data(path: Optional[Path] = None) -> pd.DataFrame:
    """Read the raw survey export from the first sheet of the workbook."""
    source = Path(path) if path is not None else resolve_raw_data_path()
    return pd.read_excel(source, sheet_name=0)


def strip_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Trim leading/trailing whitespace from every column label."""
    renamed = {col: str(col).strip() for col in df.columns}
    if len(set(renamed.values())) != len(renamed):
        duplicates = sorted({name for name in renamed.values() if list(renamed.values()).count(name) > 1})
        raise ValueError(f"Column names are not unique after stripping whitespace: {duplicates}")
    return df.rename(columns=renamed)


def drop_corrupted_rows(df: pd.DataFrame, subset: Sequence[str] = ROW_IDENTITY_SUBSET) -> pd.DataFrame:
    """Drop trailing rows whose identity columns are all empty.

    Treating ``NaN``, ``None`` and whitespace-only strings as empty means the rule
    survives exports that write blank cells as ``""`` instead of leaving them null.
    """
    missing = [col for col in subset if col not in df.columns]
    if missing:
        raise KeyError(f"Cannot identify corrupted rows, missing columns: {missing}")

    keys = df[list(subset)].astype("string").apply(lambda col: col.str.strip())
    keep = (keys.fillna("") != "").any(axis=1)
    return df.loc[keep].copy()


def validate_rename_map(df: pd.DataFrame) -> None:
    """Fail loudly if any source column expected by the rename map is absent.

    A partial rename would silently publish un-normalised column names, so this
    is treated as a hard error rather than a warning.
    """
    missing = sorted(set(COLUMN_RENAME_MAP) - set(df.columns))
    if missing:
        raise KeyError(f"Source columns missing from COLUMN_RENAME_MAP: {missing}")


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Apply the snake_case rename map to the whitespace-normalised columns."""
    validate_rename_map(df)
    return df.rename(columns=COLUMN_RENAME_MAP)


def _join_course_answers(row: pd.Series) -> Optional[str]:
    """Join one row's non-empty course answers, de-duplicated and order-preserving."""
    values = [value for value in row if pd.notna(value) and value.strip()]
    unique = list(dict.fromkeys(value.strip() for value in values))
    return "; ".join(unique) if unique else pd.NA


def consolidate_specialization(df: pd.DataFrame) -> pd.DataFrame:
    """Collapse the six course-title questions into one ``specialization`` column.

    Respondents may answer more than one course question, so all non-empty
    answers are de-duplicated (preserving form order) and joined with ``"; "``.
    A response with no course answer at all becomes ``pd.NA`` rather than an
    empty string, so it stays a genuine unknown.
    """
    available = [col for col in COURSE_TITLE_COLUMNS if col in df.columns]
    if not available:
        raise KeyError(f"None of the course title columns are present: {list(COURSE_TITLE_COLUMNS)}")

    answers = df[available].astype("string")
    out = df.drop(columns=available).copy()
    out[SPECIALIZATION_COLUMN] = answers.apply(_join_course_answers, axis=1)
    return out


def fill_upi_app(df: pd.DataFrame) -> pd.DataFrame:
    """Fill missing/blank ``upi_app`` values with ``'Not Applicable'``.

    UPI is only asked when the preferred payment method is UPI, so a blank is
    meaningful: it means the question did not apply to that respondent.
    """
    if "upi_app" not in df.columns:
        raise KeyError("Expected an 'upi_app' column before filling missing values.")
    out = df.copy()
    stripped = out["upi_app"].astype("string").str.strip()
    out["upi_app"] = stripped.where(stripped.notna() & stripped.ne(""), UPI_NOT_APPLICABLE)
    return out


def order_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Reorder to the canonical schema, appending any unmapped extra columns."""
    ordered: List[str] = [col for col in CLEANED_COLUMNS if col in df.columns]
    extras: List[str] = [col for col in df.columns if col not in ordered]
    return df[ordered + extras]


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean the raw survey export into the database-standard schema.

    Steps, in order:

    1. Trim leading/trailing whitespace from column names.
    2. Drop corrupted/empty trailing rows (blank ``Timestamp`` *and* ``Student Name``).
    3. Rename all columns to database-standard snake_case.
    4. Consolidate the six course-title columns into ``specialization``.
    5. Fill missing ``upi_app`` values with ``'Not Applicable'``.

    Parameters
    ----------
    df:
        The raw survey export exactly as read from the workbook.

    Returns
    -------
    pandas.DataFrame
        A new frame in snake_case schema, with a reset index and the canonical
        column order. The input frame is not modified.
    """
    cleaned = strip_column_names(df)
    cleaned = drop_corrupted_rows(cleaned)
    cleaned = rename_columns(cleaned)
    cleaned = consolidate_specialization(cleaned)
    cleaned = fill_upi_app(cleaned)
    cleaned = order_columns(cleaned)
    return cleaned.reset_index(drop=True)


def export_cleaned_data(cleaned: pd.DataFrame, path: Optional[Path] = None) -> Path:
    """Write the cleaned frame to CSV, creating the output directory if needed."""
    destination = Path(path) if path is not None else PROCESSED_DATA_PATH
    destination.parent.mkdir(parents=True, exist_ok=True)
    cleaned.to_csv(destination, index=False, encoding="utf-8")
    return destination


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Load the raw export, clean it, and export it to ``data/processed/``."""
    raw_path = resolve_raw_data_path()
    raw = load_raw_data(raw_path)
    print(f"[stage 3] source      : {raw_path.relative_to(PROJECT_ROOT)}")
    print(f"[stage 3] raw shape   : {raw.shape[0]} rows x {raw.shape[1]} columns")

    cleaned = clean_data(raw)
    dropped = raw.shape[0] - cleaned.shape[0]
    print(f"[stage 3] dropped     : {dropped} corrupted/empty trailing row(s)")
    print(f"[stage 3] clean shape : {cleaned.shape[0]} rows x {cleaned.shape[1]} columns")

    destination = export_cleaned_data(cleaned)
    print(f"[stage 3] wrote       : {destination.relative_to(PROJECT_ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
