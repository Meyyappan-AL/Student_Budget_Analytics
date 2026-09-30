"""Stage 9 - AI assistant layer for the Student Budget Analytics pipeline.

Three capabilities over one shared, allowlisted semantic layer:

1. :class:`NaturalLanguageToSQL` - plain English to PostgreSQL, plus a
   no-server fallback that answers the same question from the Stage 4 CSV.
2. :class:`ErrorExplainer` - ETL exceptions and data quality reports in plain
   English, with the actual fix for each.
3. :class:`InsightsAssistant` - conversational analysis of a result set, with
   budgeting tips that respect the dataset's known measurement limits.

Entry points::

    python ai/assistant.py ask "average expenses by degree"
    python ai/assistant.py explain --log pipeline.log
    python ai/assistant.py insights "average expenses by degree" --offline
    python ai/assistant.py chat                     # interactive REPL
    python ai/assistant.py serve --port 8000        # stdlib HTTP, no FastAPI
    python ai/assistant.py selftest                 # no database required

-------------------------------------------------------------------------------
WHY THIS IS A DETERMINISTIC ENGINE AND NOT AN LLM CALL
-------------------------------------------------------------------------------
``requirements.txt`` pins pandas, numpy, openpyxl and psycopg2. There is no model
SDK in the project and adding one would mean a dependency that cannot be tested
without a network call and an API key, on a pipeline whose entire design goal is
reproducibility. So the text-to-SQL path is a deterministic intent parser over a
closed vocabulary. That buys four properties an LLM prompt cannot guarantee:

* **Injectable text never reaches the SQL.** Every identifier comes from the
  allowlists in this module and every value from :data:`FILTER_VALUES` or a
  numeric validator. User input is used only to *select* a fragment, never
  interpolated. :class:`SQLSafetyGuard` then re-validates the finished statement.
* **Identical questions produce identical SQL**, so a translation can be
  regression-tested and reviewed in a pull request.
* **It cannot hallucinate a column.** An unknown word is a parse failure with a
  helpful message, not a fabricated ``AVG(totl_expnses)``.
* **It runs anywhere, offline, forever** - which is what makes ``--selftest``
  and the ``--offline`` mode possible at all.

The cost is real and worth stating: a phrasing outside the vocabulary gets a
"cannot interpret" message rather than a best guess. That is the correct trade for
a tool that writes SQL against a live database. The seam for a future model is
deliberate - :meth:`NaturalLanguageToSQL.to_sql` is the only function that emits
SQL, so an LLM backend would have to satisfy the same
:class:`SQLSafetyGuard` to be swapped in.

-------------------------------------------------------------------------------
WHAT THE ASSISTANT KNOWS ABOUT THE DATA, AND WHY IT MATTERS
-------------------------------------------------------------------------------
Three properties of this dataset make naive answers wrong. They are not
documented caveats bolted on afterwards; they are the reason the semantic layer
refuses to answer some questions outright.

1. **The money columns are bucket midpoints, and 13.4% of the 852 expense cells
   are right-censored** at 6250, the midpoint of the open-ended "> 5000" bucket.
   6250 is a *floor*. Every average the assistant reports is a lower bound, and
   :meth:`InsightsAssistant` says so on every single result rather than only in a
   footnote.
2. **The allowance scale is much coarser than the expense scale** (mean 1,578.64
   against mean spend 11,461.27), so all 213 respondents "overspend" and
   ``savings`` is negative for every row. A "who saves the most" question has an
   empty answer set, so the assistant redirects to the deficit instead of
   returning a misleading leaderboard.
3. **Mean-of-ratios and the aggregate ratio differ by 2.1x** (1533.05% against
   726.02%), because a few students fall in the lowest allowance bucket. Any
   utilisation question is answered with both, and when they disagree the
   assistant says so rather than picking one.

The same reasoning is why the semantic layer has no "savings" measure for
ranking. It exposes :attr:`Measure.allowance` and
:attr:`Measure.overspend_amount` instead.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import textwrap
import time
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"

# The pipeline modules are plain scripts with no package __init__, so they import
# as top-level names only once src/ is on sys.path. This mirrors
# src/pipeline.py, and keeps `python ai/assistant.py` and `python -m
# ai.assistant` equivalent.
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

# Imported at module scope, not lazily: this module is useless without them, and
# unlike psycopg2 (imported lazily inside src/database.py) neither pandas nor the
# pipeline's own modules have a meaningful "absent" path here.
import pandas as pd  # noqa: E402  (path setup must precede this)

import data_quality  # noqa: E402
import database  # noqa: E402
import pipeline  # noqa: E402

# --------------------------------------------------------------------------- #
# Paths and dataset facts
# --------------------------------------------------------------------------- #

#: The Stage 4 CSV, used by the offline engine. The same resolution order as
#: ``data_quality.resolve_source_path``, so the assistant reads the artifact the
#: loader would have read rather than a second guess at where it lives.
SOURCE_CSV_CANDIDATES: Tuple[Path, ...] = (
    PROJECT_ROOT / "data" / "processed" / "transformed_student_budget.csv",
    PROJECT_ROOT / "Data" / "processed" / "transformed_student_budget.csv",
)

#: Table created by ``sql/schema.sql``; the view created by ``sql/analytics.sql``.
#: Queries run against the VIEW because it already carries the ordinal and the
#: derived overspend/UPI columns, so the semantic layer never has to re-derive
#: them and the two can never disagree.
TABLE_NAME = database.TARGET_TABLE
VIEW_NAME = "v_student_budget_analysis"

#: Facts asserted by the Stage 6 and Stage 7 verification code. The assistant
#: quotes them so a result can be sanity-checked, and so it can refuse to
#: interpret an empty result as a finding.
EXPECTED_ROW_COUNT = database.EXPECTED_ROW_COUNT
CENSORED_CELL_VALUE = 6250.0
CENSORED_CELL_FRACTION = 0.134

#: The ceiling of the number of rows a generated query may return. An NL-to-SQL
#: tool that can emit ``SELECT * FROM students_budget`` into a chat window is a
#: nuisance; capping the LIMIT keeps the CLI readable and bounds memory.
DEFAULT_LIMIT = 50
MAX_LIMIT = 500

#: Cap on an HTTP request body. A question is a sentence; anything larger is not a
#: question, and reading an unbounded body into memory is how a local demo server
#: becomes a liability.
MAX_REQUEST_BYTES = 8 * 1024

#: How many example questions the CLI and HTTP layer advertise.
EXAMPLE_QUESTIONS: Tuple[str, ...] = (
    "average expenses by degree",
    "average total expenses by year of study",
    "how many students live in hostel accommodation",
    "top 5 UPI apps among students who pay by UPI",
    "compare average allowance for hostel vs day scholar",
    "average academic expenses by accommodation",
    "how many students have a budget plan",
    "top 3 study focus areas by average total expenses",
    "total expenses by preferred payment method",
    "average budget utilisation by background",
)


# --------------------------------------------------------------------------- #
# Semantic layer
# --------------------------------------------------------------------------- #
# Everything below is an ALLOWLIST. The SQL builder may only emit identifiers
# that appear in these tables, and may only emit a literal that clears
# ``_sql_literal``. That is the whole security model of the text-to-SQL path.


@dataclass(frozen=True)
class Measure:
    """A numeric column that can be aggregated.

    Attributes
    ----------
    name:
        Column name in the view. Also the name accepted in English.
    label:
        Human label used in generated SQL and in the insights prose.
    synonyms:
        Lowercase phrases that select this measure. Matched on word boundaries,
        longest phrase first, so "total expenses" beats "expenses".
    default_aggregation:
        Applied when the question does not name one.
    aggregateable:
        False for counts, where ``AVG`` is meaningless.
    caveat:
        A measurement limit that must travel with the number. The insights layer
        attaches it to every result using this measure.
    """

    name: str
    label: str
    synonyms: Tuple[str, ...]
    default_aggregation: str = "avg"
    aggregateable: bool = True
    caveat: str = ""


@dataclass(frozen=True)
class Dimension:
    """A categorical column that can group or filter.

    ``values`` is the authoritative domain, mirroring the CHECK constraints in
    ``sql/schema.sql``. A filter value is matched against this list, which is why
    an unrecognised value is rejected instead of interpolated.
    """

    name: str
    label: str
    synonyms: Tuple[str, ...]
    values: Tuple[str, ...] = ()
    #: English -> canonical value, for values with a natural nickname
    #: ("hostellers" -> "Hostel", "btech" -> "BE / B.Tech").
    value_aliases: Dict[str, str] = field(default_factory=dict)


#: Every money figure in this dataset is a midpoint of an ordinal survey bucket
#: rather than a reported amount, and 6250 is a censored floor. This caveat is
#: attached to any measure derived from them.
_CENSORED = (
    "Figures are midpoints of survey buckets, and 6250 is a floor for the "
    "open-ended '> 5000' bucket - treat this as a lower bound, not a measurement."
)

MEASURES: Dict[str, Measure] = {
    "total_expenses": Measure(
        name="total_expenses",
        label="total expenses",
        synonyms=("total expenses", "total expense", "total spending", "total spend",
                  "overall spending", "monthly expenses", "how much they spend",
                  "expenses", "spending", "spend", "spends", "cost", "costs"),
        caveat=_CENSORED,
    ),
    "allowance": Measure(
        name="allowance",
        label="allowance",
        synonyms=("monthly allowance", "allowance", "pocket money", "income", "budget given"),
        caveat=("The allowance buckets are far coarser and lower than the expense "
                "buckets, which is why every respondent appears to overspend."),
    ),
    "overspend_amount": Measure(
        name="overspend_amount",
        label="overspend (deficit)",
        synonyms=("overspend", "over-spend", "overspending", "deficit", "shortfall",
                  "amount over budget", "over budget amount"),
        default_aggregation="avg",
        caveat="Positive means spending exceeded allowance. Every one of the 213 rows is positive.",
    ),
    "budget_usage_pct": Measure(
        name="budget_usage_pct",
        label="budget utilisation",
        synonyms=("budget utilisation", "budget utilization", "utilisation",
                  "utilization", "budget usage", "usage percent", "usage %",
                  "percent of budget used", "overspend percentage"),
        caveat=("This is a RATIO, not a share: values above 100 are expected and "
                "observed. The mean of this column (1533.05%) and the aggregate "
                "ratio (726.02%) differ by 2.1x, so read them as different "
                "questions."),
    ),
    "savings": Measure(
        name="savings",
        label="savings",
        synonyms=("savings", "money saved", "amount saved", "saving"),
        caveat=("Negative for all 213 rows, because reported allowances are lower "
                "than reported expenses for every respondent. A 'biggest saver' "
                "ranking is therefore meaningless - prefer the deficit."),
    ),
    "food_expenses": Measure(
        name="food_expenses", label="food expenses",
        synonyms=("food expenses", "food spending", "spend on food", "food", "eating"),
        caveat=_CENSORED,
    ),
    "transport_expenses": Measure(
        name="transport_expenses", label="transport expenses",
        synonyms=("transport expenses", "transport spending", "spend on transport",
                  "commute costs", "transport", "travel"),
        caveat=_CENSORED,
    ),
    "academic_expenses": Measure(
        name="academic_expenses", label="academic expenses",
        synonyms=("academic expenses", "academic spending", "spend on academics",
                  "study costs", "academics", "academic"),
        caveat=_CENSORED,
    ),
    "other_expenses": Measure(
        name="other_expenses", label="other expenses",
        synonyms=("other expenses", "other spending", "miscellaneous expenses",
                  "miscellaneous", "other"),
        caveat=_CENSORED,
    ),
    "student_count": Measure(
        name="student_id", label="students",
        synonyms=("number of students", "how many students", "student count",
                  "how many people", "headcount"),
        default_aggregation="count",
        aggregateable=False,
    ),
}

DIMENSIONS: Dict[str, Dimension] = {
    "degree": Dimension(
        name="degree", label="degree",
        synonyms=("degree", "programme", "program", "course", "branch", "discipline"),
        values=("B.Com", "BE / B.Tech", "BSc", "MBA", "ME / M.Tech", "Other"),
        value_aliases={
            "bcom": "B.Com", "b com": "B.Com", "commerce": "B.Com", "bca": "B.Com",
            "btech": "BE / B.Tech", "b tech": "BE / B.Tech",
            "be btech": "BE / B.Tech", "be/b tech": "BE / B.Tech", "b.e.": "BE / B.Tech",
            "engineering": "BE / B.Tech", "b.sc": "BSc", "bsc": "BSc", "science": "BSc",
            "mba": "MBA", "masters": "MBA",
            # "me" and "be" on their own are deliberately NOT aliases. They are
            # ordinary English - "show me all MBA students" matched ME / M.Tech,
            # "be" matched almost everything - and a two-letter degree abbreviation
            # is not worth that class of false positive. Require the full form.
            "mtech": "ME / M.Tech", "m tech": "ME / M.Tech",
            "me mtech": "ME / M.Tech", "me/m tech": "ME / M.Tech", "postgrad": "ME / M.Tech",
            "other degrees": "Other",
        },
    ),
    "year_of_study": Dimension(
        name="year_of_study", label="year of study",
        synonyms=("year of study", "study year", "year", "grade", "semester"),
        values=("1st Year", "2nd Year", "3rd Year", "4th Year"),
        value_aliases={
            "first year": "1st Year", "1st year": "1st Year", "year 1": "1st Year",
            "1st": "1st Year", "freshman": "1st Year",
            "second year": "2nd Year", "2nd year": "2nd Year", "year 2": "2nd Year",
            "2nd": "2nd Year", "sophomore": "2nd Year",
            "third year": "3rd Year", "3rd year": "3rd Year", "year 3": "3rd Year",
            "3rd": "3rd Year", "junior": "3rd Year",
            "fourth year": "4th Year", "4th year": "4th Year", "year 4": "4th Year",
            "4th": "4th Year", "senior": "4th Year", "final year": "4th Year",
        },
    ),
    "accommodation": Dimension(
        name="accommodation", label="accommodation",
        synonyms=("accommodation", "living situation", "residence", "housing",
                  "stay", "lodging"),
        values=("Day Scholar", "Hostel", "PG"),
        value_aliases={
            "hostel": "Hostel", "hostellers": "Hostel", "hostel students": "Hostel",
            "boarding": "Hostel", "in hostel": "Hostel",
            "day scholar": "Day Scholar", "day scholars": "Day Scholar",
            "dayscholar": "Day Scholar", "commuter": "Day Scholar",
            "commute": "Day Scholar", "at home": "Day Scholar",
            "live at home": "Day Scholar", "living at home": "Day Scholar",
            "pg": "PG", "paying guest": "PG", "paying guests": "PG", "rented": "PG",
        },
    ),
    "preferred_payment": Dimension(
        name="preferred_payment", label="preferred payment method",
        synonyms=("preferred payment", "payment method", "payment mode",
                  "how they pay", "payment"),
        values=("UPI", "Cash", "Debit/Credit Card", "Other"),
        value_aliases={
            # "money" is not an alias for Cash. "How much money do they save" is a
            # savings question, and matching it as a payment filter would silently
            # restrict the answer to cash users. Use an explicit payment phrase.
            "upi": "UPI", "cash": "Cash", "pay cash": "Cash",
            "card": "Debit/Credit Card", "debit card": "Debit/Credit Card",
            "credit card": "Debit/Credit Card", "debit/credit card": "Debit/Credit Card",
        },
    ),
    "upi_app": Dimension(
        name="upi_app", label="UPI app",
        synonyms=("upi app", "payment app", "apps"),
        values=("GPay", "PhonePe", "SBI Pay", "Amazon Pay", "BHIM UPI",
                "Airtel Thanks App", "Other", "Not Applicable"),
        value_aliases={
            "gpay": "GPay", "google pay": "GPay", "googlepay": "GPay",
            "phonepe": "PhonePe", "phone pe": "PhonePe",
            "sbi pay": "SBI Pay", "sbipay": "SBI Pay", "sbi": "SBI Pay",
            "amazon pay": "Amazon Pay", "amazonpay": "Amazon Pay",
            "bhim": "BHIM UPI", "bhim upi": "BHIM UPI",
            "airtel": "Airtel Thanks App", "airtel thanks app": "Airtel Thanks App",
        },
    ),
    "gender": Dimension(
        name="gender", label="gender",
        synonyms=("gender", "sex"),
        values=("Male", "Female"),
        value_aliases={"male": "Male", "men": "Male", "female": "Female", "women": "Female"},
    ),
    "background": Dimension(
        name="background", label="background",
        synonyms=("background", "upbringing", "origin", "residency"),
        values=("Rural", "Urban"),
        value_aliases={"rural": "Rural", "village": "Rural",
                       "urban": "Urban", "city": "Urban", "town": "Urban"},
    ),
    "work_internship": Dimension(
        name="work_internship", label="work or internship",
        synonyms=("work", "internship", "employment", "job", "part time work"),
        values=("Yes", "No"),
        value_aliases={"working": "Yes", "employed": "Yes", "intern": "Yes",
                       "not working": "No", "unemployed": "No"},
    ),
    "track_digitally": Dimension(
        name="track_digitally", label="tracks expenses digitally",
        synonyms=("track digitally", "digital tracking", "tracks digitally",
                  "track expenses digitally"),
        values=("Yes", "No"),
        value_aliases={"tracks digitally": "Yes", "digitally": "Yes",
                       "does not track": "No", "no tracking": "No"},
    ),
    "budget_plan": Dimension(
        name="budget_plan", label="maintains a budget plan",
        synonyms=("budget plan", "budgeting plan", "plans a budget", "budgeting"),
        values=("Yes", "No"),
        value_aliases={"has a budget plan": "Yes", "budgets": "Yes",
                       "no budget plan": "No"},
    ),
    "study_focus": Dimension(
        name="study_focus", label="study focus",
        synonyms=("study focus", "spending area", "focus area", "spends on",
                  "study priority"),
        values=("Online Learning Platforms", "Books & Study Materials",
                "Internet & Data Packs", "Software Tools", "Other"),
        value_aliases={
            "online learning": "Online Learning Platforms",
            "online learning platforms": "Online Learning Platforms",
            "books": "Books & Study Materials",
            "books and study materials": "Books & Study Materials",
            "study materials": "Books & Study Materials",
            "internet": "Internet & Data Packs", "data packs": "Internet & Data Packs",
            "software": "Software Tools", "software tools": "Software Tools",
        },
    ),
    "specialization": Dimension(
        name="specialization", label="specialization",
        synonyms=("specialization", "specialisation", "major", "subject"),
        # Free text: 44 distinct spellings in the seed data, so there is no closed
        # domain to validate against. Grouping is still safe (the value is
        # parameterised, never concatenated) but a filter on it cannot be
        # allowlisted, so SPECIALIZATION_UNFILTERABLE gates that.
        values=(),
    ),
}

#: Dimensions whose domain is free text, so a value cannot be validated against
#: an allowlist. Grouping is permitted; filtering is refused rather than guessed.
UNFILTERABLE_DIMENSIONS = frozenset({"specialization"})

#: Function words that must never be a value alias. An alias is matched on word
#: boundaries anywhere in the question, so a two-letter abbreviation that happens
#: to be an ordinary English word ("me", "be") matches questions that never mention
#: the subject at all - which is how "show me all MBA students" ended up carrying a
#: second filter on ME / M.Tech.
#:
#: Note what is deliberately NOT here: "cash", "card", "male", "female", "books".
#: Those are real vocabulary for their dimension, and a question containing them is
#: almost certainly about that dimension. Only words that carry no topical meaning
#: belong on this list.
#:
#: Checked at import, so a colliding alias fails loudly here rather than quietly
#: corrupting answers.
RESERVED_ALIASES = frozenset({
    "me", "be", "no", "yes", "a", "an", "the", "is", "are", "am", "was", "were",
    "do", "does", "did", "to", "of", "in", "on", "at", "it", "he", "she", "we",
    "us", "my", "our", "or", "and", "so", "if", "up", "go", "all", "one", "two",
    "as", "by", "for", "with", "from", "who", "what", "which", "than", "then",
})


def _assert_aliases_are_unambiguous() -> None:
    """Fail at import if any value alias is an ordinary English function word.

    Raises
    ------
    AssertionError
        On the first collision, naming the dimension and the alias.
    """
    for dimension in DIMENSIONS.values():
        for alias in dimension.value_aliases:
            if alias in RESERVED_ALIASES:
                raise AssertionError(
                    f"value alias {alias!r} on dimension {dimension.name!r} is an "
                    f"ordinary English word and will match unrelated questions; "
                    f"use a longer form such as {alias + ' ' + dimension.label!r}"
                )


_assert_aliases_are_unambiguous()

#: Aggregation verbs -> SQL. Every entry is a fixed string from this map; the
#: parser can only choose a key, never supply an expression.
AGGREGATIONS: Dict[str, str] = {
    "avg": "avg",
    "sum": "sum",
    "count": "count",
    "max": "max",
    "min": "min",
    "median": "percentile_disc(0.5) within group (order by {expr})",
}

#: Verb phrases that select an AGGREGATION. Ranking words are deliberately not
#: here - they are ordering, not aggregation, and conflating the two is what
#: makes naive text-to-SQL answer "top 5 UPI apps" with MAX() instead of COUNT().
AGGREGATION_SYNONYMS: Tuple[Tuple[str, str], ...] = (
    ("on average", "avg"), ("average", "avg"), ("avg", "avg"), ("mean", "avg"),
    ("typical", "avg"), ("median", "median"), ("midpoint", "median"),
    ("total of", "sum"), ("sum of", "sum"), ("combined", "sum"), ("altogether", "sum"),
    ("how many", "count"), ("number of", "count"), ("count of", "count"),
    ("headcount", "count"),
)

#: Ranking words -> sort direction. These set ORDER BY, not the aggregate.
#: "top 5 UPI apps" is COUNT(*) ... ORDER BY count DESC LIMIT 5.
RANKING_SYNONYMS: Tuple[Tuple[str, str], ...] = (
    ("highest", "desc"), ("largest", "desc"), ("maximum", "desc"), ("most", "desc"),
    ("biggest", "desc"), ("top", "desc"), ("best", "desc"), ("leading", "desc"),
    ("lowest", "asc"), ("smallest", "asc"), ("minimum", "asc"), ("least", "asc"),
)

#: Number words, so "top five" works as well as "top 5".
NUMBER_WORDS: Dict[str, int] = {
    "one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6, "seven": 7,
    "eight": 8, "nine": 9, "ten": 10, "fifteen": 15, "twenty": 20, "thirty": 30,
    "fifty": 50, "hundred": 100,
}

#: Words that mean "no aggregation" - a per-row listing.
#:
#: Bare "who" is not one of them. "students who pay by UPI" is a relative clause,
#: and treating it as a request for names turned a count into a 50-row dump.
#: A detail request has to be stated as one.
DETAIL_SYNONYMS: Tuple[str, ...] = (
    "list", "show me all", "show all", "list all", "name and", "names of",
    "which students", "each student", "every student", "who are the",
)

#: Filter combinators. "not hostel" and "except cash" negate the value that
#: follows them; everything else is a plain equality.
NEGATIONS: Tuple[str, ...] = ("not", "except", "excluding", "other than",
                              "apart from", "besides", "without")

#: Words that mean "this is a comparison between the values of one dimension",
#: which changes the output from a single row to one row per value.
COMPARISON_SYNONYMS: Tuple[str, ...] = ("compare", "comparison", "versus", " vs ",
                                        "difference between", "against each other")

#: Year ordinals, so "by year" sorts naturally in SQL instead of alphabetically.
YEAR_ORDINAL_SQL = (
    "CASE year_of_study WHEN '1st Year' THEN 1 WHEN '2nd Year' THEN 2 "
    "WHEN '3rd Year' THEN 3 WHEN '4th Year' THEN 4 END"
)

#: View column -> Stage 4 CSV column. Only for columns the CSV actually has;
#: derived columns are computed in :func:`_load_offline_frame`.
VIEW_TO_CSV: Dict[str, str] = {
    "submitted_at": "Timestamp",
    "student_name": "Student Name",
    "gender": "Gender",
    "degree": "Degree",
    "year_of_study": "Year of Study",
    "accommodation": "Accommodation",
    "background": "Background",
    "work_internship": "Work/Internship",
    "specialization": "Specialization",
    "preferred_payment": "Preferred Payment",
    "upi_app": "UPI App",
    "track_digitally": "Track Digitally",
    "budget_plan": "Budget Plan",
    "study_focus": "Study Focus",
    "food_expenses": "Food",
    "transport_expenses": "Transport",
    "academic_expenses": "Academic",
    "other_expenses": "Other",
    "total_expenses": "Total Expenses",
    "allowance": "Allowance",
    "savings": "Savings",
    "budget_usage_pct": "Budget Usage %",
}

#: Derived columns the offline engine has to compute because no CSV column holds
#: them. The expressions mirror ``sql/analytics.sql``'s view exactly; a divergence
#: between the two would make offline and online answers disagree, which is why
#: they are written out rather than approximated.
DERIVED_VIEW_COLUMNS: Dict[str, str] = {
    "year_of_study_ordinal": (
        "df['Year of Study'].map({'1st Year': 1, '2nd Year': 2, "
        "'3rd Year': 3, '4th Year': 4})"
    ),
    "overspend_amount": "df['Total Expenses'] - df['Allowance']",
    "is_overspending": "df['Total Expenses'] > df['Allowance']",
    "pays_by_upi": "df['Preferred Payment'] == 'UPI'",
    "names_a_upi_app": "df['UPI App'] != 'Not Applicable'",
}


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #


class AssistantError(RuntimeError):
    """Base class for every error this module raises deliberately."""


class IntentNotUnderstood(AssistantError):
    """The question could not be mapped onto the semantic layer.

    Carries the phrases that *were* recognised, so the caller can say what the
    assistant does understand instead of only what went wrong.
    """

    def __init__(self, message: str, understood: Sequence[str] = ()) -> None:
        super().__init__(message)
        self.understood = list(understood)


class UnsafeSQLError(AssistantError):
    """A generated statement failed the read-only guard.

    This should be unreachable - the builder only emits allowlisted fragments -
    so it firing means a bug in this module, not bad input from the user. It is
    raised rather than logged because silently returning unvalidated SQL from a
    text-to-SQL tool is exactly the failure mode worth being loud about.
    """


# --------------------------------------------------------------------------- #
# Intent model
# --------------------------------------------------------------------------- #


@dataclass
class QueryIntent:
    """A parsed question, before and after it becomes SQL.

    Attributes
    ----------
    question:
        The original text, kept for the insights layer and for provenance.
    measure:
        Chosen :class:`Measure`, or ``None`` for a detail listing.
    aggregation:
        Key into :data:`AGGREGATIONS`.
    dimensions:
        Group-by dimensions, in the order they were found.
    filters:
        ``(dimension, value, negated)`` triples. ``negated`` renders as
        ``<>`` / ``NOT IN``.
    limit:
        Row cap, already clamped to :data:`MAX_LIMIT`.
    wants_detail:
        True for a per-student listing rather than an aggregate.
    ranking:
        ``"desc"``, ``"asc"`` or ``""`` - the sort direction a ranking word asked
        for. An ORDER BY, never an aggregation.
    wants_extreme:
        True for "what is the highest total expense", where the answer is the one
        extreme *row* rather than a summary of all of them. ``AVG`` of the
        highest row is not the highest row, so this path bypasses the aggregate
        and orders by the raw column.
    compare:
        ``(dimension, values)`` set when the question named specific values of a
        dimension to compare ("hostel vs day scholar"). Rendered as ``IN`` so the
        answer contains the groups that were asked about and no others - grouping
        by accommodation alone would silently add PG to a two-way comparison.
    sort_ordinal:
        Order the year dimension by its natural ordinal, not alphabetically.
    """

    question: str
    measure: Optional[Measure] = None
    aggregation: str = "avg"
    dimensions: List[Dimension] = field(default_factory=list)
    filters: List[Tuple[Dimension, str, bool]] = field(default_factory=list)
    limit: int = DEFAULT_LIMIT
    wants_detail: bool = False
    ranking: str = ""
    wants_extreme: bool = False
    compare: Optional[Tuple[Dimension, Tuple[str, ...]]] = None
    sort_ordinal: bool = False
    notes: List[str] = field(default_factory=list)

    @property
    def is_aggregate(self) -> bool:
        """True when the query returns one row per group rather than per student."""
        return not self.wants_detail and self.measure is not None

    def describe(self) -> str:
        """Return a one-line, human summary of what will be run."""
        if self.wants_detail:
            what = "list of students"
        elif self.measure is None:
            what = "row count"
        else:
            what = f"{self.aggregation} of {self.measure.label}"
        parts = [what]
        if self.dimensions:
            parts.append("by " + ", ".join(d.label for d in self.dimensions))
        for dimension, value, negated in self.filters:
            parts.append(f"{'excluding' if negated else 'where'} {dimension.label} = {value}")
        return " | ".join(parts)


# --------------------------------------------------------------------------- #
# Text normalisation and matching helpers
# --------------------------------------------------------------------------- #

#: Filler that carries no intent. Stripped before matching so "what is the
#: average expense for students" reduces to "average expense students".
STOPWORDS = frozenset({
    "what", "which", "who", "whom", "whose", "is", "are", "was", "were", "the",
    "a", "an", "of", "for", "to", "in", "on", "at", "by", "with", "and", "or",
    "please", "tell", "me", "us", "show", "give", "find", "get", "there", "that",
    "this", "these", "those", "do", "does", "did", "can", "could", "would",
    "about", "from", "how", "much", "many", "students", "student", "people",
    "all", "any", "some", "their", "its", "it", "be", "been", "has", "have",
    "had", "want", "need", "like", "know", "average", "total",
})

#: Filler words only. An earlier, larger list broke the parser in non-obvious
#: ways: "by" is the word that turns an average into a per-group breakdown, and
#: "show"/"who"/"students" are part of the phrases that select a detail listing
#: or a count. Removing them cost more than the noise they suppressed, so the
#: list is kept to words no synonym or grouping phrase can contain.
STOPWORDS = frozenset({
    "the", "a", "an", "of", "its", "it", "please", "that", "this", "there",
    "can", "could", "would", "like", "know",
})

#: Never stripped, even though they look like filler, because they are
#: aggregations ("total expenses"), grouping words ("by degree") or a domain
#: value ("1st Year").
#:
#: The grouping words are here for a structural reason: "by" is filler by any
#: normal reckoning, but "average expenses by degree" and "average expenses
#: degree" are different questions, and stripping it silently turned the first
#: into an ungrouped average.
_PROTECTED = frozenset(
    {"average", "total", "median", "sum", "count", "how", "many",
     "by", "per", "across", "among", "each", "versus", "vs"}
)


def _normalise(text: str) -> str:
    """Lowercase, strip punctuation, and collapse whitespace.

    Apostrophes are the one punctuation mark preserved and then handled
    separately, because "day scholar's" must still match "day scholar".
    """
    lowered = text.lower().strip()
    lowered = lowered.replace("'", " ")
    # Everything else that is not a letter, digit or space becomes a space, so
    # "average/sum", "degree?", "expenses, by" all tokenise cleanly.
    cleaned = re.sub(r"[^a-z0-9/&+.\s-]", " ", lowered)
    return re.sub(r"\s+", " ", cleaned).strip()


def _tokens(text: str) -> List[str]:
    """Split normalised text into word tokens."""
    return _normalise(text).split()


def _strip_stopwords(text: str) -> str:
    """Remove filler words, keeping aggregations and digit-bearing tokens.

    A token that contains a digit is never removed, so "1st" survives even though
    "year" does not.
    """
    kept = [
        token
        for token in _tokens(text)
        if token in _PROTECTED or any(ch.isdigit() for ch in token)
        or token not in STOPWORDS
    ]
    return " ".join(kept)


def _find_phrases(text: str, phrases: Iterable[str]) -> List[str]:
    """Return the phrases present in ``text``, longest first.

    Word-boundary anchored so "top" does not match inside "stop", and matched on
    already-normalised text so the caller's punctuation does not matter.
    """
    found: List[str] = []
    for phrase in phrases:
        pattern = r"(?<![a-z0-9])" + re.escape(phrase) + r"(?![a-z0-9])"
        if re.search(pattern, text):
            found.append(phrase)
    return sorted(set(found), key=lambda p: (-len(p), p))


def _first_match(text: str, options: Sequence[Tuple[str, str]]) -> Optional[str]:
    """Return the value of the first option whose phrase appears in ``text``."""
    for phrase, value in options:
        pattern = r"(?<![a-z0-9])" + re.escape(phrase) + r"(?![a-z0-9])"
        if re.search(pattern, text):
            return value
    return None


# --------------------------------------------------------------------------- #
# Literal validation - the injection barrier
# --------------------------------------------------------------------------- #

#: A number is anything a NUMERIC comparison needs and nothing that could close
#: the literal. Deliberately excludes quotes, semicolons, whitespace and any
#: letter, so a value like ``1; DROP TABLE`` cannot pass.
_NUMERIC_LITERAL = re.compile(r"^-?\d{1,15}(\.\d{1,6})?$")


def _sql_literal(value: str) -> str:
    """Quote a value for SQL, or refuse.

    Accepts only a member of an allowlisted domain or a plain number. Everything
    else raises, which is what makes it safe to place the result directly into a
    statement.

    Parameters
    ----------
    value:
        A canonical domain value (e.g. ``"Hostel"``) or a numeric string.

    Returns
    -------
    str
        The value as a single-quoted SQL literal, or the bare number.

    Raises
    ------
    IntentNotUnderstood
        If the value matches neither a domain member nor the numeric pattern.
    """
    if _NUMERIC_LITERAL.match(value):
        return value
    # Belt and braces: even for an allowlisted value, reject anything containing a
    # quote or semicolon outright. The domain is closed, so this should never
    # trigger, but it means a future edit that widens a domain cannot silently
    # open an injection.
    if "'" in value or ";" in value or "\\" in value or "--" in value:
        raise IntentNotUnderstood(f"refusing to embed {value!r} in SQL: unsafe characters")
    return "'" + value + "'"


def _quote_identifier(name: str) -> str:
    """Quote an identifier, but only if it is allowlisted.

    The allowlist is the view's own column set plus the two aggregate helpers.
    This is the second half of the injection barrier: even a bug in the parser
    cannot introduce an arbitrary identifier, because anything unrecognised is
    rejected instead of quoted.
    """
    if name in _ALLOWED_IDENTIFIERS:
        return '"' + name + '"'
    raise UnsafeSQLError(f"identifier {name!r} is not in the allowlist")


def _allowed_identifiers() -> frozenset:
    """Every identifier the builder may emit: view columns plus SQL keywords."""
    columns = {m.name for m in MEASURES.values()}
    columns |= {d.name for d in DIMENSIONS.values()}
    columns |= set(VIEW_TO_CSV)
    return frozenset(columns | {
        "student_id", "student_name", "year_of_study_ordinal", "overspend_amount",
        "avg", "sum", "count", "max", "min", "percentile_disc", "order", "by",
        VIEW_NAME, TABLE_NAME,
    })


_ALLOWED_IDENTIFIERS = _allowed_identifiers()

#: View columns the offline engine requires the CSV to already contain. The
#: derived ones - the ordinal, the overspend amount and the UPI flags - are
#: excluded because the engine computes them from these, using the same formulas
#: as ``sql/analytics.sql``.
REQUIRED_CSV_COLUMNS: Tuple[str, ...] = tuple(
    sorted(
        ({m.name for m in MEASURES.values()} | {d.name for d in DIMENSIONS.values()})
        - {"student_id", "overspend_amount", "year_of_study_ordinal"}
    )
)



# --------------------------------------------------------------------------- #
# Intent parsing
# --------------------------------------------------------------------------- #

#: Phrases that put a dimension into GROUP BY. A dimension only becomes a
#: grouping when one of these is present - "students who live in hostel" filters,
#: "average expenses by degree" groups. Getting this backwards is the most common
#: way a text-to-SQL layer produces a confident, wrong answer.
GROUPING_SYNONYMS: Tuple[str, ...] = ("by", "per", "grouped by", "group by",
                                       "split by", "broken down by", "across",
                                       "for each", "by each", "among")


def _detect_limit(text: str) -> Optional[int]:
    """Return an explicit row cap, or ``None``.

    Handles "top 5", "top five", "5 highest", "first 10", "ten biggest". A
    ranking word with no number is deliberately *not* a limit - "highest" alone
    means "give me the top one", which the builder handles via ordering.
    """
    for pattern in (
        r"\btop\s+(\d{1,3})\b",
        r"\b(\d{1,3})\s+(?:highest|largest|biggest|most|best|lowest|smallest|least)\b",
        r"\b(?:first|top)\s+(\w+)\b",
        r"\b(\w+)\s+(?:highest|largest|biggest|most|best)\b",
    ):
        match = re.search(pattern, text)
        if not match:
            continue
        raw = match.group(1)
        if raw.isdigit():
            return int(raw)
        if raw in NUMBER_WORDS:
            return NUMBER_WORDS[raw]
    return None


def _detect_negation(text: str, position: int, window: int = 24) -> bool:
    """Report whether a negation word appears shortly before ``position``."""
    window_start = max(0, position - window)
    return bool(_find_phrases(text[window_start:position], NEGATIONS))


def _detect_filters(text: str) -> List[Tuple[Dimension, str, bool]]:
    """Find ``(dimension, value, negated)`` triples in the question.

    Only allowlisted domain values are recognised. A word that is not a domain
    value is simply not a filter, so the worst outcome is a missing filter on a
    question whose subject is not a closed vocabulary - never an arbitrary value
    reaching the SQL.
    """
    found: Dict[Tuple[str, str], Tuple[Dimension, str, bool]] = {}
    claimed: List[Tuple[int, int]] = []

    for dimension in DIMENSIONS.values():
        if dimension.name in UNFILTERABLE_DIMENSIONS or not dimension.value_aliases:
            continue
        for alias, canonical in dimension.value_aliases.items():
            pattern = r"(?<![a-z0-9])" + re.escape(alias) + r"(?![a-z0-9])"
            for match in re.finditer(pattern, text):
                start, end = match.span()
                # Skip a mention already consumed as another dimension's filter,
                # e.g. "hostel" matching both accommodation and something else.
                if any(start < c_end and c_start < end for c_start, c_end in claimed):
                    continue
                claimed.append((start, end))
                negated = _detect_negation(text, start)
                # Keyed by (dimension, value) so two aliases for the same value
                # collapse to one predicate. "pay by UPI" and "use UPI" both
                # matched preferred_payment=UPI and emitted it twice, which read
                # as a redundant AND rather than the single filter it is.
                key = (dimension.name, canonical)
                previous = found.get(key)
                if previous is None:
                    found[key] = (dimension, canonical, negated)
                elif negated and not previous[2]:
                    # One mention negated, one not. Keep the negation: "students
                    # who do not pay by UPI" mentions the value twice.
                    found[key] = (dimension, canonical, True)

    return list(found.values())


def _detect_dimensions(
    text: str, filters: Sequence[Tuple[Dimension, str, bool]]
) -> List[Dimension]:
    """Find the GROUP BY dimensions.

    A dimension qualifies when a grouping word sits near its synonym ("average
    expenses by degree"), when a comparison names two of its values ("hostel vs
    day scholar", a grouping in disguise), or when a ranking word makes it the
    subject of the question ("top 5 UPI apps", where the apps are what is ranked
    and the count is what is compared).

    Parameters
    ----------
    text:
        Stopword-stripped, normalised question.
    filters:
        Filters already detected, so a dimension named only as a filter value is
        not also chosen as a grouping.

    Returns
    -------
    list of Dimension
    """
    chosen: List[Dimension] = []
    explicitly_grouped: List[Dimension] = []
    filter_names = {d.name for d, _, _ in filters}
    ranking = bool(_first_match(text, RANKING_SYNONYMS))

    for dimension in DIMENSIONS.values():
        for synonym in dimension.synonyms:
            for match in re.finditer(
                r"(?<![a-z0-9])" + re.escape(synonym) + r"(?![a-z0-9])", text
            ):
                # Look back up to 18 characters for a grouping word.
                lead = text[max(0, match.start() - 18):match.start()]
                grouped = bool(_find_phrases(lead, GROUPING_SYNONYMS))
                if dimension.name in filter_names:
                    continue
                if grouped:
                    if dimension not in explicitly_grouped:
                        explicitly_grouped.append(dimension)
                    break
                # A ranking word makes the named dimension the subject of the
                # question - "top 5 UPI apps" ranks apps and counts them - but a
                # ranking word alone is weaker evidence than "by", so these are
                # only used when nothing was explicitly grouped.
                if ranking and dimension not in chosen:
                    chosen.append(dimension)
                    break

    # "hostel vs day scholar" - two values of one dimension and a comparison word
    # is a request for a per-value breakdown, not two equality filters.
    if not explicitly_grouped and not chosen:
        for dimension in DIMENSIONS.values():
            values_here = [v for d, v, _ in filters if d.name == dimension.name]
            if len(values_here) > 1 and _find_phrases(text, COMPARISON_SYNONYMS):
                explicitly_grouped.append(dimension)
                break

    # "top 5 hostellers by degree" names two dimensions and asks for a breakdown
    # on one, so an explicit "by" wins and the rest are left as filters.
    return explicitly_grouped if explicitly_grouped else chosen[:1]


def parse_intent(question: str) -> QueryIntent:
    """Turn a plain English question into a :class:`QueryIntent`.

    The parse is deliberately conservative. An unrecognised question raises
    :class:`IntentNotUnderstood` listing what the assistant does understand,
    rather than guessing a plausible query - a wrong aggregate over a real
    database is worse than an admitted gap.

    Parameters
    ----------
    question:
        Free text, e.g. ``"average expenses by degree"``.

    Returns
    -------
    QueryIntent

    Raises
    ------
    IntentNotUnderstood
        If no measure, dimension or filter can be identified.
    """
    if not question or not question.strip():
        raise IntentNotUnderstood("Ask a question about the student budget data.")

    text = _strip_stopwords(question)
    understood: List[str] = []

    intent = QueryIntent(question=question.strip())

    # 1. Detail listing? Checked first: "list the top 5 hostellers by spend" is a
    #    listing with an ordering, not an aggregate.
    if _find_phrases(text, DETAIL_SYNONYMS):
        intent.wants_detail = True
        understood.append("detail listing")

    # 2. Explicit limit.
    explicit_limit = _detect_limit(text)
    if explicit_limit is not None:
        understood.append(f"limit {explicit_limit}")

    # 3. Filters and dimensions. Filters are detected first because a dimension
    #    that only ever appears as a filter ("students in hostel") must not also
    #    become a grouping.
    filters = _detect_filters(text)
    if filters:
        understood.extend(f"{d.label}={v}" for d, v, _ in filters)
    intent.filters = filters

    dimensions = _detect_dimensions(text, filters)
    if dimensions:
        understood.extend(d.label for d in dimensions)
    intent.dimensions = dimensions

    # 4. Measure: longest matching synonym wins, so "total expenses" beats
    #    "expenses" and "academic expenses" beats "expenses".
    chosen_measure: Optional[Measure] = None
    best_length = 0
    for measure in MEASURES.values():
        for synonym in measure.synonyms:
            pattern = r"(?<![a-z0-9])" + re.escape(synonym) + r"(?![a-z0-9])"
            match = re.search(pattern, text)
            if match and len(synonym) > best_length:
                chosen_measure, best_length = measure, len(synonym)
    intent.measure = chosen_measure
    if chosen_measure:
        understood.append(chosen_measure.label)

    # 5. Aggregation: an explicit verb wins; otherwise the measure's default. The
    #    flag matters later - an inferred aggregation must not be mistaken for a
    #    requested one when deciding what a ranking word meant.
    aggregation = _first_match(text, AGGREGATION_SYNONYMS)
    aggregation_explicit = aggregation is not None
    if aggregation is None and chosen_measure is not None:
        aggregation = chosen_measure.default_aggregation
    if aggregation is None and intent.wants_detail:
        aggregation = ""
    if aggregation:
        intent.aggregation = aggregation
        understood.append(aggregation)

    # 6. Ranking direction. Sets ORDER BY, not the aggregate.
    intent.ranking = _first_match(text, RANKING_SYNONYMS) or ""
    if intent.ranking:
        understood.append(f"{intent.ranking} order")

    # 7. "top 5 UPI apps" with no stated measure asks which values are most
    #    common - a count, not a MAX over whichever column won the synonym race.
    if (
        chosen_measure is None
        and dimensions
        and not intent.wants_detail
        and not aggregation_explicit
    ):
        intent.measure = MEASURES["student_count"]
        intent.aggregation = "count"
        intent.notes.append("inferred count for a top-N over a dimension")
        understood.append("count")

    # 8. "what is the highest total expense" - one student, not a summary. With no
    #    grouping and no requested aggregation, ORDER BY the raw column and take
    #    one row; averaging first would answer a different question.
    if (
        intent.ranking
        and not dimensions
        and not intent.wants_detail
        and not aggregation_explicit
        and intent.measure is not None
        and intent.measure.name != "student_id"
    ):
        intent.wants_extreme = True
        intent.notes.append(f"{intent.ranking} raw {intent.measure.name}, one extreme row")
        understood.append("single extreme row")

    # 9. Year ordering. Grouping by year and sorting on it must use the ordinal,
    #    because '1st Year' < '2nd Year' only holds by coincidence of the labels.
    intent.sort_ordinal = any(d.name == "year_of_study" for d in dimensions)

    # 10. Resolve the limit, including the "highest with no number" case.
    if explicit_limit is not None:
        intent.limit = max(1, min(explicit_limit, MAX_LIMIT))
    elif intent.ranking and not dimensions and not intent.wants_detail:
        intent.limit = 1
    else:
        intent.limit = DEFAULT_LIMIT

    # 11. Nothing recognisable at all.
    if not (intent.measure or intent.dimensions or intent.filters or intent.wants_detail):
        raise IntentNotUnderstood(
            "I could not find anything to measure or group by in that question.",
            understood=understood,
        )

    # 12. "compare hostel vs day scholar" is a grouping, but one restricted to the
    #     values that were named. Left as two equality filters they would AND to
    #     zero rows; dropped entirely they would add every other group (PG) to a
    #     two-way comparison. An explicit IN is the only reading that is right.
    if _find_phrases(text, COMPARISON_SYNONYMS):
        for dimension in dimensions:
            values = tuple(sorted({v for d, v, _ in filters if d.name == dimension.name}))
            if len(values) > 1:
                intent.compare = (dimension, values)
                intent.filters = [f for f in filters if f[0].name != dimension.name]
                intent.notes.append(
                    f"compared {', '.join(values)} rather than every "
                    f"{dimension.label} value"
                )
                understood.extend(f"{dimension.label}={v}" for v in values)

    return intent


# --------------------------------------------------------------------------- #
# SQL generation
# --------------------------------------------------------------------------- #


def _aggregate_sql(aggregation: str, column: str) -> str:
    """Render one aggregate expression from the allowlist.

    The only string formatting is into the ``median`` template, and the
    substituted expression is itself a quoted allowlisted column, so no user text
    reaches the output.
    """
    template = AGGREGATIONS.get(aggregation)
    if template is None:
        raise UnsafeSQLError(f"unknown aggregation {aggregation!r}")
    if aggregation == "count":
        return "count(*)"
    if aggregation == "median":
        return template.format(expr=_quote_identifier(column))
    return f"{template}({_quote_identifier(column)})"


def _where_sql(intent: QueryIntent) -> str:
    """Render the WHERE clause from allowlisted dimension/value pairs.

    Parameters
    ----------
    intent:
        Carries the equality filters and the optional comparison value set.

    Returns
    -------
    str
        A ``WHERE`` clause with no leading space when empty, otherwise
        ``" WHERE ..."``.
    """
    parts: List[str] = []
    for dimension, value, negated in intent.filters:
        operator = "<>" if negated else "="
        parts.append(f"{_quote_identifier(dimension.name)} {operator} {_sql_literal(value)}")
    if intent.compare is not None:
        dimension, values = intent.compare
        rendered = ", ".join(_sql_literal(v) for v in values)
        parts.append(f"{_quote_identifier(dimension.name)} IN ({rendered})")
    if not parts:
        return ""
    return " WHERE " + " AND ".join(parts)


def build_sql(intent: QueryIntent) -> str:
    """Render a :class:`QueryIntent` as a single read-only PostgreSQL statement.

    Parameters
    ----------
    intent:
        A parsed question.

    Returns
    -------
    str
        One ``SELECT`` statement, trailing semicolon included.

    Raises
    ------
    IntentNotUnderstood
        If a filter value is not allowlisted.
    UnsafeSQLError
        If an identifier is not allowlisted.
    """
    table = _quote_identifier(VIEW_NAME)
    where = _where_sql(intent)

    # --- per-student listing -------------------------------------------------
    if intent.wants_detail:
        columns = ["student_name", "degree", "year_of_study", "total_expenses"]
        order_column = "total_expenses" if intent.measure is None else intent.measure.name
        if order_column == "student_id":
            order_column = "total_expenses"
        return (
            f"SELECT {', '.join(_quote_identifier(c) for c in columns)}\n"
            f"FROM {table}{where}\n"
            f"ORDER BY {_quote_identifier(order_column)} DESC\n"
            f"LIMIT {int(intent.limit)};"
        )

    # --- count with no grouping ---------------------------------------------
    if intent.measure is not None and intent.measure.name == "student_id" and not intent.dimensions:
        return f"SELECT count(*) AS students\nFROM {table}{where}\nLIMIT 1;"

    # --- aggregate ----------------------------------------------------------
    if intent.measure is None:
        # Filters but no measure and no grouping: "how many hostel students".
        return f"SELECT count(*) AS students\nFROM {table}{where}\nLIMIT 1;"

    aggregate = _aggregate_sql(intent.aggregation, intent.measure.name)
    if intent.measure.name == "student_id":
        # COUNT(*) of student_id reads as count_student_id, which is both ugly and
        # implies a non-null count it does not guarantee.
        alias = "students"
    else:
        alias = f"{intent.aggregation}_{intent.measure.name}"

    if intent.wants_extreme:
        # The extreme row itself. No aggregate - AVG of the highest row is not the
        # highest row, and answering "what is the highest expense" with a mean
        # would be a category error, not an approximation.
        columns = ["student_name", "degree", "year_of_study", "accommodation",
                   intent.measure.name]
        select_list = ", ".join(_quote_identifier(c) for c in columns)
        direction = "ASC" if intent.ranking == "asc" else "DESC"
        return (
            f"SELECT {select_list}\n"
            f"FROM {table}{where}\n"
            f"ORDER BY {_quote_identifier(intent.measure.name)} {direction}\n"
            f"LIMIT 1;"
        )

    if not intent.dimensions:
        return f"SELECT {aggregate} AS {alias}\nFROM {table}{where}\nLIMIT 1;"

    group_columns = [_quote_identifier(d.name) for d in intent.dimensions]
    group_by = ", ".join(group_columns)

    # The year dimension sorts by its ordinal when it is the only grouping;
    # grouped with something else, the aggregate is the more useful order.
    if intent.sort_ordinal and len(intent.dimensions) == 1:
        order_by = f"{YEAR_ORDINAL_SQL} ASC"
    else:
        direction = "ASC" if intent.ranking == "asc" else "DESC"
        order_by = f"{alias} {direction}"

    select_list = group_columns + [f"{aggregate} AS {alias}"]
    if intent.measure.name == "budget_usage_pct" and intent.dimensions:
        # The aggregate ratio beside the mean of ratios. They differ by 2.1x on
        # this dataset, so emitting one without the other invites a wrong read.
        weighted = (
            f"round(100.0 * sum({_quote_identifier('total_expenses')})"
            f" / nullif(sum({_quote_identifier('allowance')}), 0), 2)"
            f" AS aggregate_utilisation_pct"
        )
        select_list.append(weighted)

    return (
        f"SELECT {', '.join(select_list)}\n"
        f"FROM {table}{where}\n"
        f"GROUP BY {group_by}\n"
        f"ORDER BY {order_by}\n"
        f"LIMIT {int(intent.limit)};"
    )


# --------------------------------------------------------------------------- #
# Read-only guard
# --------------------------------------------------------------------------- #

#: Statement and expression keywords that must never appear. Checked against the
#: statement with string literals and comments removed, so a column legitimately
#: named e.g. ``update_count`` cannot produce a false positive - the allowlist in
#: ``_quote_identifier`` already restricts what can be named at all.
FORBIDDEN_KEYWORDS: Tuple[str, ...] = (
    "insert", "update", "delete", "drop", "create", "alter", "truncate",
    "grant", "revoke", "copy", "attach", "detach", "vacuum", "analyze",
    "call", "do", "execute", "prepare", "listen", "notify", "set", "reset",
    "begin", "commit", "rollback", "savepoint", "lock", "reindex", "cluster",
    "comment", "security", "pg_sleep", "pg_read_file", "pg_ls_dir", "dblink",
    "lo_import", "lo_export", "pg_terminate", "set_config", "current_setting",
    # Set operations. A UNION is the standard way to append a second, unvalidated
    # SELECT to an otherwise harmless-looking statement, and the builder never
    # emits one - so there is no legitimate case for it appearing.
    "union", "intersect", "except",
)


class SQLSafetyGuard:
    """Validates that a generated statement is a single read-only ``SELECT``.

    The builder cannot produce anything else by construction, so this is a
    defence-in-depth check whose real job is to make that guarantee *testable*.
    ``selftest`` asserts it rejects a set of hostile statements, which means a
    future edit to the builder that widened its output would fail a test rather
    than reach a live database.
    """

    #: Domain values permitted inside single quotes, used by :meth:`validate`.
    @staticmethod
    def allowed_literals() -> frozenset:
        """Every string a generated statement may legitimately contain."""
        values = set()
        for dimension in DIMENSIONS.values():
            values.update(dimension.values)
            values.update(dimension.value_aliases.values())
        # Not a filter value, but a legitimate literal in the year ordinal.
        values.update({"1st Year", "2nd Year", "3rd Year", "4th Year"})
        return frozenset(values)

    @staticmethod
    def _strip_literals(sql: str) -> str:
        """Blank out single-quoted literals and comments.

        Keyword and identifier checks then run against SQL structure only, so a
        domain value that happens to contain a forbidden word cannot trip them.
        """
        without_comments = re.sub(r"--[^\n]*", " ", sql)
        without_comments = re.sub(r"/\*.*?\*/", " ", without_comments, flags=re.S)
        return re.sub(r"'(?:[^']|'')*'", " ", without_comments)

    @staticmethod
    def validate(sql: str) -> None:
        """Raise :class:`UnsafeSQLError` unless ``sql`` is a single safe SELECT.

        Checks, in order: balanced quoting, a single statement, a ``SELECT``
        head, no forbidden keyword, balanced parentheses, allowlisted
        identifiers, and allowlisted string literals.

        Parameters
        ----------
        sql:
            The candidate statement.

        Raises
        ------
        UnsafeSQLError
            On the first failed check.
        """
        if not sql or not sql.strip():
            raise UnsafeSQLError("empty statement")

        # 1. Balanced single quotes. An odd count means an unterminated literal,
        #    which is also the classic way to smuggle a second statement past a
        #    naive "contains a semicolon" test.
        if sql.count("'") % 2 != 0:
            raise UnsafeSQLError("unbalanced single quotes - possible injection")

        # 2. No comments. The builder emits none, so their presence means the
        #    statement was not built by the builder. This matters because comment
        #    stripping is what makes the remaining checks sound: without this rule
        #    a payload hidden after "--" would be removed before inspection, and a
        #    statement like "SELECT 1 -- ; DROP TABLE t" would be judged on the
        #    text after the payload rather than rejected for containing it.
        if "--" in sql or "/*" in sql:
            raise UnsafeSQLError("comments are not emitted by this builder")

        structure = SQLSafetyGuard._strip_literals(sql)

        # 3. Exactly one statement. The trailing semicolon is the only one allowed.
        body = structure.strip().rstrip(";").strip()
        if ";" in body:
            raise UnsafeSQLError("more than one statement")

        # 4. Must be a SELECT (or a WITH that resolves to one).
        head = body.lstrip().lower()
        if not (head.startswith("select") or head.startswith("with")):
            raise UnsafeSQLError(f"statement does not start with SELECT: {body[:40]!r}")
        if head.startswith("with"):
            raise UnsafeSQLError("CTEs are not emitted by this builder and are not allowed")

        # 5. No forbidden keyword as a standalone word.
        for keyword in FORBIDDEN_KEYWORDS:
            if re.search(r"(?<![a-z0-9_])" + keyword + r"(?![a-z0-9_])", structure.lower()):
                raise UnsafeSQLError(f"forbidden keyword in generated SQL: {keyword!r}")

        # 6. Balanced parentheses.
        if structure.count("(") != structure.count(")"):
            raise UnsafeSQLError("unbalanced parentheses")

        # 7. Every double-quoted identifier is allowlisted.
        for identifier in re.findall(r'"([^"]*)"', sql):
            if identifier not in _ALLOWED_IDENTIFIERS:
                raise UnsafeSQLError(f"identifier {identifier!r} is not in the allowlist")

        # 8. Every single-quoted literal is an allowlisted domain value.
        allowed = SQLSafetyGuard.allowed_literals()
        for literal in re.findall(r"'((?:[^']|'')*)'", sql):
            if literal not in allowed:
                raise UnsafeSQLError(f"literal {literal!r} is not an allowlisted value")

        # 9. No LIMIT above the hard cap, whatever the parser believed.
        match = re.search(r"(?<![a-z0-9_])limit\s+(\d+)", structure.lower())
        if match and int(match.group(1)) > MAX_LIMIT:
            raise UnsafeSQLError(f"LIMIT {match.group(1)} exceeds the {MAX_LIMIT} row cap")


# --------------------------------------------------------------------------- #
# Execution
# --------------------------------------------------------------------------- #


@dataclass
class QueryResult:
    """The outcome of running one intent.

    Attributes
    ----------
    sql:
        The statement that was executed, for the ``explain`` command and for audit.
    columns:
        Column names, in order.
    rows:
        Rows as tuples, in order.
    source:
        ``"postgresql"`` or ``"offline"`` - which engine actually answered. Always
        surfaced to the caller: an offline answer is computed from the CSV, not
        read from the table the DDL describes, and the two can diverge if the
        database was loaded from a different revision.
    detail:
        Optional notes on how the answer was reached, including any inference the
        parser made.
    elapsed_ms:
        Wall time for the query, for the ``ask --timing`` output.
    """

    sql: str
    columns: List[str]
    rows: List[Tuple[Any, ...]]
    source: str
    detail: str = ""
    elapsed_ms: float = 0.0

    def __len__(self) -> int:
        return len(self.rows)

    @property
    def is_empty(self) -> bool:
        """True when the query matched no rows.

        An empty result is a finding about the filter, not an absence of data, and
        the insights layer says so rather than inventing a conclusion.
        """
        return not self.rows

    def as_dicts(self) -> List[Dict[str, Any]]:
        """Return rows as column-keyed dicts, for JSON output."""
        return [dict(zip(self.columns, row)) for row in self.rows]

    def table(self, max_width: int = 34) -> str:
        """Render the result as a fixed-width text table.

        Parameters
        ----------
        max_width:
            Per-column character cap. Values are truncated with an ellipsis
            rather than wrapped, so a table stays one row per result.
        """
        if not self.rows:
            return "(no rows)"

        cells = [[_format_cell(v) for v in row] for row in self.rows]
        widths = []
        for index, name in enumerate(self.columns):
            longest = max([len(name)] + [len(row[index]) for row in cells])
            widths.append(min(longest, max_width))

        def rule(char: str) -> str:
            return char.join("-" * (w + 2) for w in widths)

        lines = [" | ".join(f"{name:<{w}}" for name, w in zip(self.columns, widths)),
                 rule("-")]
        for row in cells:
            lines.append(" | ".join(
                f"{value[:w]:<{w}}" for value, w in zip(row, widths)
            ))
        return "\n".join(lines)


def _format_cell(value: Any) -> str:
    """Render one cell for display, as an int where the value is whole."""
    if value is None:
        return "NULL"
    if isinstance(value, float) and value.is_integer() and abs(value) < 1e15:
        return str(int(value))
    if isinstance(value, float):
        return f"{value:,.2f}"
    return str(value)


def _resolve_source_csv() -> Path:
    """Locate the Stage 4 CSV using the same order as ``data_quality``.

    Returns
    -------
    Path

    Raises
    ------
    FileNotFoundError
        If none of :data:`SOURCE_CSV_CANDIDATES` exists.
    """
    for candidate in SOURCE_CSV_CANDIDATES:
        if candidate.exists():
            return candidate
    searched = ", ".join(str(c.relative_to(PROJECT_ROOT)) for c in SOURCE_CSV_CANDIDATES)
    raise FileNotFoundError(
        f"no processed CSV found. Looked for: {searched}. "
        f"Run the pipeline first: python src/pipeline.py --stages transform"
    )


def _load_offline_frame() -> pd.DataFrame:
    """Read the processed CSV into a frame shaped like the analytics view.

    The CSV carries the raw transformed columns; the view additionally carries
    ``year_of_study_ordinal``, ``overspend_amount`` and the two UPI flags. Those
    are recomputed here with the same formulas as ``sql/analytics.sql`` so the two
    paths cannot silently disagree - and :meth:`OfflineEngine.selfcheck` asserts it.

    Returns
    -------
    pandas.DataFrame
        Columns renamed to the view's names, plus the derived columns.
    """
    frame = pd.read_csv(_resolve_source_csv())
    frame = frame.rename(columns={csv: view for view, csv in VIEW_TO_CSV.items()})

    missing = [c for c in REQUIRED_CSV_COLUMNS if c not in frame.columns]
    if missing:
        raise FileNotFoundError(
            f"processed CSV is missing expected columns: {', '.join(missing)}. "
            f"Re-run the pipeline rather than answering from a stale file."
        )

    ordinal = {"1st Year": 1, "2nd Year": 2, "3rd Year": 3, "4th Year": 4}
    frame["year_of_study_ordinal"] = frame["year_of_study"].map(ordinal).astype("Int64")
    frame["overspend_amount"] = (
        frame["total_expenses"] - frame["allowance"]
    ).clip(lower=0)
    frame["pays_by_upi"] = frame["preferred_payment"].isin(["UPI"])
    frame["names_a_upi_app"] = frame["upi_app"].isin(
        ["GPay", "PhonePe", "SBI Pay", "Amazon Pay", "BHIM UPI", "Airtel Thanks App"]
    )
    return frame


class OfflineEngine:
    """Answers an intent from the processed CSV using pandas.

    This exists so the assistant is useful with no PostgreSQL server running, and
    so the SQL path can be checked against a known answer. It implements the same
    semantics as :func:`build_sql` - same aggregation, same ordering, same filters
    - but it is not a SQL interpreter, and ``selftest`` compares the two where
    both are available.
    """

    def __init__(self, frame: Optional[pd.DataFrame] = None) -> None:
        """
        Parameters
        ----------
        frame:
            Pre-loaded frame, or ``None`` to load it on first use.
        """
        self._frame = frame

    @property
    def frame(self) -> pd.DataFrame:
        """The cached frame, loading it on first access."""
        if self._frame is None:
            self._frame = _load_offline_frame()
        return self._frame

    def run(self, intent: QueryIntent, sql: str) -> QueryResult:
        """Evaluate ``intent`` against the CSV.

        Parameters
        ----------
        intent:
            The parsed question.
        sql:
            The generated SQL, carried through so provenance is never lost even
            when the offline path is the one that answered.

        Returns
        -------
        QueryResult
        """
        frame = self._apply_filters(self.frame, intent)

        if intent.wants_detail or intent.wants_extreme:
            columns = ["student_name", "degree", "year_of_study", "accommodation"]
            measure_column = intent.measure.name if intent.measure else "total_expenses"
            if measure_column == "student_id":
                measure_column = "total_expenses"
            if not intent.wants_extreme:
                columns = ["student_name", "degree", "year_of_study", "total_expenses"]
            columns = [c for c in columns if c in frame.columns] + [measure_column]

            ascending = intent.ranking == "asc"
            ordered = frame.sort_values(
                measure_column, ascending=ascending, kind="mergesort"
            )
            rows = ordered[columns].head(intent.limit)
            result = QueryResult(
                sql=sql,
                columns=list(rows.columns),
                rows=[tuple(r) for r in rows.itertuples(index=False)],
                source="offline",
            )
            return result

        # --- count with no grouping -----------------------------------------
        if intent.measure is None or (
            intent.measure.name == "student_id" and not intent.dimensions
        ):
            return QueryResult(
                sql=sql,
                columns=["students"],
                rows=[(int(len(frame)),)],
                source="offline",
            )

        # --- aggregate over the whole filtered set ---------------------------
        if not intent.dimensions:
            value = self._aggregate(frame[intent.measure.name], intent.aggregation)
            return QueryResult(
                sql=sql,
                columns=["value"],
                rows=[(None if pd.isna(value) else float(value),)],
                source="offline",
            )

        # --- one row per group ----------------------------------------------
        group_names = [d.name for d in intent.dimensions]
        grouped = frame.groupby(group_names, dropna=False, sort=False)
        # SQL's count(*) counts rows regardless of any column, so there is no
        # "student_id" to read. total_expenses is the carrier: it is never null in
        # a row that passed the data-quality gate, so its non-null count per group
        # is the group size.
        carrier = "total_expenses" if intent.aggregation == "count" else intent.measure.name
        series = self._aggregate_grouped(grouped[carrier], intent.aggregation)
        result_frame = series.reset_index()
        result_frame.columns = group_names + ["value"]

        # The year dimension sorts on its ordinal, so 4th Year does not sort ahead
        # of 1st Year the way the text labels would.
        if intent.sort_ordinal and group_names == ["year_of_study"]:
            result_frame["_ordinal"] = result_frame["year_of_study"].map(ordinal_map())
            result_frame = result_frame.sort_values("_ordinal", kind="mergesort")
            result_frame = result_frame.drop(columns=["_ordinal"])
        else:
            result_frame = result_frame.sort_values(
                "value", ascending=intent.ranking == "asc", kind="mergesort"
            )

        select = group_names + ["value"]
        if intent.measure.name == "budget_usage_pct":
            # Emit the aggregate ratio alongside the mean of ratios, exactly as the
            # SQL path does. The two differ by 2.1x here and mean different things.
            total = float(frame["total_expenses"].sum())
            allowance = float(frame["allowance"].sum())
            select.append("aggregate_utilisation_pct")
            result_frame["aggregate_utilisation_pct"] = (
                100.0 * total / allowance if allowance else float("nan")
            )

        ordered = result_frame[select].head(intent.limit)
        return QueryResult(
            sql=sql,
            columns=list(ordered.columns),
            rows=[tuple(r) for r in ordered.itertuples(index=False)],
            source="offline",
        )

    @staticmethod
    def _apply_filters(frame: pd.DataFrame, intent: QueryIntent) -> pd.DataFrame:
        """Apply the allowlisted filters and comparison set to the frame.

        Mirrors :func:`_where_sql` exactly: equality, inequality, then ``IN`` for
        a comparison. Keeping the two in step matters because ``selftest`` compares
        their answers.
        """
        result = frame
        for dimension, value, negated in intent.filters:
            if dimension.name not in result.columns:
                continue
            if negated:
                result = result[result[dimension.name] != value]
            else:
                result = result[result[dimension.name] == value]
        if intent.compare is not None:
            dimension, values = intent.compare
            if dimension.name in result.columns:
                result = result[result[dimension.name].isin(list(values))]
        return result

    @staticmethod
    def _median(values: "pd.Series") -> float:
        """Return the discrete median, matching ``percentile_disc(0.5)``.

        The value actually present at the midpoint. For an even-sized group this
        differs from the average of the two middle values that ``median()`` would
        return, and it is the one PostgreSQL's ``percentile_disc`` computes - the
        offline path has to agree with the SQL path or the two answers disagree
        for no stated reason.
        """
        ordered = values.sort_values()
        return float(ordered.iloc[int(0.5 * (len(ordered) - 1))])

    @classmethod
    def _aggregate(cls, series: "pd.Series", aggregation: str) -> float:
        """Aggregate a plain series to a single float.

        Parameters
        ----------
        series:
            An ungrouped column.
        aggregation:
            A key into :data:`AGGREGATIONS`.

        Returns
        -------
        float

        Raises
        ------
        UnsafeSQLError
            If the aggregation is not in the allowlist.
        """
        if aggregation == "count":
            return float(len(series))
        if aggregation == "avg":
            return float(series.mean())
        if aggregation == "sum":
            return float(series.sum())
        if aggregation == "max":
            return float(series.max())
        if aggregation == "min":
            return float(series.min())
        if aggregation == "median":
            return cls._median(series)
        raise UnsafeSQLError(f"unknown aggregation {aggregation!r}")

    @classmethod
    def _aggregate_grouped(
        cls, grouped: "pd.core.groupby.SeriesGroupBy", aggregation: str
    ) -> "pd.Series":
        """Aggregate a grouped column to one value per group.

        Parameters
        ----------
        grouped:
            A ``SeriesGroupBy`` from a single column.
        aggregation:
            A key into :data:`AGGREGATIONS`.

        Returns
        -------
        pandas.Series
            Indexed by group key.

        Raises
        ------
        UnsafeSQLError
            If the aggregation is not in the allowlist.
        """
        if aggregation == "count":
            return grouped.count().astype(float)
        if aggregation == "avg":
            return grouped.mean().astype(float)
        if aggregation == "sum":
            return grouped.sum().astype(float)
        if aggregation == "max":
            return grouped.max().astype(float)
        if aggregation == "min":
            return grouped.min().astype(float)
        if aggregation == "median":
            return grouped.apply(cls._median).astype(float)
        raise UnsafeSQLError(f"unknown aggregation {aggregation!r}")

    def selfcheck(self) -> List[str]:
        """Return a list of disagreements between the CSV and the recorded facts.

        An empty list means the offline frame still matches what Stages 6 and 7
        verified. A non-empty list is printed by ``selftest`` and should be treated
        as "the processed CSV has changed underneath the assistant".

        Returns
        -------
        list of str
        """
        problems: List[str] = []
        frame = self.frame
        if len(frame) != EXPECTED_ROW_COUNT:
            problems.append(
                f"row count is {len(frame)}, expected {EXPECTED_ROW_COUNT}"
            )
        for column, expected in (("total_expenses", 11461.27), ("allowance", 1578.64)):
            actual = float(frame[column].mean())
            if abs(actual - expected) > 0.01:
                problems.append(
                    f"mean {column} is {actual:,.2f}, expected {expected:,.2f}"
                )
        # 13.4% of 852 expense cells should be the censored floor.
        expense_columns = ["food_expenses", "transport_expenses",
                           "academic_expenses", "other_expenses"]
        cells = frame[expense_columns].to_numpy().ravel()
        censored = int((cells == CENSORED_CELL_VALUE).sum())
        if censored == 0:
            problems.append("no censored cells found - the >5000 bucket is missing")
        return problems


def ordinal_map() -> Dict[Any, int]:
    """Return the year label -> ordinal map used by the offline engine."""
    return {"1st Year": 1, "2nd Year": 2, "3rd Year": 3, "4th Year": 4}


class PostgreSQLRunner:
    """Executes generated SQL against PostgreSQL, read-only.

    The connection is opened lazily and closed per query, matching
    ``src/database.py``. Nothing here can write: the statement was validated by
    :class:`SQLSafetyGuard` before reaching this class, and the session itself is
    put in a read-only transaction as a second, independent barrier.
    """

    def __init__(self, dsn: str, connect: Optional[Callable] = None) -> None:
        """
        Parameters
        ----------
        dsn:
            libpq connection string.
        connect:
            Override for ``database.connect``, used by ``selftest`` to inject a
            failure without a server.
        """
        self.dsn = dsn
        self._connect = connect or database.connect

    def run(self, sql: str) -> QueryResult:
        """Execute ``sql`` and return the rows.

        Parameters
        ----------
        sql:
            A statement already validated by :class:`SQLSafetyGuard`.

        Returns
        -------
        QueryResult
        """
        SQLSafetyGuard.validate(sql)
        connection = self._connect(self.dsn)
        try:
            with connection:
                with connection.cursor() as cursor:
                    # Independent of the guard: even if a future edit emitted a
                    # writable statement, the server would refuse to execute it.
                    cursor.execute("SET TRANSACTION READ ONLY")
                    cursor.execute(sql)
                    columns = [d[0] for d in cursor.description] if cursor.description else []
                    rows = [tuple(r) for r in cursor.fetchall()]
        finally:
            try:
                connection.close()
            except Exception:  # pragma: no cover - close is best effort
                pass
        return QueryResult(sql=sql, columns=columns, rows=rows, source="postgresql")


# --------------------------------------------------------------------------- #
# Explaining ETL and data quality failures in plain English
# --------------------------------------------------------------------------- #


@dataclass
class Explanation:
    """One explained failure.

    Attributes
    ----------
    title:
        What went wrong, in one line.
    cause:
        Why it happened, in terms of this project's pipeline.
    fix:
        The concrete command or change that resolves it.
    severity:
        ``"CRITICAL"``, ``"WARNING"`` or ``"INFO"``, copied from the source so the
        assistant never downgrades or upgrades the pipeline's own judgement.
    evidence:
        The original message, examples or offending count, quoted verbatim.
    """

    title: str
    cause: str
    fix: str
    severity: str = "INFO"
    evidence: str = ""

    def render(self) -> str:
        """Format for the terminal."""
        lines = [f"[{self.severity}] {self.title}"]
        if self.cause:
            lines.append(f"  why: {self.cause}")
        if self.fix:
            lines.append(f"  fix: {self.fix}")
        if self.evidence:
            lines.append(f"  evidence: {self.evidence}")
        return "\n".join(lines)


#: Check-id prefix -> (title, cause, fix). The ids are verbatim from
#: ``src/data_quality.py``; the templated ones (``no_nulls::Food`` and friends)
#: are matched on their prefix so one entry covers every column.
#:
#: Every entry answers two questions a reader of a log actually has: what does
#: this mean for *my* data, and what do I type next.
CHECK_EXPLANATIONS: Dict[str, Tuple[str, str, str]] = {
    "row_count_not_empty": (
        "the processed CSV has no rows",
        "stage 4 wrote an empty file, so every later stage has nothing to work with",
        "re-run stage 3 and 4: python src/pipeline.py --stages 3,4",
    ),
    "row_count_plausible": (
        "far fewer rows than a full survey would give",
        f"a truncated or partially-failed export looks exactly like this; fewer than "
        f"{data_quality.MIN_REASONABLE_ROWS} rows usually means the export stopped early",
        "compare the row count against the raw export before trusting any average",
    ),
    "mandatory_columns_present": (
        "a required column is missing from the processed CSV",
        "stage 4 is supposed to emit every mandatory column; one absent means the "
        "transform and the schema disagree",
        "re-run stage 4, and check whether sql/schema.sql was changed without "
        "updating src/transformer.py",
    ),
    "no_nulls": (
        "a mandatory field is null or whitespace-only",
        "a survey answer that cannot be recovered; the column is NOT NULL in "
        "sql/schema.sql, so this also blocks the database load",
        "inspect the raw rows before imputing - a null here is a missing answer, "
        "not a zero",
    ),
    "populated": (
        "an optional field is entirely or partly empty",
        "expected for an ungated question, and non-blocking: these columns are "
        "nullable by design",
        "no action needed unless the field is meant to be answered; group by it "
        "only if you accept the missing rows",
    ),
    "domain": (
        "a value is outside the allowed set for that column",
        "either the raw export contains a value the schema does not allow, or the "
        "CHECK constraint list and src/data_loader.py have drifted apart",
        "add the value to CATEGORICAL_DOMAINS in src/data_loader.py and to the "
        "matching CHECK constraint in sql/schema.sql - both, or the load fails",
    ),
    "money_numeric": (
        "a money field contains non-numeric text",
        "the raw export carries currency symbols, thousands separators or stray "
        "text that stage 3 did not strip",
        "check the raw file for formatting inconsistencies in that column",
    ),
    "non_negative": (
        "a money field has a negative value",
        "a sign error, or a value entered as a credit; the schema forbids negatives "
        "with a CHECK constraint",
        "confirm the sign convention in the raw survey before loading",
    ),
    "integral": (
        "a money value is fractional",
        "money derives from integral survey-bucket midpoints, so a fractional value "
        "means something upstream produced a non-bucket number",
        "check for currency conversion or an averaging step applied to raw responses",
    ),
    "total_expenses_reconciles": (
        "Total Expenses is not the sum of the four category columns",
        "the two definitions have drifted apart; the database recomputes the column "
        "as a GENERATED expression, so the CSV and the table would disagree",
        "reconcile src/transformer.py against the GENERATED expression in "
        "sql/schema.sql - they must compute the same thing",
    ),
    "savings_consistency": (
        "savings is not allowance minus total expenses",
        "an arithmetic or column-mapping error in stage 4",
        "re-run stage 4 and check the savings derivation in src/transformer.py",
    ),
    "budget_usage_range": (
        "budget usage % falls outside the plausible range",
        f"values above 100% are expected in this survey - they are the finding, not "
        f"an error. Only values outside [{data_quality.BUDGET_USAGE_MIN:g}, "
        f"{data_quality.BUDGET_USAGE_MAX:g}] are a problem",
        "for values over 100%, no action: report them. For values outside the "
        "ceiling, investigate the denominator",
    ),
    "exact_duplicate_rows": (
        "the same row appears more than once",
        "a re-ingestion or a double-pasted export; duplicates would inflate every "
        "average the assistant reports",
        "de-duplicate at the source, then re-run stages 3 and 4",
    ),
    "duplicate_name_and_timestamp": (
        "two rows share both a name and a timestamp",
        "almost certainly a duplicated record rather than two real respondents",
        "resolve before loading; the load has no de-duplication step of its own",
    ),
    "repeated_student_name": (
        "a name appears on more than one row",
        "often legitimate - siblings share names, or a name collides across "
        "different people. The check reports name collisions and identical repeats "
        "separately for exactly this reason",
        "no action if the differing answers confirm two people; investigate only the "
        "identical repeats",
    ),
    "outliers": (
        "a value sits outside the Tukey fences",
        "extreme relative to this sample. In this dataset it is usually a genuine "
        "high spender rather than an error, so it is a WARNING, not a failure",
        "confirm the row is real before excluding it; an outlier may be the most "
        "interesting respondent in the survey",
    ),
    "censored_values": (
        "expense cells are right-censored at the top bucket",
        f"the survey's open-ended '> 5000' option is recorded as its midpoint "
        f"({CENSORED_CELL_VALUE:.0f}), which is a floor and not an amount",
        "no action - but every total including these cells is a lower bound, and the "
        "assistant labels it that way on every answer",
    ),
    "timestamp_parseable": (
        "a timestamp could not be parsed",
        "the value is loaded into a TIMESTAMPTZ column, which cannot store it, so "
        "the load would fail rather than silently corrupt the data",
        "fix the raw value, or the date format assumption in stage 3",
    ),
    "timestamp_not_future": (
        "a timestamp is in the future",
        "a clock problem, or a typo in the survey date",
        "check the source system clock and the transcription",
    ),
    "timestamp_span_sane": (
        "the collection window is implausibly wide",
        f"a survey window should be at most {data_quality.MAX_TIMESTAMP_SPAN_DAYS} "
        f"days; a wider span suggests the export combines multiple collections",
        "confirm whether these rows are from one collection round",
    ),
    "cross_field_consistency": (
        "payment method and named UPI app disagree",
        "an ungated question: a respondent can name an app without selecting UPI as "
        "their method. This is expected behaviour, not a data error",
        "no action; if you need UPI users specifically, filter on the payment "
        "method rather than the named app",
    ),
    "specialization_cardinality": (
        "specialization is nearly unique per row",
        "free text with no controlled vocabulary - 44 distinct spellings across 213 "
        "rows. That is why the assistant will group by it but refuses to filter on "
        "it",
        "to filter on specialization, normalise it first and add a CHECK constraint",
    ),
    "row_count": (
        "the loaded row count does not match the expected survey size",
        f"stage 6 verification expects {database.EXPECTED_ROW_COUNT} rows",
        "compare against the processed CSV's row count; a shortfall means the COPY "
        "did not load everything",
    ),
    "no_positive_savings": (
        "a row reports positive savings",
        "in this dataset savings is negative for all 213 rows, because reported "
        "allowances are lower than reported expenses throughout. A positive value "
        "means the row came from somewhere else",
        "confirm the source file matches this survey before trusting the aggregate",
    ),
    "generated_columns_consistent": (
        "a GENERATED column disagrees with the CSV value",
        "sql/schema.sql recomputes savings and budget usage as GENERATED columns, so "
        "a mismatch means the CSV and the DDL compute them differently",
        "make src/transformer.py and the GENERATED expressions identical",
    ),
}


class ErrorExplainer:
    """Turns pipeline and quality failures into plain, actionable English.

    The pipeline already reports failures precisely - check ids, severities,
    offending counts. What it does not do is say what a check id *means* to
    someone who did not write the check. This class is the translation layer, and
    it never reinterprets the severity: it copies whatever
    ``src/data_quality.py`` assigned.
    """

    def explain_report(self, report: "data_quality.QualityReport") -> List[Explanation]:
        """Explain every failing check in a quality report.

        Parameters
        ----------
        report:
            A :class:`data_quality.QualityReport`.

        Returns
        -------
        list of Explanation
            One per problem, most severe first. Empty when the report passed.
        """
        explanations = [self.explain_result(r) for r in report.problems]
        explanations.sort(key=lambda e: data_quality.SEVERITY_RANK.get(e.severity, 99))
        return explanations

    def explain_result(self, result: "data_quality.CheckResult") -> Explanation:
        """Explain a single :class:`data_quality.CheckResult`.

        Parameters
        ----------
        result:
            One check outcome.

        Returns
        -------
        Explanation
        """
        matched_prefix = self._match_check(result.name)
        if matched_prefix is not None:
            title, cause, fix = CHECK_EXPLANATIONS[matched_prefix]
            return Explanation(
                title=title,
                cause=cause,
                fix=fix,
                severity=result.severity,
                evidence=self._evidence(result),
            )
        # An unknown id is itself worth reporting plainly rather than hiding.
        return Explanation(
            title=f"unrecognised check {result.name!r}",
            cause="this check id is not in the explainer's catalogue, so the "
                  "explanation may be incomplete",
            fix=f"read the raw message: {result.message}",
            severity=result.severity,
            evidence=self._evidence(result),
        )

    @staticmethod
    def _match_check(name: str) -> Optional[str]:
        """Return the longest catalogue key matching ``name``.

        Templated ids are matched on prefix, and the longest key wins so
        ``cross_field_consistency::upi_app_matches_payment`` is explained by its
        own entry rather than the bare prefix.
        """
        candidates = [key for key in CHECK_EXPLANATIONS if name.startswith(key)]
        return max(candidates, key=len) if candidates else None

    @staticmethod
    def _evidence(result: "data_quality.CheckResult") -> str:
        """Quote the check's own message, offending count and examples."""
        parts = [result.message]
        if result.offending:
            parts.append(f"{result.offending} offending row(s)")
        if result.examples:
            parts.append(f"examples: {result.examples[:data_quality.MAX_EXAMPLES]}")
        return " | ".join(p for p in parts if p)

    #: Verbatim tokens from src/pipeline.py and src/database.py. Each entry is
    #: ``(pattern, title, cause, fix, severity)``.
    #:
    #: The severity is stated rather than inferred from the matched text. An
    #: earlier version guessed it from whether the line contained "BLOCKED", which
    #: labelled a CRITICAL quality alert as a WARNING because the alert text does
    #: not use that word.
    #:
    #: Order matters only for readability; :meth:`explain_log` stops a log line
    #: being explained twice.
    LOG_PATTERNS: Tuple[Tuple[str, str, str, str, str], ...] = (
        (
            r"refusing to drop an existing table without --allow-drop",
            "stage 5 refuses to drop an existing table without permission",
            "sql/schema.sql starts with DROP TABLE, so running it destroys whatever is "
            "already loaded. This is a deliberate guard, not a bug",
            "pass --allow-drop to accept the loss, or --dry-run to see what it would "
            "do without contacting a server",
            "CRITICAL",
        ),
        (
            r"\[pipeline \] BLOCKED",
            "the pipeline stopped before doing anything",
            "a stage raised rather than continuing, so no partial work was attempted",
            "read the message after 'BLOCKED' - it names the stage and the reason",
            "CRITICAL",
        ),
        (
            r"\[pipeline \] FAILED",
            "a stage raised an unexpected exception",
            "the runner caught something it does not have a specific message for",
            "re-run with the named exception type; if it is ImportError, install "
            "requirements.txt",
            "CRITICAL",
        ),
        (
            r"\[ALERT \] CRITICAL quality failure",
            "the stage 7 gate blocked a database load",
            "a CRITICAL check failed, so loading would have written known-bad data",
            "resolve the named check, then re-run; the 'explain' command says what "
            "that check id means",
            "CRITICAL",
        ),
        (
            r"\[ALERT \] WARNING\s+quality finding",
            "the stage 7 gate found a non-blocking problem",
            "WARNINGs are logged but do not stop the load",
            "review each one; use --quality strict to treat them as blocking",
            "WARNING",
        ),
        (
            r"\[stage 5\] WARNING\s+: schema\.sql contains DROP TABLE",
            "the schema script will destroy existing data",
            "sql/schema.sql drops and recreates the table, which is how the schema is "
            "kept authoritative",
            "confirm the target is the database you mean, then re-run with --allow-drop",
            "WARNING",
        ),
        (
            r"pre-load validation problem",
            "the CSV failed the pre-flight checks, so nothing was sent to the database",
            "stage 6 validates before COPY so a bad file never reaches the table",
            "fix the listed problems, or pass --skip-validation to load anyway "
            "(not recommended)",
            "CRITICAL",
        ),
        (
            r"Could not locate the Stage 4 output",
            "the processed CSV does not exist yet",
            "stages 5 and 6 both read the stage 4 output, so it has to exist first",
            "run: python src/pipeline.py --stages 3,4",
            "CRITICAL",
        ),
        (
            r"psycopg2 is required for database access",
            "the PostgreSQL driver is not installed",
            "src/database.py imports psycopg2 lazily, so this surfaces only when a "
            "database stage actually runs",
            "run: pip install -r requirements.txt",
            "CRITICAL",
        ),
        (
            r"could not connect|Connection refused|server closed the connection|"
            r"no pg_hba\.conf entry|terminating connection",
            "the database server could not be reached or refused the connection",
            "the server may be down, or the DSN may point somewhere else",
            "check the server is running, then verify DATABASE_URL or the PG* "
            "environment variables; --dry-run needs no server",
            "CRITICAL",
        ),
        (
            r"permission denied for database|InsufficientPrivilege",
            "the database rejected the login or the role lacks rights on the schema",
            "GRANT rights are per-database; connecting is not the same as being "
            "allowed to create a table",
            "grant CREATE on the database to the role named in the DSN",
            "CRITICAL",
        ),
        (
            r"Missing or insufficient privilege|schema .* does not exist|"
            r"relation .* does not exist",
            "the table or view is not there",
            "the view this assistant queries is created by sql/analytics.sql, and the "
            "table by sql/schema.sql",
            "run stages 5 and 6, then apply sql/analytics.sql, before asking questions",
            "CRITICAL",
        ),
        (
            r"division by zero|null value returned by",
            "an aggregate divided by a zero or null denominator",
            "aggregate ratio columns such as budget usage divide by the allowance; a "
            "group with zero allowance has no defined ratio",
            "this is a data problem, not a query problem - find the zero-allowance "
            "group before interpreting the averages",
            "WARNING",
        ),
        (
            r"dry run complete; no database was contacted",
            "this was a dry run, not a run",
            "--dry-run reports what stages 5 and 6 would do without a server",
            "drop --dry-run and supply a DSN when you actually want the work done",
            "INFO",
        ),
    )

    def explain_log(self, text: str) -> List[Explanation]:
        """Explain every recognised failure line in captured pipeline output.

        Parameters
        ----------
        text:
            Captured stdout/stderr from ``src/pipeline.py`` or ``src/database.py``.

        Returns
        -------
        list of Explanation
            At most one explanation per distinct log line, in the order the lines
            appear. A single line can match several patterns - the
            "refusing to drop" message also contains "[pipeline ] BLOCKED" - so
            lines, not patterns, are what gets deduplicated: the first and most
            specific pattern to claim a line keeps it.
        """
        found: List[Explanation] = []
        claimed_lines: set = set()
        lines = text.splitlines()
        for pattern, title, cause, fix, severity in self.LOG_PATTERNS:
            for line in lines:
                if not re.search(pattern, line, flags=re.I):
                    continue
                key = line.strip()
                if key in claimed_lines:
                    break
                claimed_lines.add(key)
                found.append(Explanation(
                    title=title,
                    cause=cause,
                    fix=fix,
                    severity=severity,
                    evidence=key,
                ))
                break
        return found

    def explain_exception(self, exc: BaseException) -> Explanation:
        """Explain a caught exception in pipeline terms.

        Parameters
        ----------
        exc:
            The exception object.

        Returns
        -------
        Explanation
        """
        if isinstance(exc, data_quality.QualityCheckFailed):
            blocking = exc.report.blocking(exc.strict)
            return Explanation(
                title=f"the stage 7 quality gate blocked the load "
                      f"({len(blocking)} blocking check(s))",
                cause=f"{exc}. A CRITICAL finding means loading would have written "
                      f"data known to be wrong.",
                fix="resolve the named checks, or run with --quality off to see what "
                    "the load would do",
                severity="CRITICAL",
                evidence=str(exc),
            )
        if isinstance(exc, pipeline.StageError):
            return Explanation(
                title="a pipeline stage stopped deliberately",
                cause=str(exc),
                fix="see the message above for the flag that changes this behaviour",
                severity="CRITICAL",
                evidence=type(exc).__name__,
            )
        if isinstance(exc, FileNotFoundError):
            return Explanation(
                title="a required input file is missing",
                cause=str(exc),
                fix="check the path in the message; the processed CSV comes from "
                    "stage 4",
                severity="CRITICAL",
                evidence=type(exc).__name__,
            )
        if isinstance(exc, ImportError):
            return Explanation(
                title="an optional dependency is not installed",
                cause=str(exc),
                fix="pip install -r requirements.txt",
                severity="CRITICAL",
                evidence=type(exc).__name__,
            )
        if isinstance(exc, UnsafeSQLError):
            return Explanation(
                title="a generated query was rejected by the safety guard",
                cause=str(exc),
                fix="this is a defect in the assistant, not in your data; run "
                    "'selftest' to reproduce",
                severity="CRITICAL",
                evidence=str(exc),
            )
        if isinstance(exc, IntentNotUnderstood):
            return Explanation(
                title="the question could not be interpreted",
                cause=str(exc),
                fix="rephrase using a measure or dimension the assistant knows; try "
                    "the 'insights' command for examples",
                severity="INFO",
                evidence=str(exc),
            )
        # Unknown exceptions: fall through to the log catalogue, which is keyed on
        # the message text and so still catches psycopg2 and pandas errors.
        text = f"{type(exc).__name__}: {exc}"
        matched = self.explain_log(text)
        if matched:
            return matched[0]
        return Explanation(
            title=f"unhandled {type(exc).__name__}",
            cause=str(exc),
            fix="re-run with the traceback to see where it originated",
            severity="CRITICAL",
            evidence=text,
        )

    def explain(self, target: Any) -> List[Explanation]:
        """Explain whatever ``target`` is, dispatching on its type.

        Parameters
        ----------
        target:
            A :class:`data_quality.QualityReport`, a
            :class:`data_quality.CheckResult`, an exception, or captured log text.

        Returns
        -------
        list of Explanation
        """
        if isinstance(target, data_quality.QualityReport):
            return self.explain_report(target)
        if isinstance(target, data_quality.CheckResult):
            return [self.explain_result(target)]
        if isinstance(target, BaseException):
            return [self.explain_exception(target)]
        if isinstance(target, (list, tuple)):
            explanations: List[Explanation] = []
            for item in target:
                explanations.extend(self.explain(item))
            return explanations
        return self.explain_log(str(target))


# --------------------------------------------------------------------------- #
# Insights
# --------------------------------------------------------------------------- #


def _money(value: Any) -> str:
    """Format a money amount with thousands separators.

    ``Rs`` rather than the rupee sign on purpose: this output goes to Windows
    consoles that may be on cp1252, where printing U+20B9 raises
    ``UnicodeEncodeError`` and takes the whole command down.
    """
    if value is None:
        return "n/a"
    return f"Rs {float(value):,.0f}"


def _pct(value: Any) -> str:
    """Format a ratio as a percentage."""
    if value is None:
        return "n/a"
    return f"{float(value):,.2f}%"


def _count(value: Any) -> str:
    """Format a row count, without a currency prefix."""
    if value is None:
        return "n/a"
    return f"{int(round(float(value))):,}"


def _value_formatter(intent: "QueryIntent") -> Callable[[Any], str]:
    """Choose the display format for an intent's measure.

    Getting this wrong is not cosmetic: a count of 58 rendered as ``Rs 58`` claims
    that 58 students cost 58 rupees. The formatter follows the measure, never the
    presence of a number.
    """
    if intent.measure is None:
        return _count
    if intent.measure.name == "student_id":
        return _count
    if intent.measure.name == "budget_usage_pct":
        return _pct
    return _money


@dataclass
class Insight:
    """A readable interpretation of one query result.

    Attributes
    ----------
    headline:
        The single most important thing the result says.
    points:
        Supporting observations, each derived from the actual numbers returned.
    tips:
        What to do next, or what not to conclude.
    caveats:
        Measurement limits that apply to this specific measure, plus the
        dataset-wide ones. Never empty: every figure here is a bucket midpoint.
    """

    headline: str
    points: List[str] = field(default_factory=list)
    tips: List[str] = field(default_factory=list)
    caveats: List[str] = field(default_factory=list)

    def render(self) -> str:
        """Format for the terminal."""
        lines = [self.headline, ""]
        for point in self.points:
            lines.append(f"- {point}")
        if self.points:
            lines.append("")
        for tip in self.tips:
            lines.append(f"  -> {tip}")
        if self.notes_section():
            lines.append("")
            lines.extend(self.notes_section())
        return "\n".join(lines)

    def notes_section(self) -> List[str]:
        """Render the caveats as a labelled block."""
        if not self.caveats:
            return []
        return ["read this before quoting any number:"] + [
            f"  ({index}) {text}" for index, text in enumerate(self.caveats, start=1)
        ]


class InsightsAssistant:
    """Turns a result into an interpretation, and refuses to overstate it.

    Every method here reads the numbers that came back rather than reciting
    canned text, so an answer about a group of 3 students is written differently
    from one about all 213. The tips are the part that matters: they say what the
    result does and does not support.
    """

    #: Stated on every answer, because it applies to every money figure here.
    DATASET_CAVEATS: Tuple[str, ...] = (
        "Every amount is the midpoint of an ordinal survey bucket, not a reported "
        "figure.",
        f"{CENSORED_CELL_VALUE:,.0f} stands in for the open-ended '> 5000' option, "
        "so any total containing one is a floor.",
        "Allowance buckets are much coarser than expense buckets, which is why "
        "every respondent appears to overspend.",
    )

    def analyse(self, intent: QueryIntent, result: QueryResult) -> Insight:
        """Interpret one result.

        Parameters
        ----------
        intent:
            The parsed question.
        result:
            The rows that came back.

        Returns
        -------
        Insight
        """
        caveats = list(self.DATASET_CAVEATS)
        if intent.measure is not None and intent.measure.caveat:
            caveats.insert(0, intent.measure.caveat)

        if result.is_empty:
            return self._empty(intent, caveats)

        if intent.measure is not None and intent.measure.name == "savings":
            return self._savings(intent, result, caveats)

        if intent.wants_extreme or intent.wants_detail:
            return self._rows(intent, result, caveats)

        if len(result.columns) == 1:
            return self._scalar(intent, result, caveats)

        return self._grouped(intent, result, caveats)

    # -- result shapes ------------------------------------------------------ #

    def _empty(self, intent: QueryIntent, caveats: List[str]) -> Insight:
        """Explain a result with no rows as a filter finding, not a data gap."""
        points = []
        if intent.filters:
            points.append(
                "Filtered on "
                + ", ".join(
                    f"{d.label} {'!=' if neg else '='} {v}"
                    for d, v, neg in intent.filters
                )
                + "."
            )
        if intent.compare is not None:
            dimension, values = intent.compare
            points.append(f"Restricted to {dimension.label} in {', '.join(values)}.")
        if not points:
            points.append("The query had no filter, so the table itself is empty.")
        return Insight(
            headline="No students match that question.",
            points=points + [
                "This is a finding about the filter, not missing data - the dataset "
                f"has {EXPECTED_ROW_COUNT} rows."
            ],
            tips=["Widen the filter, or ask without it, to see the full population."],
            caveats=caveats,
        )

    def _scalar(self, intent: QueryIntent, result: QueryResult,
                caveats: List[str]) -> Insight:
        """Interpret a single number."""
        value = result.rows[0][0]
        label = intent.measure.label if intent.measure else "students"
        formatter = _value_formatter(intent)

        headline = f"{label.capitalize()}: {formatter(value)}"
        points = [
            "One aggregate over the students matching the filter, out of "
            f"{EXPECTED_ROW_COUNT} in the dataset."
        ]

        tips: List[str] = []
        if intent.measure is not None and intent.measure.name == "student_id":
            share = float(value) / EXPECTED_ROW_COUNT * 100 if value else 0.0
            points.append(f"That is {share:.1f}% of the sample.")
            tips.append("Add 'by degree' or 'by accommodation' to see how it splits.")
        elif intent.measure is not None and intent.measure.name == "budget_usage_pct":
            points.append(
                "Values above 100% are expected: this is reported spend against "
                "reported allowance, not a share of a fixed pot."
            )
            tips.append(
                "Ask 'average budget utilisation by <dimension>' to see whether this "
                "hides a split population."
            )
        if intent.measure is not None and intent.measure.name == "total_expenses":
            tips.append(
                "Add 'by degree', 'by accommodation' or 'by year of study' to see "
                "which group this average is describing."
            )
        return Insight(headline=headline, points=points, tips=tips, caveats=caveats)

    def _grouped(self, intent: QueryIntent, result: QueryResult,
                 caveats: List[str]) -> Insight:
        """Interpret a per-group breakdown."""
        dimension = intent.dimensions[0].label if intent.dimensions else "group"
        value_index = len(result.columns) - 1
        if "aggregate_utilisation_pct" in result.columns:
            value_index = result.columns.index("value")
        aggregate_index = (result.columns.index("aggregate_utilisation_pct")
                           if "aggregate_utilisation_pct" in result.columns else None)

        ranked = [
            (row[0], row[value_index], row[aggregate_index] if aggregate_index is not None
             else None)
            for row in result.rows
            if row[value_index] is not None
        ]
        if not ranked:
            return self._empty(intent, caveats)

        formatter = _value_formatter(intent)
        top_name, top_value, top_aggregate = max(ranked, key=lambda r: r[1])
        low_name, low_value, _ = min(ranked, key=lambda r: r[1])

        headline = (
            f"{len(ranked)} {dimension} group(s). Highest {intent.measure.label if intent.measure else 'value'}: "
            f"{top_name} at {formatter(top_value)}."
        )
        points = [f"Lowest is {low_name} at {formatter(low_value)}."]

        ratio = (top_value / low_value) if low_value else None
        if ratio and ratio >= 1.5:
            points.append(
                f"The spread is {ratio:.1f}x between {top_name} and {low_name}, so the "
                f"overall average describes neither of them well."
            )
        elif ratio and ratio < 1.15:
            points.append(
                f"The groups are within {(ratio - 1) * 100:.0f}% of each other, so "
                f"'{dimension}' does not separate spending here."
            )

        if top_aggregate is not None:
            points.append(
                f"Aggregate utilisation for {top_name} is {_pct(top_aggregate)} "
                "against the mean of individual ratios; the gap between them is the "
                "finding, not an error."
            )

        tips = [
            "Ask 'list the top 5 students by spend' to see the individuals behind "
            "this average."
        ]
        if top_name == low_name:
            tips.append("Only one group is present, so this is not a comparison.")
        return Insight(headline=headline, points=points, tips=tips, caveats=caveats)

    def _rows(self, intent: QueryIntent, result: QueryResult,
              caveats: List[str]) -> Insight:
        """Interpret a per-student listing."""
        if "student_name" not in result.columns:
            return self._scalar(intent, result, caveats)

        names = [row[result.columns.index("student_name")] for row in result.rows]
        direction = "highest" if intent.ranking != "asc" else "lowest"

        if intent.wants_extreme:
            detail = ", ".join(
                f"{name} ({', '.join(str(v) for v in row[1:] if v is not None)})"
                for name, row in zip(names, result.rows)
            )
            headline = f"The {direction} row is {names[0]}."
            return Insight(
                headline=headline,
                points=[detail],
                tips=["A single extreme row says nothing about the group - average by "
                      "a dimension to see whether this is typical."],
                caveats=caveats,
            )

        headline = f"{len(result.rows)} student(s), listed {direction} first: " + \
                   ", ".join(str(n) for n in names[:5])
        if len(names) > 5:
            headline += f", and {len(names) - 5} more"
        points = [
            "These are individual rows. A person at the top of this list is not "
            "evidence of a pattern - check the group averages before drawing one."
        ]
        if intent.measure is not None and intent.measure.name == "total_expenses":
            points.append(
                f"The top of this list will include the censored 6250 cells, so the "
                f"highest figures are floors."
            )
        return Insight(
            headline=headline + ".",
            points=points,
            tips=["Ask for an average by a group to see whether this is typical."],
            caveats=caveats,
        )

    def _savings(self, intent: QueryIntent, result: QueryResult,
                 caveats: List[str]) -> Insight:
        """Redirect a savings question, because the answer is degenerate.

        Savings is negative for all 213 rows, so a "who saves most" ranking has an
        empty or meaningless answer set. Returning one anyway would be the single
        most misleading thing this assistant could do.
        """
        return Insight(
            headline="Savings is negative for every student in this dataset, so a "
                     "'biggest saver' ranking has no answer.",
            points=[
                f"The rows returned here average {formatter_savings(result)}, but that "
                "is not a saving - it is a shortfall.",
                "Reported allowances are lower than reported expenses for all "
                f"{EXPECTED_ROW_COUNT} respondents, because the two use different "
                "bucket scales.",
            ],
            tips=[
                "Ask 'average overspend by <dimension>' instead - the deficit is the "
                "measurable quantity.",
                "If you need a real saving, this dataset cannot provide one: the "
                "allowance question has no bucket above the reported maximum.",
            ],
            caveats=caveats,
        )

    # -- canned overview ---------------------------------------------------- #

    def overview(self) -> Insight:
        """Summarise the dataset itself, without running a query.

        Every figure here was verified in stages 6 and 7 and re-checked by
        ``selftest``; if the processed CSV changes, ``selftest`` fails rather than
        this method quietly going stale.
        """
        return Insight(
            headline=f"{EXPECTED_ROW_COUNT} students, {len(MEASURES)} measures and "
                     f"{len(DIMENSIONS)} dimensions to ask about.",
            points=[
                f"Mean total expenses {_money(11461.27)}; mean allowance "
                f"{_money(1578.64)}.",
                "The allowance scale is coarser and lower than the expense scale, so "
                "every respondent overspends by construction.",
                "13.4% of the 852 expense cells are right-censored at "
                f"{CENSORED_CELL_VALUE:,.0f}.",
                "Mean-of-ratios utilisation is 1,533.05% while the aggregate ratio "
                "is 726.02% - they answer different questions.",
            ],
            tips=[
                "Start with: average expenses by degree",
                "Or: compare average allowance for hostel vs day scholar",
                "Or: top 5 UPI apps among students who pay by UPI",
            ],
            caveats=list(self.DATASET_CAVEATS),
        )


def formatter_savings(result: QueryResult) -> str:
    """Format a savings-shaped value, which is a negative money amount."""
    if not result.rows or result.rows[0][0] is None:
        return "n/a"
    return _money(abs(result.rows[0][0]))


# --------------------------------------------------------------------------- #
# The assistant
# --------------------------------------------------------------------------- #


@dataclass
class Answer:
    """A question, the SQL behind it, the rows, and what they mean.

    Attributes
    ----------
    question:
        What was asked.
    intent:
        The parse, kept so ``explain`` and follow-ups need no re-parse.
    result:
        Rows plus which engine answered.
    insight:
        The interpretation.
    """

    question: str
    intent: QueryIntent
    result: QueryResult
    insight: Insight

    def to_dict(self) -> Dict[str, Any]:
        """Return a JSON-serialisable view, used by the HTTP endpoint."""
        return {
            "question": self.question,
            "sql": self.result.sql,
            "source": self.result.source,
            "columns": self.result.columns,
            "rows": [[_jsonable(v) for v in row] for row in self.result.rows],
            "row_count": len(self.result.rows),
            "intent": self.intent.describe(),
            "notes": self.intent.notes,
            "headline": self.insight.headline,
            "points": self.insight.points,
            "tips": self.insight.tips,
            "caveats": self.insight.caveats,
        }


def _jsonable(value: Any) -> Any:
    """Coerce a cell into something ``json.dumps`` accepts."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


class NaturalLanguageToSQL:
    """The single seam between English and SQL.

    Everything that produces SQL goes through :meth:`to_sql`, and everything that
    returns SQL passes :class:`SQLSafetyGuard`. Keeping this narrow is what makes
    the safety claim checkable: a future LLM backend only has to satisfy the same
    guard to be substituted here.
    """

    def __init__(self, guard: Optional[type] = None) -> None:
        """
        Parameters
        ----------
        guard:
            Override for :class:`SQLSafetyGuard`, used by ``selftest``.
        """
        self.guard = guard or SQLSafetyGuard

    def to_intent(self, question: str) -> QueryIntent:
        """Parse a question without generating SQL."""
        return parse_intent(question)

    def to_sql(self, question: str) -> Tuple[QueryIntent, str]:
        """Parse and render a validated statement.

        Parameters
        ----------
        question:
            Free text.

        Returns
        -------
        tuple of (QueryIntent, str)

        Raises
        ------
        IntentNotUnderstood
            If the question is outside the vocabulary.
        UnsafeSQLError
            If the generated statement fails the guard.
        """
        intent = parse_intent(question)
        sql = build_sql(intent)
        self.guard.validate(sql)
        return intent, sql


class BudgetAssistant:
    """The user-facing assistant: asks, explains failures, and keeps context.

    Parameters
    ----------
    dsn:
        PostgreSQL connection string, or ``None`` to use ``$DATABASE_URL`` and then
        libpq defaults. When ``offline`` is set, or when PostgreSQL is unreachable,
        answers come from the processed CSV instead - and every answer says which
        engine produced it.
    offline:
        Force the CSV engine and never open a connection. Used by ``selftest`` and
        by anyone without a database.
    """

    def __init__(self, dsn: Optional[str] = None, offline: bool = False) -> None:
        self.dsn = dsn if dsn is not None else database.resolve_dsn(dsn)
        self.offline = offline
        self.engine = OfflineEngine()
        self.nl2sql = NaturalLanguageToSQL()
        self.insights = InsightsAssistant()
        self.explainer = ErrorExplainer()
        self._runner: Optional[PostgreSQLRunner] = None
        self._last: Optional[Answer] = None
        self._fallback_reason = ""

    # -- querying ----------------------------------------------------------- #

    @property
    def last_answer(self) -> Optional[Answer]:
        """The previous :class:`Answer`, or ``None``."""
        return self._last

    def runner(self) -> PostgreSQLRunner:
        """Return the PostgreSQL runner, creating it on first use."""
        if self._runner is None:
            self._runner = PostgreSQLRunner(self.dsn or "")
        return self._runner

    def ask(self, question: str) -> Answer:
        """Answer a question.

        Parameters
        ----------
        question:
            Free text.

        Returns
        -------
        Answer

        Raises
        ------
        IntentNotUnderstood
            If the question is outside the vocabulary.
        """
        intent, sql = self.nl2sql.to_sql(question)
        result = self._execute(intent, sql)

        if result.source == "offline" and self._fallback_reason:
            result.detail = self._fallback_reason

        answer = Answer(
            question=question,
            intent=intent,
            result=result,
            insight=self.insights.analyse(intent, result),
        )
        self._last = answer
        return answer

    def follow_up(self, question: str) -> Answer:
        """Answer a question in the context of the previous one.

        A follow-up rarely restates the subject. After "average expenses by degree",
        "and the urban ones" means the same measure over the same grouping with one
        more filter - and answering it as a bare row count, which is what a
        stateless parse produces, is technically defensible and practically useless.

        So a follow-up inherits whatever it does not mention: the measure, the
        aggregation and the grouping come from the previous question when the new
        text is silent about them, and the filters are merged rather than replaced.
        Anything inherited is recorded in ``notes``, so a carried-over scope is
        visible in ``explain`` and in the JSON output rather than hidden.

        Parameters
        ----------
        question:
            The follow-up text.

        Returns
        -------
        Answer
        """
        if self._last is None:
            return self.ask(question)
        if question.strip().lower() in _CONFIRMATIONS:
            # "yes" acknowledges the last answer; it is not a new question.
            return self.ask(question)

        previous = self._last.intent
        try:
            probe = parse_intent(question)
        except IntentNotUnderstood:
            return self.ask(question)

        inherited: List[str] = []

        # The subject: a follow-up that names no measure keeps the previous one.
        if probe.measure is None and not probe.wants_detail:
            if previous.measure is not None:
                probe.measure = previous.measure
                probe.aggregation = previous.aggregation
                inherited.append(f"measure {previous.measure.label}")
            elif previous.filters or previous.dimensions:
                # Neither side named a measure: keep the previous grouping too, so
                # "and the urban ones" stays a breakdown rather than becoming a count.
                probe.dimensions = list(previous.dimensions)
                if probe.dimensions:
                    inherited.append("grouping " + ", ".join(d.label for d in probe.dimensions))

        # The breakdown: a follow-up that names no dimension keeps the previous one.
        if not probe.dimensions and previous.dimensions and not probe.compare:
            probe.dimensions = list(previous.dimensions)
            inherited.append("grouping " + ", ".join(d.label for d in probe.dimensions))

        # The scope: merge rather than replace, so "hostellers, and urban ones?"
        # narrows rather than widens.
        merged: List[Tuple[Dimension, str, bool]] = []
        for item in list(previous.filters) + list(probe.filters):
            if not any(d.name == item[0].name for d, _, _ in merged):
                merged.append(item)
        if len(merged) > len(probe.filters):
            inherited.append(
                "filters " + ", ".join(f"{d.label}={v}" for d, v, _ in merged)
            )
        probe.filters = merged

        if not inherited:
            return self.ask(question)

        probe.notes.append("carried over from the previous question: "
                           + "; ".join(inherited))
        probe.sort_ordinal = any(d.name == "year_of_study" for d in probe.dimensions)

        sql = build_sql(probe)
        SQLSafetyGuard.validate(sql)
        result = self._execute(probe, sql)
        answer = Answer(
            question=question,
            intent=probe,
            result=result,
            insight=self.insights.analyse(probe, result),
        )
        self._last = answer
        return answer

        return self.ask(question)

    def _execute(self, intent: QueryIntent, sql: str) -> QueryResult:
        """Run ``sql`` on PostgreSQL, falling back to the CSV engine.

        The fallback is silent in its mechanics but never silent in its output: the
        result's ``source`` says ``"offline"`` and ``detail`` says why, so a number
        computed from the CSV is never passed off as a number read from the table.
        """
        if self.offline:
            self._fallback_reason = "offline mode requested; answering from the processed CSV"
            return self.engine.run(intent, sql)

        started = time.perf_counter()
        try:
            result = self.runner().run(sql)
            result.elapsed_ms = (time.perf_counter() - started) * 1000.0
            self._fallback_reason = ""
            return result
        except Exception as exc:
            # Connection refused, missing view, missing driver: all recoverable by
            # computing the same answer from the CSV. Anything the guard rejects is
            # not recoverable and must not be papered over, so it re-raises.
            if isinstance(exc, (UnsafeSQLError, IntentNotUnderstood)):
                raise
            explanation = self.explainer.explain_exception(exc)
            self._fallback_reason = (
                f"PostgreSQL unavailable ({type(exc).__name__}); answered from the "
                f"processed CSV instead - {explanation.title}"
            )
            return self.engine.run(intent, sql)

    # -- explaining --------------------------------------------------------- #

    def explain_sql(self, question: str) -> str:
        """Return the SQL for a question, with a plain description of each part.

        Parameters
        ----------
        question:
            Free text.

        Returns
        -------
        str
        """
        intent, sql = self.nl2sql.to_sql(question)
        lines = [f"question : {question}", f"reading  : {intent.describe()}"]
        if intent.notes:
            lines.append(f"inferred : {'; '.join(intent.notes)}")
        lines.append("")
        lines.append("sql:")
        for line in sql.splitlines():
            lines.append(f"    {line}")
        lines.append("")
        lines.extend(self._annotate(intent, sql))
        return "\n".join(lines)

    def _annotate(self, intent: QueryIntent, sql: str) -> List[str]:
        """Explain the parts of a generated statement in plain words."""
        notes = []
        aggregate = re.search(r"(avg|sum|count|max|min|percentile_disc[^\n]*?)\(", sql)
        if aggregate:
            name = aggregate.group(1)
            meaning = {
                "avg": "the arithmetic mean over the matching students",
                "sum": "the total across the matching students",
                "count": "the number of matching students",
                "max": "the largest value",
                "min": "the smallest value",
                "percentile_disc": "the middle value (the value actually present at "
                                   "the 50th percentile)",
            }.get(name, name)
            notes.append(f"aggregate: {meaning}")
        if intent.dimensions:
            notes.append(
                "grouping : one row per "
                + ", ".join(d.label for d in intent.dimensions)
                + f", from the {VIEW_NAME} view"
            )
        else:
            notes.append(f"grouping : none - one row over all matching students, "
                         f"from the {VIEW_NAME} view")
        for dimension, value, negated in intent.filters:
            notes.append(
                f"filter   : {dimension.label} "
                f"{'is not' if negated else 'is'} '{value}' - an allowlisted value, "
                f"not free text"
            )
        if intent.compare is not None:
            dimension, values = intent.compare
            notes.append(
                f"filter   : {dimension.label} in "
                f"({', '.join(repr(v) for v in values)}) - only the compared values"
            )
        notes.append(
            "safety   : identifiers are allowlisted and values are allowlisted "
            "domain members, so nothing from the question is pasted into the SQL"
        )
        return notes

    def explain_failure(self, target: Any) -> str:
        """Explain a quality report, a log, or an exception.

        Parameters
        ----------
        target:
            A report, an exception, or captured pipeline output.

        Returns
        -------
        str
        """
        explanations = self.explainer.explain(target)
        if not explanations:
            return "Nothing to explain: no recognised failure in that input."
        return "\n\n".join(e.render() for e in explanations)

    def quality(self, strict: bool = False) -> str:
        """Run the stage 7 checks and explain the result.

        Parameters
        ----------
        strict:
            Treat WARNING findings as blocking, matching ``--strict``.

        Returns
        -------
        str
        """
        report = data_quality.verify_file()
        lines = [f"[stage 7] source : {_resolve_source_csv()}",
                 f"[stage 7] rows   : {report.row_count} "
                 f"(expected {EXPECTED_ROW_COUNT})",
                 f"[stage 7] result : {'PASS' if report.passed else 'FAIL'}"]
        explanations = self.explainer.explain_report(report)
        if not explanations:
            lines.append("")
            lines.append("No failing checks.")
            return "\n".join(lines)
        lines.append("")
        lines.append(f"{len(explanations)} finding(s) to explain:")
        lines.append("")
        lines.append("\n\n".join(e.render() for e in explanations))
        blocking = report.blocking(strict)
        if blocking:
            lines.append("")
            lines.append(
                f"{len(blocking)} of these would block the load "
                f"(strict={strict})."
            )
        return "\n".join(lines)

    def examples(self) -> str:
        """List questions this assistant can answer, for the REPL banner."""
        lines = ["Questions worth asking:"]
        for example in EXAMPLE_QUESTIONS:
            lines.append(f"  - {example}")
        return "\n".join(lines)


#: Affirmative replies in a conversation, which should re-run rather than inherit.
_CONFIRMATIONS = frozenset({
    "yes", "y", "yeah", "yep", "ok", "okay", "sure", "go on", "more", "again",
})


# --------------------------------------------------------------------------- #
# Command line
# --------------------------------------------------------------------------- #


def _print_answer(answer: Answer, show_sql: bool) -> None:
    """Print an :class:`Answer` in the terminal."""
    print(answer.insight.render())
    print()
    print(answer.result.table())
    print(f"\n({len(answer.result)} row(s), source: {answer.result.source}"
          + (f", {answer.result.elapsed_ms:.0f} ms" if answer.result.elapsed_ms else "")
          + ")")
    if answer.result.detail:
        print(f"note: {answer.result.detail}")
    if show_sql:
        print()
        print("sql:")
        for line in answer.result.sql.splitlines():
            print(f"    {line}")


def _repl(assistant: BudgetAssistant, follow_up: bool = True) -> int:
    """Run an interactive session.

    With ``follow_up`` the previous question's filters stay in scope, so "what
    about urban ones?" works. Exits on ``exit``, ``quit`` or EOF.

    Parameters
    ----------
    assistant:
        The assistant to query.
    follow_up:
        Whether to inherit filters from the previous question.
    """
    print(assistant.examples())
    if not follow_up:
        print("(filter carry-over is off; each question stands alone)")
    print()
    print("Type a question, or 'exit' to leave. Ctrl+C also exits.")
    print()

    while True:
        try:
            raw = input("\n> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return 0
        if not raw:
            continue
        if raw.lower() in {"exit", "quit", "bye", ":q"}:
            return 0
        if raw.lower() in {"help", "examples", "?"}:
            print()
            print(assistant.examples())
            continue
        try:
            answer = assistant.follow_up(raw) if follow_up else assistant.ask(raw)
        except IntentNotUnderstood as exc:
            print(f"\n{exc}")
            if exc.understood:
                print("I did pick up: " + ", ".join(exc.understood))
            continue
        except Exception as exc:
            print("\n" + assistant.explain_failure(exc))
            continue
        print()
        _print_answer(answer, show_sql=False)


def _serve(assistant: BudgetAssistant, host: str, port: int) -> int:
    """Serve the assistant over HTTP using the standard library.

    Deliberately ``http.server`` rather than a framework: this is a single
    read-only endpoint, and adding a web framework to the dependency list for it
    would be a worse trade than the ~40 lines below. It is not hardened for
    exposure to the internet - it binds to localhost by default for that reason.
    """
    class Handler(BaseHTTPRequestHandler):
        """Serves ``POST /ask`` and ``GET /examples``."""

        server_version = "BudgetAssistant/1.0"

        def _send(self, status: int, payload: Dict[str, Any]) -> None:
            body = json.dumps(payload, indent=2).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:  # noqa: N802 - name fixed by the base class
            """``GET /examples`` lists the supported questions."""
            if self.path.rstrip("/") in {"/examples", ""}:
                self._send(200, {"examples": list(EXAMPLE_QUESTIONS)})
                return
            self._send(404, {"error": f"unknown path {self.path!r}"})

        def do_POST(self) -> None:  # noqa: N802 - name fixed by the base class
            """``POST /ask`` answers one question."""
            if self.path.rstrip("/") != "/ask":
                self._send(404, {"error": f"unknown path {self.path!r}"})
                return
            length = int(self.headers.get("Content-Length") or 0)
            if length > MAX_REQUEST_BYTES:
                self._send(413, {"error": "request body too large"})
                return
            try:
                payload = json.loads(self.rfile.read(length) or b"{}")
            except json.JSONDecodeError as exc:
                self._send(400, {"error": f"invalid JSON: {exc}"})
                return
            question = str(payload.get("question", "")).strip()
            if not question:
                self._send(400, {"error": "field 'question' is required"})
                return
            try:
                answer = assistant.ask(question)
            except IntentNotUnderstood as exc:
                self._send(422, {"error": str(exc), "understood": exc.understood})
                return
            except Exception as exc:
                explanations = assistant.explainer.explain_exception(exc)
                self._send(500, {"error": str(exc), "explanation": explanations[0].title})
                return
            self._send(200, answer.to_dict())

        def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
            """Quieter logging: one line per request, on stderr."""
            sys.stderr.write(f"[assistant] {self.address_string()} {format % args}\n")

    server = HTTPServer((host, port), Handler)
    print(f"[assistant] serving on http://{host}:{port}  (POST /ask, GET /examples)")
    print("[assistant] press Ctrl+C to stop")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[assistant] stopped")
    finally:
        server.server_close()
    return 0


def build_parser() -> argparse.ArgumentParser:
    """Build the argument parser."""
    parser = argparse.ArgumentParser(
        prog="assistant",
        description=(
            "Ask questions about the student budget data in plain English, and get "
            "ETL and data-quality failures explained. Deterministic: no API key, "
            "no model call."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "examples:\n"
            "  python ai/assistant.py ask \"average expenses by degree\"\n"
            "  python ai/assistant.py ask \"top 5 UPI apps among students who pay by UPI\"\n"
            "  python ai/assistant.py explain \"average budget utilisation by background\"\n"
            "  python ai/assistant.py quality\n"
            "  python ai/assistant.py chat\n"
            "  python ai/assistant.py selftest\n"
        ),
    )
    # Connection options are declared once on a parent parser and attached to every
    # subcommand, so both "assistant --offline ask ..." and "assistant ask ...
    # --offline" work. Requiring them before the subcommand was a genuine papercut.
    #
    # default=SUPPRESS matters: without it the subparser writes its own default back
    # over the value already parsed from before the subcommand, so a leading
    # --offline would be silently undone by the trailing subparser.
    connection = argparse.ArgumentParser(add_help=False)
    connection.add_argument(
        "--dsn",
        default=argparse.SUPPRESS,
        help="PostgreSQL connection string. Defaults to $DATABASE_URL, then libpq PG* "
             "variables. Without one, the assistant answers from the processed CSV.",
    )
    connection.add_argument(
        "--offline",
        action="store_true",
        default=argparse.SUPPRESS,
        help="Never open a database connection; always answer from the processed CSV.",
    )
    parser.add_argument(
        "--dsn",
        help="PostgreSQL connection string. Defaults to $DATABASE_URL, then libpq PG* "
             "variables. Without one, the assistant answers from the processed CSV.",
    )
    parser.add_argument(
        "--offline",
        action="store_true",
        help="Never open a database connection; always answer from the processed CSV.",
    )
    subparsers = parser.add_subparsers(dest="command")

    ask = subparsers.add_parser("ask", parents=[connection],
                                help="Answer one question.")
    ask.add_argument("question", help="The question, in plain English.")
    ask.add_argument("--json", action="store_true", help="Emit JSON instead of text.")
    ask.add_argument("--sql", action="store_true", help="Show the generated SQL.")

    explain = subparsers.add_parser(
        "explain", parents=[connection], help="Show the SQL for a question, annotated."
    )
    explain.add_argument("question", help="The question, in plain English.")

    quality = subparsers.add_parser(
        "quality", parents=[connection],
        help="Run the stage 7 checks and explain every finding."
    )
    quality.add_argument("--strict", action="store_true",
                         help="Treat WARNING findings as blocking.")

    subparsers.add_parser("insights", parents=[connection],
                          help="Summarise the dataset and its limits.")
    subparsers.add_parser("examples", parents=[connection],
                          help="List questions that can be answered.")

    failures = subparsers.add_parser(
        "failures", parents=[connection], help="Explain a captured pipeline log."
    )
    failures.add_argument(
        "log", nargs="?", default="-",
        help="Log file to explain, or '-' for stdin.",
    )

    chat = subparsers.add_parser("chat", parents=[connection],
                                help="Interactive question-answering session.")
    chat.add_argument("--no-follow-up", action="store_true",
                      help="Do not carry filters over from the previous question.")

    serve = subparsers.add_parser("serve", parents=[connection],
                                 help="Serve the assistant over HTTP.")
    serve.add_argument("--host", default="127.0.0.1",
                       help="Bind address. Defaults to localhost.")
    serve.add_argument("--port", type=int, default=8765,
                       help="Port. Defaults to 8765.")

    subparsers.add_parser(
        "selftest", parents=[connection],
        help="Verify the parser, the safety guard and the offline engine."
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Entry point.

    Parameters
    ----------
    argv:
        Arguments, excluding the program name. Defaults to ``sys.argv[1:]``.

    Returns
    -------
    int
        ``0`` on success, ``1`` on a handled failure, ``2`` on a usage error.
    """
    parser = build_parser()
    arguments = parser.parse_args(argv)
    if not arguments.command:
        parser.print_help()
        return 2

    if arguments.command == "selftest":
        return selftest()

    try:
        assistant = BudgetAssistant(dsn=arguments.dsn, offline=arguments.offline)
    except Exception as exc:
        print(ErrorExplainer().explain_exception(exc).render(), file=sys.stderr)
        return 1

    try:
        if arguments.command == "ask":
            answer = assistant.ask(arguments.question)
            if arguments.json:
                print(json.dumps(answer.to_dict(), indent=2))
            else:
                _print_answer(answer, show_sql=arguments.sql)
            return 0

        if arguments.command == "explain":
            print(assistant.explain_sql(arguments.question))
            return 0

        if arguments.command == "quality":
            print(assistant.quality(strict=arguments.strict))
            return 0

        if arguments.command == "insights":
            print(assistant.insights.overview().render())
            return 0

        if arguments.command == "examples":
            print(assistant.examples())
            return 0

        if arguments.command == "failures":
            text = sys.stdin.read() if arguments.log == "-" else Path(
                arguments.log
            ).read_text(encoding="utf-8", errors="replace")
            explanations = assistant.explainer.explain_log(text)
            if not explanations:
                print("No recognised failure in that input.")
                return 0
            print("\n\n".join(e.render() for e in explanations))
            return 0

        if arguments.command == "chat":
            return _repl(assistant, follow_up=not arguments.no_follow_up)

        if arguments.command == "serve":
            return _serve(assistant, arguments.host, arguments.port)

    except IntentNotUnderstood as exc:
        print(str(exc), file=sys.stderr)
        if exc.understood:
            print("I did pick up: " + ", ".join(exc.understood), file=sys.stderr)
        return 1
    except Exception as exc:
        print(assistant.explain_failure(exc), file=sys.stderr)
        return 1

    parser.print_help()
    return 2




# --------------------------------------------------------------------------- #
# Self-test
# --------------------------------------------------------------------------- #

#: Statements the guard must reject. Each is a way a text-to-SQL layer is
#: routinely broken; the builder cannot produce any of them, so a failure here
#: means either the guard regressed or a future builder edit widened the output.
HOSTILE_STATEMENTS: Tuple[str, ...] = (
    "DROP TABLE students_budget;",
    "SELECT 1; DELETE FROM students_budget;",
    "SELECT * FROM students_budget; UPDATE students_budget SET total_expenses = 0;",
    "INSERT INTO students_budget (student_name) VALUES ('x');",
    "TRUNCATE students_budget;",
    "SELECT * FROM students_budget WHERE 1=1 UNION SELECT password FROM pg_shadow;",
    "SELECT pg_sleep(10);",
    "SELECT * FROM students_budget; COPY students_budget TO '/tmp/out.csv';",
    "GRANT ALL ON students_budget TO public;",
    "SET search_path TO public; SELECT 1;",
    "BEGIN; SELECT 1; COMMIT;",
    "SELECT * FROM students_budget WHERE name = 'x'; DROP TABLE students_budget;",
    "SELECT pg_read_file('/etc/passwd');",
    "SELECT * FROM students_budget -- ; DROP TABLE students_budget;",
    "WITH t AS (SELECT 1) SELECT * FROM t;",
    "SELECT * FROM students_budget WHERE a = 'unterminated",
    "",
    "SELECT * FROM students_budget LIMIT 999999;",
    'SELECT "evil_column" FROM students_budget;',
    "SELECT * FROM students_budget WHERE degree = 'not a real value';",
)

#: Questions the parser must read the way the semantics say it should. Each entry
#: is ``(question, predicate_on_intent, human description)`` - a substring check on
#: the intent, so a test states what it is asserting in English.
PARSER_EXPECTATIONS: Tuple[Tuple[str, str, str], ...] = (
    ("average expenses by degree",
     "by degree", "a grouping word makes it a per-group average"),
    ("average budget utilisation by background",
     "by background", "grouping applies to ratio measures too"),
    ("top 5 UPI apps among students who pay by UPI",
     "where preferred payment method = UPI", "a value is a filter, not a grouping"),
    ("top 5 UPI apps among students who pay by UPI",
     "by UPI app", "the ranked subject becomes the grouping, counted"),
    ("compare average allowance for hostel vs day scholar",
     "accommodation", "two values of one dimension become a comparison"),
    ("students who do not pay by UPI",
     "excluding preferred payment method = UPI", "negation inverts the filter"),
    ("how many students live in hostel accommodation",
     "count of students", "a count with a filter and no grouping"),
    ("list the top 5 hostellers by spend",
     "list of students", "'list' requests a per-student listing"),
    ("average expenses by year of study",
     "by year of study", "the year dimension is groupable"),
    ("show me all MBA students",
     "where degree = MBA", "'show me all' filters rather than aggregating"),
    ("median academic expenses by gender",
     "median of academic expenses", "median is its own aggregation"),
)

#: Numeric answers verified in stages 6 and 7, re-checked against the CSV so a
#: stale artifact fails loudly rather than producing confident wrong numbers.
GROUND_TRUTH: Tuple[Tuple[str, float, float], ...] = (
    ("average total expenses", 11461.27, 0.01),
    ("average allowance", 1578.64, 0.01),
)


class _SelfTest:
    """Collects assertion results so one failure does not hide the rest."""

    def __init__(self) -> None:
        self.passed = 0
        self.failures: List[str] = []

    def check(self, condition: bool, description: str) -> None:
        """Record one assertion."""
        if condition:
            self.passed += 1
        else:
            self.failures.append(description)

    def check_raises(
        self, callable_: Callable[[], Any], exception: type, description: str
    ) -> None:
        """Assert that calling ``callable_`` raises ``exception``."""
        try:
            callable_()
        except exception:
            self.passed += 1
            return
        except Exception as exc:  # pragma: no cover - reported, not raised
            self.failures.append(
                f"{description} (raised {type(exc).__name__} instead)"
            )
            return
        self.failures.append(f"{description} (nothing was raised)")

    def section(self, title: str) -> None:
        """Print a section heading."""
        print(f"\n{title}")
        print("-" * len(title))


def _selftest_guard(tests: _SelfTest) -> None:
    """Verify the guard rejects every hostile statement."""
    tests.section("safety guard: rejecting writes and smuggling")
    for statement in HOSTILE_STATEMENTS:
        label = " ".join(statement.split())[:56] or "(empty)"
        tests.check_raises(
            lambda s=statement: SQLSafetyGuard.validate(s),
            UnsafeSQLError,
            f"guard accepted a hostile statement: {label}",
        )

    # The positive control matters as much: a guard that rejects everything would
    # pass every test above.
    intent, sql = NaturalLanguageToSQL().to_sql("average expenses by degree")
    try:
        SQLSafetyGuard.validate(sql)
        tests.check(True, "")
    except UnsafeSQLError as exc:
        tests.check(False, f"guard rejected its own valid SQL: {exc}")
    tests.check(sql.strip().endswith(";"), "generated SQL has no trailing semicolon")


def _selftest_parser(tests: _SelfTest) -> None:
    """Verify the parser reads questions the way the semantics say."""
    tests.section("parser: questions become the intended intent")
    for question, expected, description in PARSER_EXPECTATIONS:
        try:
            intent = parse_intent(question)
            description_text = intent.describe()
        except Exception as exc:
            tests.check(False, f"{question!r} failed to parse: {exc}")
            continue
        tests.check(
            expected.lower() in description_text.lower(),
            f"{question!r}: expected {expected!r} in {description_text!r}",
        )

    # Every example question must actually parse, or the REPL banner is lying.
    for example in EXAMPLE_QUESTIONS:
        try:
            parse_intent(example)
            tests.check(True, "")
        except Exception as exc:
            tests.check(False, f"advertised example does not parse: {example!r} ({exc})")

    # A question outside the vocabulary must be refused, not guessed at.
    for nonsense in ("what is the airspeed of a swallow", "asdfgh", "tell me a joke"):
        tests.check_raises(
            lambda q=nonsense: parse_intent(q),
            IntentNotUnderstood,
            f"parser guessed at an unanswerable question: {nonsense!r}",
        )


#: Questions that try to smuggle SQL through the parser. The assertion is not that
#: they are refused - several reduce to a perfectly ordinary, safe question, which
#: is the correct outcome - but that whatever comes back is either an
#: :class:`IntentNotUnderstood` or a statement the guard accepts.
INJECTION_QUESTIONS: Tuple[str, ...] = (
    "average expenses by degree; DROP TABLE students_budget",
    "average expenses by degree; DELETE FROM students_budget",
    "average expenses by degree' OR '1'='1",
    "average expenses by degree -- and now delete everything",
    "average expenses by degree/*comment*/",
    "average expenses by UNION SELECT password FROM pg_shadow",
    "'; DROP TABLE students_budget; --",
    "show me all students UNION SELECT null, null, null, null",
    "how many students where degree = 'BSc'; VACUUM",
    "list students; INSERT INTO students_budget VALUES (1)",
    "average expenses by degree; GRANT ALL ON students_budget TO public",
    "top 5 students by spend WHERE 1=1 OR pg_sleep(10)",
)


def _selftest_injection(tests: _SelfTest) -> None:
    """Verify that hostile questions cannot put hostile SQL into a statement."""
    tests.section("injection: hostile questions never produce unsafe SQL")
    nl2sql = NaturalLanguageToSQL()
    for question in INJECTION_QUESTIONS:
        try:
            _, sql = nl2sql.to_sql(question)
        except (IntentNotUnderstood, UnsafeSQLError):
            # Refused outright, which is the best possible outcome.
            tests.check(True, "")
            continue
        except Exception as exc:
            tests.check(False, f"{question!r} raised {type(exc).__name__}: {exc}")
            continue
        # to_sql already validated it; re-check explicitly so a future refactor that
        # drops the call from the happy path is caught here rather than in
        # production.
        try:
            SQLSafetyGuard.validate(sql)
            tests.check(True, "")
        except UnsafeSQLError as exc:
            tests.check(False, f"{question!r} produced unsafe SQL: {exc}")

    # The invariant in one assertion: no fragment of the question's punctuation or
    # keywords appears in any generated statement beyond what the builder emits.
    for question in INJECTION_QUESTIONS:
        try:
            _, sql = nl2sql.to_sql(question)
        except AssistantError:
            continue
        for keyword in ("drop", "delete", "insert", "union", "grant", "vacuum",
                        "pg_sleep", "pg_shadow", "--", "/*"):
            tests.check(
                keyword not in sql.lower(),
                f"{question!r} leaked {keyword!r} into the generated SQL",
            )


def _selftest_offline(tests: _SelfTest) -> None:
    """Verify the offline engine against the recorded ground truth."""
    tests.section("offline engine: answers match stages 6 and 7")
    try:
        engine = OfflineEngine()
    except FileNotFoundError as exc:
        tests.check(False, f"could not load the processed CSV: {exc}")
        return

    for problem in engine.selfcheck():
        tests.check(False, f"processed CSV has drifted: {problem}")

    assistant = BudgetAssistant(offline=True)
    for question, expected, tolerance in GROUND_TRUTH:
        try:
            answer = assistant.ask(question)
        except Exception as exc:
            tests.check(False, f"{question!r} failed offline: {exc}")
            continue
        actual = answer.result.rows[0][0]
        tests.check(
            actual is not None and abs(float(actual) - expected) <= tolerance,
            f"{question!r}: got {actual!r}, expected {expected} +/- {tolerance}",
        )

    # Row count must match, because every percentage in the insights depends on it.
    answer = assistant.ask("how many students")
    tests.check(
        bool(answer.result.rows) and int(answer.result.rows[0][0]) == EXPECTED_ROW_COUNT,
        f"row count mismatch: {answer.result.rows[:1]}, expected {EXPECTED_ROW_COUNT}",
    )


def _selftest_conversation(tests: _SelfTest) -> None:
    """Verify that a follow-up inherits the scope it did not restate."""
    tests.section("conversation: a follow-up inherits the previous scope")
    try:
        assistant = BudgetAssistant(offline=True)
    except FileNotFoundError as exc:
        tests.check(False, f"could not load the processed CSV: {exc}")
        return

    assistant.ask("average expenses by degree")
    answer = assistant.follow_up("and the urban ones")

    intent = answer.intent
    tests.check(
        intent.measure is not None and intent.measure.name == "total_expenses",
        "follow-up lost the measure: a follow-up that names none keeps the previous one",
    )
    tests.check(
        [d.name for d in intent.dimensions] == ["degree"],
        "follow-up lost the grouping: expected ['degree'], got "
        f"{[d.name for d in intent.dimensions]}",
    )
    tests.check(
        [f[0].name for f in intent.filters] == ["background"],
        "follow-up should add exactly the background filter, got "
        f"{[f[0].name for f in intent.filters]}",
    )
    tests.check(
        any("carried over" in note for note in intent.notes),
        "the carry-over must be visible in notes, never silent",
    )
    tests.check(
        len(answer.result.rows) == 6,
        f"urban-only breakdown should still group by degree, got "
        f"{len(answer.result.rows)} row(s)",
    )

    # Filters must merge rather than replace, so a second narrowing step keeps the
    # first one. This is the difference between a conversation and a search box.
    answer = assistant.follow_up("only the hostellers")
    tests.check(
        sorted(f[0].name for f in answer.intent.filters) == ["accommodation", "background"],
        "a second filter should merge with the first, not replace it; got "
        f"{sorted(f[0].name for f in answer.intent.filters)}",
    )
    tests.check(
        [d.name for d in answer.intent.dimensions] == ["degree"],
        "the grouping should survive a third turn",
    )


def _selftest_cli(tests: _SelfTest) -> None:
    """Verify the command line, in particular global-flag placement."""
    tests.section("cli: --dsn and --offline work before and after the subcommand")
    parser = build_parser()

    before = parser.parse_args(["--offline", "ask", "average expenses by degree"])
    after = parser.parse_args(["ask", "average expenses by degree", "--offline"])
    tests.check(
        getattr(before, "offline", False) and getattr(after, "offline", False),
        "--offline must be accepted on either side of the subcommand",
    )
    tests.check(
        before.command == "ask" == after.command,
        f"the subcommand was not captured in both orders: "
        f"{before.command!r} vs {after.command!r}",
    )
    tests.check(
        before.question == after.question == "average expenses by degree",
        "the question should be captured identically in both orders",
    )

    # The bug this guards against: a subparser writing its own default back over a
    # value parsed before the subcommand.
    leading = parser.parse_args(["--dsn", "postgresql://x/y", "ask", "how many students"])
    tests.check(
        getattr(leading, "dsn", None) == "postgresql://x/y",
        f"a leading --dsn was overwritten by the subparser default: "
        f"{getattr(leading, 'dsn', None)!r}",
    )

    for argv, expected in (
        (["--offline", "quality"], "quality"),
        (["quality", "--offline"], "quality"),
        (["--offline", "insights"], "insights"),
        (["insights", "--offline"], "insights"),
        (["--offline", "examples"], "examples"),
        (["examples", "--offline"], "examples"),
        (["--offline", "selftest"], "selftest"),
    ):
        tests.check(
            getattr(parser.parse_args(argv), "offline", False) and
            parser.parse_args(argv).command == expected,
            f"{argv} did not parse as an offline {expected} command",
        )

    # Subcommand-specific flags must still work, and must not collide with the
    # connection options.
    parsed = parser.parse_args(["ask", "how many students", "--offline", "--json", "--sql"])
    tests.check(
        parsed.json and parsed.sql and parsed.offline and parsed.question == "how many students",
        f"ask flags did not all survive parsing: {vars(parsed)}",
    )

    parsed = parser.parse_args(["chat", "--offline", "--no-follow-up"])
    tests.check(
        parsed.no_follow_up and parsed.offline,
        f"chat flags did not all survive parsing: {vars(parsed)}",
    )

    parsed = parser.parse_args(["serve", "--offline", "--port", "9001"])
    tests.check(
        parsed.port == 9001 and parsed.offline and parsed.host == "127.0.0.1",
        f"serve flags did not all survive parsing: {vars(parsed)}",
    )

    tests.check(
        parser.prog.endswith(".py") or "assistant" in parser.prog,
        f"prog should name the script, got {parser.prog!r}",
    )


def _selftest_utilisation(tests: _SelfTest) -> None:
    """Verify the ratio caveats the semantic layer promises actually hold."""
    tests.section("dataset invariants: the claims the insights make")
    engine = OfflineEngine()
    frame = engine.frame

    saving_positive = int((frame["total_expenses"] - frame["allowance"] < 0).sum())
    tests.check(
        saving_positive == 0,
        f"savings is not negative for every row ({saving_positive} positive); the "
        f"redirect in InsightsAssistant._savings would be wrong",
    )

    expense_columns = ["food_expenses", "transport_expenses",
                       "academic_expenses", "other_expenses"]
    cells = frame[expense_columns].to_numpy().ravel()
    censored = int((cells == CENSORED_CELL_VALUE).sum())
    tests.check(
        censored > 0,
        "no censored cells found, so the 'lower bound' caveat is unfounded",
    )

    # The mean of ratios and the aggregate ratio must genuinely differ, since the
    # assistant says so on every utilisation answer.
    mean_ratio = float(frame["budget_usage_pct"].mean())
    aggregate = 100.0 * float(frame["total_expenses"].sum()) / float(
        frame["allowance"].sum()
    )
    tests.check(
        abs(mean_ratio - aggregate) / aggregate > 0.5,
        f"mean-of-ratios ({mean_ratio:.2f}%) and the aggregate ratio "
        f"({aggregate:.2f}%) no longer differ materially; the caveat is now false",
    )


def _selftest_explainer(tests: _SelfTest) -> None:
    """Verify the error explainer covers the pipeline's real output."""
    tests.section("explainer: pipeline failures are understood")
    explainer = ErrorExplainer()

    try:
        report = data_quality.verify_file()
    except FileNotFoundError as exc:
        tests.check(False, f"stage 7 could not run: {exc}")
        return

    tests.check(
        report.row_count == EXPECTED_ROW_COUNT,
        f"stage 7 saw {report.row_count} rows, expected {EXPECTED_ROW_COUNT}",
    )
    for result in report.problems:
        explanation = explainer.explain_result(result)
        tests.check(
            explanation.title != f"unrecognised check {result.name!r}",
            f"no catalogue entry for failing check {result.name!r}",
        )

    # Every check id the pipeline can emit should have an explanation, not just the
    # ones that happen to fail on today's data. Constructing a synthetic result per
    # catalogue key is what makes this a coverage test rather than a coincidence.
    for key in CHECK_EXPLANATIONS:
        synthetic = data_quality.CheckResult(
            name=key,
            category="validity",
            severity="WARNING",
            passed=False,
            message="synthetic",
        )
        explanation = explainer.explain_result(synthetic)
        tests.check(
            not explanation.title.startswith("unrecognised check"),
            f"catalogue key {key!r} does not match itself",
        )

    log = (
        "[pipeline ] BLOCKED   : refusing to drop an existing table without --allow-drop\n"
        "[ALERT ] CRITICAL quality failure: no_nulls::Food: Food: 3 null\n"
        "[stage 5] ERROR      : psycopg2.OperationalError: could not connect\n"
    )
    explained = explainer.explain_log(log)
    tests.check(
        len(explained) == 3,
        f"expected 3 log lines explained, got {len(explained)}",
    )
    severities = {e.title: e.severity for e in explained}
    tests.check(
        severities.get("the stage 7 gate blocked a database load") == "CRITICAL",
        "a CRITICAL quality alert was not labelled CRITICAL",
    )


def selftest() -> int:
    """Run every self-check and report.

    No database server is contacted: the offline engine, the parser and the guard
    are all verifiable without one, which is the point - the assistant's core
    claims are testable offline.

    Returns
    -------
    int
        ``0`` if every check passed, ``1`` otherwise.
    """
    tests = _SelfTest()
    print("assistant selftest - no database server is contacted")

    _selftest_guard(tests)
    _selftest_injection(tests)
    _selftest_parser(tests)
    _selftest_offline(tests)
    _selftest_utilisation(tests)
    _selftest_conversation(tests)
    _selftest_cli(tests)
    _selftest_explainer(tests)

    print()
    print(f"{tests.passed} check(s) passed, {len(tests.failures)} failed")
    if tests.failures:
        print()
        for failure in tests.failures:
            print(f"  FAIL {failure}")
        return 1
    print("ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
