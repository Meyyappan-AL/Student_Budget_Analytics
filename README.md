# Student Budget Analytics

A reproducible pipeline that turns a raw survey export into a queryable PostgreSQL
table, plus a deterministic assistant that answers questions about the result in
plain English.

There is no model in this project. The assistant maps known English phrases onto an
allowlisted set of measures and dimensions, builds one read-only `SELECT`, and
refuses anything it does not recognise. That is a deliberate design choice, and it
is what makes the answers checkable: `python ai/assistant.py selftest` verifies the
parser, the safety guard, and every headline number against the processed CSV,
without contacting a database.

---

## Contents

- [Requirements](#requirements)
- [Setup](#setup)
- [Quick start](#quick-start)
- [Pipeline stages](#pipeline-stages)
- [Database](#database)
- [The assistant](#the-assistant)
- [How the assistant guarantees safety](#how-the-assistant-guarantees-safety)
- [Reading the numbers honestly](#reading-the-numbers-honestly)
- [Repository layout](#repository-layout)
- [Testing](#testing)
- [Troubleshooting](#troubleshooting)

---

## Requirements

- Python 3.9 or newer
- PostgreSQL 12 or newer, only if you want to load the data or query the live view
- The dependencies in `requirements.txt`

Only `pandas`, `numpy` and `openpyxl` are needed for the data work. `psycopg2` is
imported lazily, so stages 3, 4 and `--dry-run` run fine without it installed.

## Setup

```bash
git clone https://github.com/Meyyappan-AL/Student_Budget_Analytics.git
cd Student_Budget_Analytics

python -m venv .venv
```

Activate it:

```bash
# Windows (PowerShell)
.venv\Scripts\Activate.ps1

# macOS / Linux
source .venv/bin/activate
```

Then install:

```bash
pip install -r requirements.txt
```

To also run the notebooks, install the commented-out notebook extras in
`requirements.txt`.

## Quick start

No database required. From the processed CSV:

```bash
python ai/assistant.py ask "average expenses by degree"
python ai/assistant.py insights
python ai/assistant.py selftest
```

Run the whole pipeline offline to see what it would do:

```bash
python src/pipeline.py --dry-run
```

## Pipeline stages

| Stage | Script | What it does |
| --- | --- | --- |
| 3 | `src/cleaner.py` | Reads the raw `.xlsx` export, standardises column names and values, writes `Data/processed/cleaned_student_budget.csv` |
| 4 | `src/transformer.py` | Derives analysis-ready columns, writes `Data/processed/transformed_student_budget.csv` |
| 5 | `src/database.py` | Applies `sql/schema.sql`: drops and recreates the table with constraints and indexes |
| 6 | `src/data_loader.py` | Bulk-loads the transformed CSV into PostgreSQL |
| 7 | `src/data_quality.py` | Runs the checks and reports findings; gates the load |
| 8 | `sql/analytics.sql` | Defines `v_student_budget_analysis`, the view the assistant queries |
| 9 | `ai/assistant.py` | Natural-language question answering, failure explanation, and the HTTP interface |

Run any subset:

```bash
python src/pipeline.py --stages 3,4
python src/pipeline.py --stages 5,6,7 --dsn "postgresql://user:pass@localhost:5432/budget"
python src/pipeline.py --stages all --quality strict
python src/pipeline.py --stages 5,6,7 --allow-drop --quality-json Data/processed/quality_report.json
```

### The stage 7 quality gate

`--quality` controls how strictly the checks are enforced:

| Value | Behaviour |
| --- | --- |
| `off` | Skip stage 7 entirely |
| `report` | Default. Block on CRITICAL findings, log WARNINGs as alerts |
| `verbose` | Also print the checks that passed |
| `strict` | Also block on WARNING findings |

`--skip-validation` bypasses the pre-flight CSV checks. It is available, and it is
not recommended: the checks exist to catch a bad load before it reaches the
database.

## Database

The connection string is resolved in this order:

1. `--dsn`
2. `$DATABASE_URL`
3. the standard libpq variables (`PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE`)

**Stage 5 is destructive.** `sql/schema.sql` begins with `DROP TABLE`, so it destroys
whatever is already loaded. Stage 5 therefore refuses to run without `--allow-drop`.
Note that the pipeline opens its connection first, so against an unreachable server
you will see the connection error before the drop refusal:

```
refusing to drop an existing table without --allow-drop
(or use --dry-run to inspect without touching the database)
```

The assistant recognises and explains that message:

```bash
python src/pipeline.py --stages 5 2>&1 | tee pipeline.log
python ai/assistant.py failures pipeline.log
```

Load the data:

```bash
python src/pipeline.py --stages 5,6,7 --allow-drop \
    --dsn "postgresql://user:pass@localhost:5432/budget"
```

Inspect it by hand with the view:

```sql
SELECT * FROM v_student_budget_analysis LIMIT 10;
```

## The assistant

`ai/assistant.py` is a single self-contained script. It works offline against the
processed CSV, or against PostgreSQL when a connection is available.

```bash
# One question
python ai/assistant.py ask "average total expenses by year of study"
python ai/assistant.py ask "how many students live in hostel accommodation" --json
python ai/assistant.py ask "compare average allowance for hostel vs day scholar"

# See the SQL and why it is shaped that way
python ai/assistant.py explain "average budget utilisation by background"

# Dataset overview, and what the numbers cannot tell you
python ai/assistant.py insights

# Run the checks and explain every finding
python ai/assistant.py quality
python ai/assistant.py quality --strict

# Explain a failed pipeline run (capture the log first)
python src/pipeline.py --stages 5 2>&1 | tee pipeline.log
python ai/assistant.py failures pipeline.log

# What it can answer
python ai/assistant.py examples

# Interactive session, with follow-up context
python ai/assistant.py chat

# HTTP interface on http://127.0.0.1:8765
python ai/assistant.py serve
```

`--offline` and `--dsn` work on either side of the subcommand, so both of these are
equivalent:

```bash
python ai/assistant.py --offline ask "average expenses by degree"
python ai/assistant.py ask "average expenses by degree" --offline
```

### Follow-up questions

The REPL carries context forward. After asking about one subject, a follow-up that
does not restate it inherits the measure, the aggregation and the grouping, and adds
only what is new:

```
> average expenses by degree
6 degree group(s). Highest total expenses: MBA at Rs 12,819.

> and the urban ones
6 degree group(s). Highest total expenses: MBA at Rs 13,531.
```

Filters merge instead of replacing each other, so the conversation narrows rather
than restarts. Whatever was inherited is listed in `explain` and in the JSON
`notes` field, so a carried-over scope is always visible rather than implied.

### HTTP interface

```bash
python ai/assistant.py serve --offline --port 8765
```

| Method | Path | Body | Returns |
| --- | --- | --- | --- |
| `POST` | `/ask` | `{"question": "..."}` | `200` with the answer, `422` if the question is not answerable |
| `GET` | `/examples` | – | `200` with example questions |

The server binds to `127.0.0.1` by default, rejects request bodies over 8 KB, and
returns JSON errors with the appropriate status code rather than a stack trace.

```bash
curl -s -X POST http://127.0.0.1:8765/ask \
     -H "Content-Type: application/json" \
     -d '{"question": "average expenses by degree"}'
```

## How the assistant guarantees safety

The question text is never concatenated into SQL. This is the whole design:

1. **Vocabulary.** Measures, dimensions, aggregations, years and dimension values
   come from fixed allowlists derived from the stage 4 transformation.
2. **Single statement.** The builder emits exactly one `SELECT` against
   `v_student_budget_analysis`. A ranking question with a dimension is treated as a
   `count(*)` grouped by that dimension, so "top 5 UPI apps" is a query and not a
   second statement.
3. **Independent guard.** Every generated statement is re-parsed and checked by
   `SQLSafetyGuard`, which rejects writes, multiple statements, comments, set
   operations, CTEs, unapproved identifiers, unapproved literals, and excessive row
   limits. It does not trust the builder.
4. **Refusal.** A question outside the vocabulary raises `IntentNotUnderstood`
   instead of being guessed at. "What is the airspeed of a swallow" is refused.

So a hostile question is either refused or reduced to a safe query. It never
produces hostile SQL:

```
average expenses by degree; DROP TABLE students_budget
  -> SELECT "degree", avg("total_expenses") ... GROUP BY "degree"
```

When a database is available, the runner additionally opens a
`SET TRANSACTION READ ONLY` transaction, so the database enforces the same boundary
independently of the application.

## Reading the numbers honestly

The assistant attaches its caveats to every answer, because the source data cannot
support the conclusions a number invites. It covers 213 students across 10 measures
and 12 dimensions.

- **Every amount is a bucket midpoint**, not a reported figure. "Average expenses
  of 11,461" is an average of midpoints.
- **6250 is a floor.** It stands in for the open-ended "> 5000" option, so any total
  containing one is a lower bound.
- **13.4% of the 852 expense cells are right-censored** at that floor. Totals near
  the top of the distribution are understated by an unknown amount.
- **Allowance buckets are far coarser than expense buckets.** This is why every
  respondent appears to overspend, and it is an artefact of the instrument rather
  than a finding about students.
- **Utilisation has two defensible definitions** that disagree: the mean of
  per-student ratios is 1533.05%, the aggregate ratio is 726.02%. They answer
  different questions. The assistant labels which one it is reporting.
- **"Savings" is negative for every row**, a direct consequence of the two points
  above.
- **One data-quality warning is expected and is not a bug**: 112 respondents name a
  UPI app without selecting UPI as their payment method. The question is ungated,
  so filter on payment method rather than on the named app.

## Repository layout

```
.
├── Data/
│   ├── Student Budget Analytics Raw Data.xlsx   # raw survey export
│   └── processed/                               # stage 3 and 4 output
├── Notebooks/                                   # stage 3 and 4 exploration
├── ai/
│   └── assistant.py                             # stage 9: the assistant
├── sql/
│   ├── schema.sql                               # table DDL, constraints, indexes
│   └── analytics.sql                            # the analytical view
├── src/
│   ├── pipeline.py                              # stage runner and CLI
│   ├── data_loader.py                           # stage 6 loader
│   ├── cleaner.py                               # stage 3
│   ├── transformer.py                           # stage 4
│   ├── data_quality.py                          # stage 7 checks
│   └── database.py                              # stage 5 connection and DDL
├── requirements.txt
└── README.md
```

## Testing

The assistant's selftest is self-contained and needs no database:

```bash
python ai/assistant.py selftest
```

It covers the safety guard, SQL-injection attempts, the parser, the offline engine
against known ground truth, the dataset invariants the insights assert, follow-up
conversation handling, and command-line argument handling.

The pipeline's own checks run as part of any stage 3 to 7 invocation. The easiest
way to see them on their own is through the assistant, which explains each finding:

```bash
python ai/assistant.py quality
```

On the current dataset that reports 63 checks, 0 critical, 1 warning, 0 info, and a
`PASS`. To consume the report programmatically instead:

```python
import sys
sys.path.insert(0, "src")
import data_quality

report = data_quality.verify_file("Data/processed/transformed_student_budget.csv")
for result in report.results:
    if not result.passed:
        print(result.severity, result.name, "-", result.message)
```

## Troubleshooting

**`unrecognized arguments: --offline`**
The flag must be recognised by the subcommand. Current versions accept it on either
side of the subcommand; if you hit this, you are running an older copy.

**`could not connect to server` / `no pg_hba.conf entry`**
No database is required. Use `--offline` to answer from the processed CSV. If you do
want PostgreSQL, check the host, port and credentials in your DSN.

**`refusing to drop an existing table without --allow-drop`**
Stage 5 is destructive and is gated. Pass `--allow-drop` if you accept losing the
contents of `students_budget`, or use `--dry-run` to inspect what stage 5 would do
without touching a server. To have the assistant explain the message, feed it the
log:

```bash
python src/pipeline.py --stages 5 --allow-drop 2>&1 | tee pipeline.log
python ai/assistant.py failures pipeline.log
```

**`I could not find anything to measure or group by in that question`**
The question is outside the vocabulary. Run `python ai/assistant.py examples` to see
the supported shapes. The refusal is intentional: guessing at an unrecognised
question is how a confident assistant ends up confidently wrong.

**Quality report shows one WARNING**
The UPI cross-field warning described above is expected. Use `--quality off` to skip
the gate, or leave it as `report` to log it without blocking.
