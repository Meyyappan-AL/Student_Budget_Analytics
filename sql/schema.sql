-- =============================================================================
-- Stage 5 - PostgreSQL schema for the Student Budget Analytics pipeline
-- Target:   PostgreSQL 12+ (GENERATED columns require 12+)
-- Source:   data/processed/transformed_student_budget.csv  (213 rows)
-- =============================================================================
--
-- HOW THE COLUMNS MAP FROM THE PIPELINE CSV
-- -----------------------------------------
-- The Stage 4 CSV deliberately uses short Title Case headers for presentation
-- ("Food", "Budget Usage %", "Work/Internship"). Those are NOT valid unquoted
-- SQL identifiers, so the table uses conventional snake_case and the load step
-- has to map them:
--
--   CSV "Timestamp"        -> submitted_at          (renamed: "timestamp" is a
--                                                   type name in PostgreSQL)
--   CSV "Student Name"     -> student_name
--   CSV "Gender"           -> gender
--   CSV "Degree"           -> degree
--   CSV "Year of Study"    -> year_of_study
--   CSV "Accommodation"    -> accommodation
--   CSV "Background"       -> background
--   CSV "Work/Internship"  -> work_internship
--   CSV "Specialization"   -> specialization
--   CSV "Food"             -> food_expenses
--   CSV "Transport"        -> transport_expenses
--   CSV "Academic"         -> academic_expenses
--   CSV "Other"            -> other_expenses
--   (derived)              -> total_expenses       GENERATED
--   CSV "Allowance"        -> allowance
--   (derived)              -> savings              GENERATED
--   CSV "Budget Usage %"   -> budget_usage_pct
--   CSV "Preferred Payment"-> preferred_payment
--   CSV "UPI App"          -> upi_app
--   CSV "Track Digitally"  -> track_digitally
--   CSV "Budget Plan"      -> budget_plan
--   CSV "Study Focus"      -> study_focus
--
-- CONSTRAINTS THAT LOOK OBVIOUS BUT WOULD REJECT EVERY ROW
-- -------------------------------------------------------
-- The data was measured before this DDL was written. Three "sensible" checks
-- fail on 100% of rows and are therefore NOT applied as one might assume:
--
--   1. budget_usage_pct BETWEEN 0 AND 100  -> REJECTS ALL 213 ROWS.
--      Observed range is 125.0 .. 8600.0; *zero* rows are <= 100. The cause is
--      upstream: the survey's allowance buckets are far coarser and lower than
--      its expense buckets (allowance tops out at "> 3000" -> 4500, while a
--      single expense bucket "> 5000" maps to 6250), so every respondent
--      overspends. The column is a RATIO, not a share, and is not centred on
--      100%. The check below is therefore "non-negative, with a sane ceiling".
--
--   2. savings >= 0  -> REJECTS ALL 213 ROWS.
--      Observed range is -24000 .. -250. Savings is Allowance - Total Expenses
--      and is negative for all 213 respondents for the same reason as above.
--      Clamping it at zero would erase the only signal the column carries.
--
--   3. UNIQUE (student_name)  -> REJECTS THE LOAD.
--      "HARINE SHREE G" appears twice (213 responses, 212 distinct names). Two
--      different people can share a name, so student_name is indexed but NOT
--      unique. Only the surrogate student_id is unique.
--
-- MYSQL vs POSTGRESQL - THE DIFFERENCES THAT MATTER HERE
-- ------------------------------------------------------
-- SERIAL vs AUTO_INCREMENT
--   MySQL:  id INT AUTO_INCREMENT PRIMARY KEY
--   Postgres: id SERIAL PRIMARY KEY
--   SERIAL is shorthand that expands to a sequence plus a NOT NULL DEFAULT
--   nextval(...) column. Two consequences MySQL does not share: the sequence
--   lives independently of the table (so ids are not reused after a rollback of
--   the INSERT but CAN leave gaps, and TRUNCATE does not reset it), and adding
--   the column later means ALTER TABLE ... ADD COLUMN ... DEFAULT nextval(...).
--   Postgres 10+ prefers an identity column, which is standards-compliant,
--   keeps the counter attached to the table, and is what you would use on a new
--   project:
--       student_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY
--   SERIAL is used here because the brief specifies it. It behaves identically
--   for this workload.
--
-- VARCHAR vs TEXT
--   In Postgres these are related but not interchangeable:
--     VARCHAR(n) enforces a maximum LENGTH. Over-long values raise
--       "value too long for type character varying(n)"
--     TEXT accepts any length, has no limit, and - importantly - is just as
--       fast in Postgres (there is no per-row length prefix penalty like MySQL
--       has for its own TEXT/BLOB types).
--   In MySQL, VARCHAR(n) stores n bytes inline and switches to an overflow
--   pointer beyond that; TEXT is stored off-page with a 64KB cap, and TEXT
--   cannot have a default value or be indexed without a prefix length.
--   Postgres can index a whole TEXT column with no prefix. So the Postgres rule
--   of thumb is: use VARCHAR(n) where n is a genuine business rule, TEXT where
--   it is not. Every length below is real headroom, not a guess.
--
-- NUMERIC vs DECIMAL
--   They are the SAME type - DECIMAL is a synonym for NUMERIC in both engines -
--   so this is not a portability trap. What matters is precision, which is
--   exact/decimal in both (neither uses binary floating point):
--       NUMERIC(12,2)  = up to 10 integer digits, 2 decimal places
--   Use NUMERIC for money, never FLOAT/DOUBLE, because 0.1 is not
--   representable in binary floating point and money columns must total
--   exactly. Note Postgres also has a distinct MONEY type; avoid it - it is
--   locale-dependent, has no true arithmetic support and is widely considered a
--   mistake. MySQL's DECIMAL is capped at 65 digits, Postgres at 131072.
--
-- Other differences worth knowing for this pipeline
--   TIMESTAMPTZ vs DATETIME: the survey export carries naive local timestamps
--   (no offset). Postgres TIMESTAMPTZ normalises to UTC on write and converts
--   on read, so it is the correct choice; the loader must supply an offset
--   (Asia/Kolkata) or the values will be interpreted as UTC. MySQL DATETIME
--   has no time zone at all and TIMESTAMP silently converts using server
--   settings.
--   DDL is transactional in Postgres: the BEGIN/COMMIT below is real, so a
--   failed CREATE rolls back cleanly. MySQL performs an implicit commit on
--   most DDL, so a failed statement leaves the schema half-applied.
--   Identifiers are case-folded to lower case unless double-quoted. So
--   "Budget Usage %" would need quoting everywhere - another reason the table
--   uses snake_case. MySQL's behaviour depends on lower_case_table_names.
--   BOOLEAN: Postgres has a true boolean; MySQL's BOOL is TINYINT(1). The
--   Yes/No columns are kept as VARCHAR(3) here so the survey's literal values
--   survive the round trip, with a CHECK to police the domain.
--   ENUM: Postgres ENUM is ordered and enforced but ALTERing it is awkward and
--   values cannot be removed easily. For small closed sets a VARCHAR plus a
--   CHECK constraint is used instead: same enforcement, trivial to extend.
-- =============================================================================

BEGIN;

DROP TABLE IF EXISTS students_budget;

CREATE TABLE students_budget (
    -- ---------------------------------------------------------------- identity
    -- SERIAL: see the SERIAL vs AUTO_INCREMENT note above.
    student_id           SERIAL                      PRIMARY KEY,

    -- Source timestamps are naive local time (Feb-Mar 2026 survey window).
    -- TIMESTAMPTZ stores UTC and renders in the session time zone.
    submitted_at         TIMESTAMPTZ                 NOT NULL,

    -- ------------------------------------------------------------- dimensions
    -- Lengths carry real headroom over the observed maxima
    -- (student_name 33, specialization 35, study_focus 25).
    student_name         VARCHAR(100)                NOT NULL,
    gender               VARCHAR(20)                 NOT NULL,
    degree               VARCHAR(30)                 NOT NULL,
    year_of_study        VARCHAR(20)                 NOT NULL,
    accommodation        VARCHAR(30)                 NOT NULL,
    background           VARCHAR(20)                 NOT NULL,
    work_internship      VARCHAR(3)                  NOT NULL,
    -- specialization is free-text from the survey: 44 distinct spellings
    -- (including "BACHELOR OF BUSINESS ADMINISTRATION"), so it is deliberately
    -- unconstrained rather than pinned to a course list.
    specialization       VARCHAR(100)                NOT NULL,
    preferred_payment    VARCHAR(30)                 NOT NULL,
    upi_app              VARCHAR(30)                 NOT NULL,
    track_digitally      VARCHAR(3)                  NOT NULL,
    budget_plan          VARCHAR(3)                  NOT NULL,
    study_focus          VARCHAR(100)                NOT NULL,

    -- ------------------------------------------------------------------ money
    -- NUMERIC(12,2): exact decimal, never floating point. 12 digits is far
    -- beyond the observed maximum total of 25000.
    food_expenses        NUMERIC(12,2)               NOT NULL,
    transport_expenses   NUMERIC(12,2)               NOT NULL,
    academic_expenses    NUMERIC(12,2)               NOT NULL,
    other_expenses       NUMERIC(12,2)               NOT NULL,
    allowance            NUMERIC(12,2)               NOT NULL,

    -- --------------------------------------------------------------- measures
    -- budget_usage_pct is a ratio, NOT a percentage share: it is unbounded
    -- above 100 by design (observed 125.0 .. 8600.0). NUMERIC(7,2) covers
    -- 0 .. 99999.99. See warning 1 in the header before "fixing" this to 100.
    budget_usage_pct     NUMERIC(7,2)                NOT NULL,

    -- -------------------------------------------------------------- derived
    -- GENERATED ALWAYS AS ... STORED makes the database the single source of
    -- truth for these two, so they can never drift from their components. This
    -- also means the loader must NOT supply them (see the COPY note below).
    -- budget_usage_pct is intentionally NOT generated: it depends on the
    -- open-ended midpoint assumption ("> 5000" -> 6250) that the pipeline owns,
    -- and baking the rounding policy into the DDL would hide future changes.
    total_expenses       NUMERIC(12,2)
                             GENERATED ALWAYS AS
                             (food_expenses + transport_expenses
                              + academic_expenses + other_expenses) STORED,
    savings              NUMERIC(12,2)
                             GENERATED ALWAYS AS
                             (allowance - (food_expenses + transport_expenses
                              + academic_expenses + other_expenses)) STORED,

    -- ============================================================== CONSTRAINTS
    -- Closed categorical domains, enforced as CHECKs rather than ENUM so the
    -- value lists stay easy to extend (see the ENUM note in the header).
    CONSTRAINT ck_students_budget_gender
        CHECK (gender IN ('Male', 'Female')),
    CONSTRAINT ck_students_budget_degree
        CHECK (degree IN ('B.Com', 'BE / B.Tech', 'BSc', 'MBA', 'ME / M.Tech', 'Other')),
    CONSTRAINT ck_students_budget_year_of_study
        CHECK (year_of_study IN ('1st Year', '2nd Year', '3rd Year', '4th Year')),
    CONSTRAINT ck_students_budget_accommodation
        CHECK (accommodation IN ('Day Scholar', 'Hostel', 'PG')),
    CONSTRAINT ck_students_budget_background
        CHECK (background IN ('Rural', 'Urban')),
    CONSTRAINT ck_students_budget_work_internship
        CHECK (work_internship IN ('Yes', 'No')),
    CONSTRAINT ck_students_budget_track_digitally
        CHECK (track_digitally IN ('Yes', 'No')),
    CONSTRAINT ck_students_budget_budget_plan
        CHECK (budget_plan IN ('Yes', 'No')),
    CONSTRAINT ck_students_budget_preferred_payment
        CHECK (preferred_payment IN ('UPI', 'Cash', 'Debit/Credit Card', 'Other')),
    -- "Not Applicable" is a real value: UPI is only asked when the preferred
    -- payment method is UPI, so a blank is meaningful and is filled with this
    -- sentinel at Stage 3. Do not treat it as missing data.
    CONSTRAINT ck_students_budget_upi_app
        CHECK (upi_app IN ('GPay', 'PhonePe', 'SBI Pay', 'Amazon Pay',
                           'BHIM UPI', 'Airtel Thanks App', 'Other', 'Not Applicable')),

    -- Money cannot be negative. This is safe for every spending column: the
    -- minimum observed value is 250.
    CONSTRAINT ck_students_budget_food_non_negative
        CHECK (food_expenses >= 0),
    CONSTRAINT ck_students_budget_transport_non_negative
        CHECK (transport_expenses >= 0),
    CONSTRAINT ck_students_budget_academic_non_negative
        CHECK (academic_expenses >= 0),
    CONSTRAINT ck_students_budget_other_non_negative
        CHECK (other_expenses >= 0),
    CONSTRAINT ck_students_budget_allowance_non_negative
        CHECK (allowance >= 0),

    -- Budget usage: non-negative, with a ceiling derived from the data rather
    -- than guessed. The survey's own bucket scheme makes 10000 the arithmetic
    -- maximum: the largest expense midpoint ("> 5000" -> 6250) across four
    -- categories is 25000, divided by the smallest allowance midpoint (250),
    -- gives exactly 10000%. Observed maximum is 8600. If the bucket scheme
    -- changes, relax this constant rather than deleting the check.
    CONSTRAINT ck_students_budget_usage_range
        CHECK (budget_usage_pct >= 0 AND budget_usage_pct <= 10000),

    -- Student names are intentionally NOT unique - see warning 3 in the header.
    CONSTRAINT ck_students_budget_name_not_blank
        CHECK (length(btrim(student_name)) > 0)
);

-- =============================================================================
-- INDEXES
-- =============================================================================
-- Requested: degree, year_of_study, preferred_payment.
--
-- Selectivity caveat, stated honestly: all three hold only 4-6 distinct values,
-- so a B-tree on any of them alone lets the planner discard well under 1% of a
-- 213-row table. They earn their place as filter/composite components and for
-- index-only group-by scans, not as selective lookups. The composite below is
-- the index that actually earns its write cost for the obvious dashboard
-- question "spend by degree and year of study".
CREATE INDEX ix_students_budget_degree
    ON students_budget (degree);
CREATE INDEX ix_students_budget_year_of_study
    ON students_budget (year_of_study);
CREATE INDEX ix_students_budget_preferred_payment
    ON students_budget (preferred_payment);

-- Natural dashboard slice: spend by course stage.
CREATE INDEX ix_students_budget_degree_year
    ON students_budget (degree, year_of_study);

-- Highest-cardinality dimension, so the most selective of the categorical
-- indexes - useful for per-course breakdowns.
CREATE INDEX ix_students_budget_specialization
    ON students_budget (specialization);

-- Name lookup by support staff. Non-unique on purpose (HARINE SHREE G).
CREATE INDEX ix_students_budget_student_name
    ON students_budget (student_name);

-- "Who is overspending most?" - a descending index lets the planner satisfy the
-- top-N scan from the index alone.
CREATE INDEX ix_students_budget_usage_desc
    ON students_budget (budget_usage_pct DESC);

-- =============================================================================
-- DOCUMENTATION
-- =============================================================================
-- COMMENT ON is Postgres-specific (MySQL has no equivalent DDL comment).
COMMENT ON TABLE  students_budget IS
    'Stage 5 normalised student budget survey responses. Money columns are '
    'midpoints of ordinal survey buckets (e.g. ''1000 - 2000'' -> 1500), not '
    'exact reported amounts; do not present them as precise figures.';
COMMENT ON COLUMN students_budget.budget_usage_pct IS
    'Ratio (Total Expenses / Allowance) * 100. NOT a share: values above 100 '
    'are expected and observed (125.0 to 8600.0 in the seed data).';
COMMENT ON COLUMN students_budget.savings IS
    'Allowance minus Total Expenses. Negative for every seed row; deliberately '
    'unclamped.';
COMMENT ON COLUMN students_budget.specialization IS
    'Free-text course answer, 44 distinct spellings in the seed data. Not yet '
    'canonicalised - no CHECK constraint is applied.';
COMMENT ON COLUMN students_budget.upi_app IS
    '''Not Applicable'' means the UPI question did not apply (payment is not by '
    'UPI). It is a meaningful value, not missing data.';

COMMIT;

-- =============================================================================
-- LOADING THE DATA
-- =============================================================================
-- The executable version of everything below is src/data_loader.py, which
-- generates the staging DDL from the CSV header list so the two cannot drift,
-- and is what Stage 6 actually runs:
--
--     python src/pipeline.py --stages 6 --allow-drop
--     python src/data_loader.py --dry-run      # validate + print the SQL, no server
--
-- The example that follows documents the approach for readers working directly
-- in psql. Note it stages under the *lowercased* headers for brevity; the
-- loader instead uses the CSV headers verbatim ("Year of Study"), which is
-- equally valid and avoids a rename step.
--
-- total_expenses and savings are GENERATED columns, so the loader must omit
-- them; PostgreSQL will not accept an explicit value for a GENERATED ALWAYS
-- column. A staged table plus INSERT ... SELECT keeps the column mapping
-- explicit and lets the CSV header names stay in Title Case:
--
--   CREATE TEMP TABLE stage_5_raw (
--       timestamp         TEXT,
--       student_name      TEXT,
--       gender            TEXT,
--       degree            TEXT,
--       "year of study"   TEXT,
--       accommodation     TEXT,
--       background        TEXT,
--       "work/internship" TEXT,
--       specialization    TEXT,
--       food              NUMERIC,
--       transport         NUMERIC,
--       academic          NUMERIC,
--       other             NUMERIC,
--       "total expenses"  NUMERIC,
--       allowance         NUMERIC,
--       savings           NUMERIC,
--       "budget usage %"  NUMERIC,
--       "preferred payment" TEXT,
--       "upi app"         TEXT,
--       "track digitally" TEXT,
--       "budget plan"     TEXT,
--       "study focus"     TEXT
--   );
--
--   \copy stage_5_raw FROM 'data/processed/transformed_student_budget.csv' WITH (FORMAT csv, HEADER true)
--
--   -- The survey timestamps are naive local time. Pin the offset explicitly;
--   -- without it PostgreSQL reads them as UTC and every row shifts by 5:30.
--   INSERT INTO students_budget (
--       submitted_at, student_name, gender, degree, year_of_study,
--       accommodation, background, work_internship, specialization,
--       food_expenses, transport_expenses, academic_expenses, other_expenses,
--       allowance, budget_usage_pct, preferred_payment, upi_app,
--       track_digitally, budget_plan, study_focus)
--   SELECT
--       ("timestamp"::timestamp AT TIME ZONE 'Asia/Kolkata'), student_name, gender,
--       degree, "year of study", accommodation, background, "work/internship",
--       specialization, food, transport, academic, other, allowance,
--       "budget usage %", "preferred payment", "upi app", "track digitally",
--       "budget plan", "study focus"
--   FROM stage_5_raw;
--
-- Verification after load:
--   SELECT count(*) FROM students_budget;                      -- expect 213
--   SELECT min(budget_usage_pct), max(budget_usage_pct);      -- expect 125.0, 8600.0
--   SELECT min(savings), max(savings);                         -- expect -24000, -250
--   SELECT * FROM students_budget WHERE savings >= 0;         -- expect 0 rows
