-- =============================================================================
-- Stage 8 - Analytical queries for the Student Budget Analytics pipeline
-- Target:  PostgreSQL 12+ (uses FILTER, LATERAL, percentile_disc, window fns)
-- Input:  table students_budget, created by sql/schema.sql and loaded by
--         src/data_loader.py (213 rows in the seed dataset)
-- =============================================================================
--
-- HOW TO RUN
-- ----------
-- Requires sql/schema.sql to have been applied and the Stage 6 load to have run:
--
--     python src/pipeline.py --stages 5,6 --allow-drop
--     psql "$DATABASE_URL" -f sql/analytics.sql
--
-- The script is READ-ONLY with respect to students_budget: it creates one
-- (idempotent) view and then only SELECTs. Nothing is inserted, updated or
-- deleted, so it is safe to run against a loaded database, and safe to re-run.
-- Each query is independently executable - select any single block in psql and it
-- returns rows without needing the ones above it, because every query derives its
-- own inputs from the view rather than from a chain of temp tables.
--
-- ---------------------------------------------------------------------
-- FOUR THINGS TO KNOW BEFORE QUOTING ANY NUMBER FROM THIS FILE
-- ---------------------------------------------------------------------
-- The dataset is a fixed-choice survey export. Three of the four points below are
-- properties of the *instrument* that constrain what can honestly be concluded;
-- the fourth is a property of the study design. All of them are quantified by
-- queries in this file rather than merely asserted here.
--
-- 1. THE MONEY COLUMNS ARE BUCKET MIDPOINTS, AND THEY ARE RIGHT-CENSORED.
--    A respondent who answered "> 5000" was recorded as 6250. The true figure is
--    unknown and higher, so every total, average and ranking here is a LOWER BOUND.
--    13.4% of the 852 expense cells sit at that censored ceiling (Q19), and each of
--    the four categories tops out at exactly 6250 (Q14). The categories take only
--    6 or 7 distinct values each across the whole sample, which is why Q15 and Q16
--    find so many ties for "top" category.
--
-- 2. THE ALLOWANCE SCALE IS COARSER AND FAR LOWER THAN THE EXPENSE SCALE, SO
--    "OVERSPENDING" IS MOSTLY AN ARTIFACT.
--    Mean total expenses are 11,461.27 against a mean allowance of 1,578.64 (Q1) -
--    a gap of about 7x. That is a property of how the questionnaire was written,
--    not evidence that students are 7x over budget: the survey offers expense
--    buckets topping out at "> 5000" but allowance buckets topping out around
--    "> 3000" (midpoint 4500). All 213 respondents therefore "overspend", savings
--    is negative for every row, and budget_usage_pct never drops below 125 (Q18).
--    Consequence: this file reports a DEFICIT, and never ranks students by
--    "savings" - the set of savers is empty, so that ranking is vacuous.
--
-- 3. THE MEAN OF THE PER-STUDENT RATIOS AND THE AGGREGATE RATIO DIFFER BY MORE
--    THAN A FACTOR OF TWO. THIS IS THE MOST IMPORTANT QUANTITATIVE POINT HERE.
--        mean of budget_usage_pct          = 1533.05%   (Q1)
--        sum(total) / sum(allowance) * 100 =  726.02%   (Q1)
--    Both are "correct"; they answer different questions. The mean of ratios gives
--    every student equal weight and is dominated by the small number of students
--    whose allowance landed in the lowest bucket - one of them reports 8600%. The
--    aggregate ratio is weighted by rupee and answers "how far over budget is this
--    population in total". Every grouped query below reports BOTH, because a
--    comparison that flips sign depending on the choice is not a finding. (For the
--    accommodation split in Q10 the conclusion is fortunate: it holds either way.)
--
-- 4. NOTHING HERE IS CAUSAL.
--    This is an observational, cross-sectional survey with no control for
--    programme, year, gender, branch or income. "4th year students spend more" is a
--    description of these 213 responses, not a statement about what causes it.
--
-- ---------------------------------------------------------------------
-- CONVENTIONS USED THROUGHOUT
-- ---------------------------------------------------------------------
-- * Percentages of a whole are computed with a window function
--   (100.0 * <n> / sum(<n>) OVER ()) rather than a correlated subquery, so there
--   is one pass over the data and no self-join.
-- * Aggregates that can be skewed report a median next to the mean. The
--   distribution is right-skewed by the censored 6250 ceiling, and on several
--   splits the mean and median tell noticeably different stories.
-- * Medians use percentile_disc, not percentile_cont. The amounts come from a
--   coarse discrete set of bucket midpoints, so an interpolated median can report
--   a figure that no respondent actually gave.
-- * "Year of study" is ordered by an explicit ordinal, never by its text value.
--   '1st Year' < '2nd Year' < ... happens to hold under the default collation here
--   because all four labels start with a single digit, but that is a coincidence
--   of this data and would break the moment a '10th Year' or a differently
--   spelled label appeared. The ordinal lives in the view so it cannot be
--   forgotten in one query and remembered in another.
-- * Empty group cells are reported as NULL, never as 0. The degree-by-year matrix
--   in Q5 has genuinely empty cells (no BSc 4th years, no MBA 3rd years), and
--   COALESCE-ing those to zero would assert that such a cohort exists and spends
--   nothing.
-- =============================================================================


-- =============================================================================
-- THE VIEW: one definition of every derived column, shared by all 20 queries
-- =============================================================================
-- Four derived facts are needed throughout, and restating them in each query is
-- how they drift apart. Defining them once here means a change to the
-- overspend definition is a one-line edit that all 20 queries pick up.
CREATE OR REPLACE VIEW v_student_budget_analysis AS
SELECT
    -- identity and dimensions, passed through unchanged
    s.student_id,
    s.submitted_at,
    s.student_name,
    s.gender,
    s.degree,
    s.year_of_study,
    s.accommodation,
    s.background,
    s.work_internship,
    -- Free text, 44 distinct spellings in the seed data. Exposed so Q20 can
    -- report that cardinality; deliberately not used to group anything, because
    -- 44 near-synonyms of a handful of courses would fragment every slice.
    s.specialization,
    s.preferred_payment,
    s.upi_app,
    s.track_digitally,
    s.budget_plan,
    s.study_focus,

    -- Natural sort order for the year dimension. NULL for any label outside the
    -- CHECK domain, which the schema makes impossible but the view tolerates so
    -- that adding a year value later cannot silently break 20 queries.
    CASE s.year_of_study
        WHEN '1st Year' THEN 1
        WHEN '2nd Year' THEN 2
        WHEN '3rd Year' THEN 3
        WHEN '4th Year' THEN 4
    END AS year_of_study_ordinal,

    -- money, as loaded (total_expenses and savings are GENERATED in the schema,
    -- so they can never disagree with their components)
    s.food_expenses,
    s.transport_expenses,
    s.academic_expenses,
    s.other_expenses,
    s.total_expenses,
    s.allowance,
    s.savings,
    s.budget_usage_pct,

    -- The deficit, stated positively. savings is the same number with the sign
    -- flipped; it is negative for all 213 rows, so reading "savings" aloud as a
    -- ranking produces the perverse result that the least-overspending student is
    -- the "worst saver". This column is the one to group, sort and average.
    s.total_expenses - s.allowance AS overspend_amount,
    s.total_expenses >  s.allowance AS is_overspending,

    -- The UPI relationship, made explicit because the survey did not gate the
    -- question (see Q8). 112 of 213 respondents name a UPI app while paying by
    -- some other method, so "who uses GPay" and "who pays by UPI" are separate
    -- populations and must not be conflated.
    s.preferred_payment = 'UPI'       AS pays_by_upi,
    s.upi_app <> 'Not Applicable'     AS names_a_upi_app
FROM students_budget AS s;


-- =============================================================================
-- SECTION 0 - THE DENOMINATOR
-- =============================================================================
-- Every other section divides by something. This query establishes what those
-- denominators are, so a later result can be sanity-checked against them.

-- Q1. Population baseline: scale, and the two competing utilisation measures.
-- Seed data: 213 rows, 2,441,250 total spend against 336,250 total allowance.
-- mean-of-ratios 1533.05% vs aggregate 726.02% - see the header, point 3.
SELECT
    count(*)                                                            AS respondents,
    sum(total_expenses)                                                 AS total_spend,
    round(avg(total_expenses), 2)                                       AS avg_total_expenses,
    percentile_disc(0.5) WITHIN GROUP (ORDER BY total_expenses)         AS median_total_expenses,
    min(total_expenses)                                                 AS min_total_expenses,
    max(total_expenses)                                                 AS max_total_expenses,
    round(avg(allowance), 2)                                            AS avg_allowance,
    sum(allowance)                                                      AS total_allowance,

    -- Weighted by rupee: "as a population, how far over budget?"
    round(100.0 * sum(total_expenses) / nullif(sum(allowance), 0), 2)   AS aggregate_utilisation_pct,

    -- Unweighted: "for the typical respondent, how far over budget?" The gap
    -- between these two is the single most important number in this file.
    round(avg(budget_usage_pct), 2)                                     AS mean_of_ratios_pct,
    percentile_disc(0.5) WITHIN GROUP (ORDER BY budget_usage_pct)       AS median_of_ratios_pct,

    -- Magnitude of the shortfall, positive.
    round(avg(overspend_amount), 2)                                     AS avg_overspend,
    min(overspend_amount)                                               AS min_overspend,
    max(overspend_amount)                                               AS max_overspend,
    count(*) FILTER (WHERE is_overspending)                             AS overspending_students,
    count(*) FILTER (WHERE NOT is_overspending)                         AS saving_students
FROM v_student_budget_analysis;
-- Expected: respondents 213, saving_students 0, overspending_students 213,
--           min_overspend 250, max_overspend 24000.


-- =============================================================================
-- SECTION 1 - AVERAGE EXPENSES BY DEGREE AND YEAR OF STUDY
-- =============================================================================

-- Q2. Average expenses by degree, most expensive first.
-- Seed data: MBA 12,819.44 > ME/M.Tech 12,567.07 > BE/B.Tech 12,121.71 >
--            Other 10,365.38 > B.Com 9,892.05 > BSc 9,714.29
-- Read the median alongside the mean, because the two rank the degrees
-- differently: ME / M.Tech is second on the mean (12,567.07) but third on the
-- median (12,000.00), behind BE / B.Tech (12,750.00). BE / B.Tech has by far the
-- most censored cells of any degree - 20.7% of its expense cells sit at the 6250
-- floor (Q14) - and because that floor is a *lower* bound on the truth, the
-- censoring biases its mean down toward its median. So the degree that looks
-- third on the mean is second on what respondents actually reported.
-- n is included because "Other" is 13 students - thin enough that its position
-- in this table is not meaningful.
SELECT
    degree,
    count(*)                                                          AS students,
    round(100.0 * count(*) / sum(count(*)) OVER (), 1)                 AS share_of_students_pct,
    sum(total_expenses)                                               AS total_spend,
    round(avg(total_expenses), 2)                                     AS avg_total_expenses,
    percentile_disc(0.5) WITHIN GROUP (ORDER BY total_expenses)       AS median_total_expenses,
    round(avg(allowance), 2)                                          AS avg_allowance,
    round(avg(budget_usage_pct), 2)                                   AS mean_of_ratios_pct,
    round(100.0 * sum(total_expenses) / nullif(sum(allowance), 0), 2) AS aggregate_utilisation_pct,
    round(avg(overspend_amount), 2)                                   AS avg_overspend
FROM v_student_budget_analysis
GROUP BY degree
ORDER BY avg_total_expenses DESC, degree;
-- Note: the two utilisation columns rank the degrees DIFFERENTLY, which is the
-- reason both are here. B.Sc is last of six on the mean-of-ratios (937.43) but
-- third on the aggregate (709.57); BE / B.Tech moves from third to fifth. Both
-- are correct answers to different questions, and the disagreement is a size
-- effect - B.Sc's 21 respondents include several whose allowance fell in the
-- lowest bucket, which the unweighted mean rewards and the aggregate ignores.
-- A ranking that depends on the aggregation choice is not a finding.


-- Q3. Average expenses by year of study, in course order.
-- Seed data: 1st 10,732.14 (n=84), 2nd 12,296.30 (n=81),
--            3rd 10,000.00 (n=30), 4th 13,541.67 (n=18)
-- The 3rd-year row is the one to study, because its spend and its utilisation
-- point in opposite directions: it has the LOWEST average spend of the four
-- years (10,000.00) yet the second-highest aggregate utilisation (810.81%), purely
-- because it also has the lowest average allowance (1,233.33). The 4th year is
-- highest on both (13,541.67 and 894.50%) but n=18 is too small for that mean to
-- carry much weight. The apparent "3rd year dip" in spending is not a finding -
-- n=30 against n=84, over four discrete bucket values.
SELECT
    year_of_study_ordinal                       AS year_ordinal,
    year_of_study,
    count(*)                                     AS students,
    round(100.0 * count(*) / sum(count(*)) OVER (), 1) AS share_of_students_pct,
    round(avg(total_expenses), 2)                AS avg_total_expenses,
    percentile_disc(0.5) WITHIN GROUP (ORDER BY total_expenses) AS median_total_expenses,
    round(avg(allowance), 2)                     AS avg_allowance,
    round(avg(budget_usage_pct), 2)              AS mean_of_ratios_pct,
    round(100.0 * sum(total_expenses) / nullif(sum(allowance), 0), 2) AS aggregate_utilisation_pct
FROM v_student_budget_analysis
GROUP BY year_of_study_ordinal, year_of_study
ORDER BY year_of_study_ordinal;


-- Q4. The degree-by-year cross-tabulation in long form, with cell sizes.
-- This is the honest shape of Q5: every cell carries the number of students
-- behind it, so a single-student cell cannot be mistaken for a cohort.
-- Of the 24 cells, 15 hold 6 or more students, 6 hold between 1 and 5, and 3 are
-- empty: B.Sc has no 4th years, MBA no 3rd years, ME/M.Tech no 4th years. The
-- emptiest populated cells are B.Com/4th, Other/1st and ME/M.Tech/3rd, each a
-- single student. The cell_too_small_to_interpret flag below marks exactly those
-- six thin cells.
SELECT
    degree,
    year_of_study_ordinal                       AS year_ordinal,
    year_of_study,
    count(*)                                     AS students,
    round(avg(total_expenses), 2)                AS avg_total_expenses,
    percentile_disc(0.5) WITHIN GROUP (ORDER BY total_expenses) AS median_total_expenses,
    round(avg(allowance), 2)                     AS avg_allowance,
    -- Flag the cells too small to support a conclusion, rather than letting the
    -- reader work it out from n.
    (count(*) < 5)                               AS cell_too_small_to_interpret
FROM v_student_budget_analysis
GROUP BY degree, year_of_study_ordinal, year_of_study
ORDER BY degree, year_of_study_ordinal;


-- Q5. The same cross-tabulation pivoted for presentation, via conditional
-- aggregation. This is the standard way to pivot in pure SQL - crosstab() needs
-- the table to be materialised in the client first.
-- The empty cells are left as NULL on purpose. B.Sc has no 4th years, MBA no
-- 3rd years and ME/M.Tech no 4th years, so those cells are "no such cohort",
-- which is not the same claim as "a cohort that spends nothing".
SELECT
    degree,
    count(*) AS students,
    round(avg(total_expenses) FILTER (WHERE year_of_study_ordinal = 1), 2) AS yr1_avg_expenses,
    round(avg(total_expenses) FILTER (WHERE year_of_study_ordinal = 2), 2) AS yr2_avg_expenses,
    round(avg(total_expenses) FILTER (WHERE year_of_study_ordinal = 3), 2) AS yr3_avg_expenses,
    round(avg(total_expenses) FILTER (WHERE year_of_study_ordinal = 4), 2) AS yr4_avg_expenses,
    count(*) FILTER (WHERE year_of_study_ordinal = 1) AS yr1_students,
    count(*) FILTER (WHERE year_of_study_ordinal = 2) AS yr2_students,
    count(*) FILTER (WHERE year_of_study_ordinal = 3) AS yr3_students,
    count(*) FILTER (WHERE year_of_study_ordinal = 4) AS yr4_students
FROM v_student_budget_analysis
GROUP BY degree
ORDER BY students DESC, degree;
-- The four *_students columns are the guard rail on this table: a 0 means the
-- adjacent average is NULL because there is nobody in that cohort, not because
-- they spend nothing.


-- =============================================================================
-- SECTION 2 - PAYMENT METHOD DISTRIBUTION AND TOP UPI APPS
-- =============================================================================

-- Q6. How students pay, with the spend attached to each method.
-- Seed data: UPI 82 (38.5%), Cash 58 (27.2%), Debit/Credit Card 40 (18.8%),
--            Other 33 (15.5%)
-- "Other" is the highest-spending method on both measures (12,810.61 mean,
-- 858.38% aggregate) but is a residual bucket the survey never defined, so the
-- ranking inside it is unknowable. Debit/Credit Card shows the reverse: above
-- average spend (12,400.00) yet the LOWEST aggregate utilisation (686.51%),
-- because those 40 students also report the HIGHEST allowance average of any
-- method (1,806.25) - more than every other group except by a wide margin.
SELECT
    preferred_payment,
    count(*)                                                          AS students,
    round(100.0 * count(*) / sum(count(*)) OVER (), 1)                 AS share_of_students_pct,
    round(100.0 * sum(total_expenses)
              / nullif(sum(sum(total_expenses)) OVER (), 0), 1)        AS share_of_total_spend_pct,
    round(avg(total_expenses), 2)                                     AS avg_total_expenses,
    percentile_disc(0.5) WITHIN GROUP (ORDER BY total_expenses)       AS median_total_expenses,
    round(avg(budget_usage_pct), 2)                                   AS mean_of_ratios_pct,
    round(100.0 * sum(total_expenses) / nullif(sum(allowance), 0), 2) AS aggregate_utilisation_pct,
    -- Cash is the only method with no digital trail at all, so the spend split
    -- between tracked and untracked spenders is only meaningful here.
    count(*) FILTER (WHERE track_digitally = 'Yes')                    AS track_digitally_yes,
    count(*) FILTER (WHERE track_digitally = 'No')                     AS track_digitally_no
FROM v_student_budget_analysis
GROUP BY preferred_payment
ORDER BY students DESC, preferred_payment;


-- Q7. TOP UPI APPS - among students who actually pay by UPI.
-- Seed data: GPay 35 (43.2%), PhonePe 25 (30.9%), Other 6, Amazon Pay 5,
--            SBI Pay 5, Airtel Thanks App 3, BHIM UPI 2  (denominator 81)
-- The filter matters. Google Pay and PhonePe together hold 74.1% of the UPI
-- market in this sample. Spreading all 193 app-naming respondents across the
-- same ranking instead - the obvious reading of the raw column - reports 28.0%
-- and 23.3%, and 51.3% for the pair, understating the concentration by 22.8
-- percentage points. Q8 shows what the unfiltered version hides, Q9 contrasts it
-- directly.
--
-- 'Not Applicable' is excluded twice over: it is not an app, and no UPI payer
-- should carry it. One UPI payer does (Q8), and that row is excluded here rather
-- than being counted as a seventh app. That is why the denominator is 81 and not
-- the 82 students in Q8 who pay by UPI - the difference is exactly that one row.
--
-- The denominator is written out as a named CTE rather than a window FILTER so
-- that "81" is visible in the query, and the two shares below it are visibly
-- different populations: 81 students and 915,250 of spend.
WITH upi_payers AS (
    SELECT *
    FROM v_student_budget_analysis
    WHERE pays_by_upi
      AND names_a_upi_app
)
SELECT
    upi_app,
    count(*)                                                          AS students,
    -- Share of UPI payers, not of all respondents: the denominator for "which
    -- app should we optimise for" is the population that can actually use it.
    round(100.0 * count(*) / (SELECT count(*) FROM upi_payers), 1)      AS share_of_upi_payers_pct,
    round(avg(total_expenses), 2)                                     AS avg_total_expenses,
    round(100.0 * sum(total_expenses)
              / (SELECT sum(total_expenses) FROM upi_payers), 1)       AS share_of_upi_spend_pct
FROM upi_payers
GROUP BY upi_app
ORDER BY students DESC, upi_app;
-- 'Other' is a legitimate answer to "which app" but is not an app; it is kept
-- here so the shares still sum to 100% and the residual stays visible instead
-- of being silently redistributed across the named apps.


-- Q8. Data-integrity check on the UPI pair, because Q7 depends on it.
-- Expected in the seed data:
--   upi_payers                        82
--   upi_payers_naming_an_app          81   <- 1 payer says 'Not Applicable'
--   upi_payers_with_no_app             1
--   non_upi_payers_naming_an_app     112   <- names an app but does not pay by UPI
--   total_naming_an_app              193
-- The 112 is not a data error. The questionnaire asked every respondent which UPI
-- app they use without first asking whether they pay by UPI, so the two answers
-- are independent by construction. It does mean "UPI app" cannot be read as a
-- proxy for "pays digitally", and Q7 is scoped accordingly.
SELECT
    count(*)                                                          AS respondents,
    count(*) FILTER (WHERE pays_by_upi)                               AS upi_payers,
    count(*) FILTER (WHERE pays_by_upi AND names_a_upi_app)           AS upi_payers_naming_an_app,
    count(*) FILTER (WHERE pays_by_upi AND NOT names_a_upi_app)       AS upi_payers_with_no_app,
    count(*) FILTER (WHERE NOT pays_by_upi AND names_a_upi_app)       AS non_upi_payers_naming_an_app,
    count(*) FILTER (WHERE names_a_upi_app)                           AS total_naming_an_app
FROM v_student_budget_analysis;


-- Q9. App mentions across ALL respondents, for contrast with Q7.
-- Seed data: GPay 54, PhonePe 45, SBI Pay 27, Other 23, Not Applicable 20,
--            Amazon Pay 18, Airtel Thanks App 16, BHIM UPI 10  (denominator 213)
-- GPay and PhonePe lead both this table and Q7, but the order below them is
-- different, which is the point of showing both. SBI Pay is third here (27) and
-- only fifth among UPI payers (5), because 22 of its 27 respondents do not pay by
-- UPI. 'Not Applicable' appears only here: it is a category of the raw question,
-- not a state Q7 can report on. Read this as "brand reach", never as "payment
-- share".
SELECT
    upi_app,
    count(*)                                                          AS respondents,
    round(100.0 * count(*) / sum(count(*)) OVER (), 1)                 AS share_of_all_respondents_pct,
    count(*) FILTER (WHERE pays_by_upi)                               AS of_which_pay_by_upi,
    round(100.0 * count(*) FILTER (WHERE pays_by_upi)
              / nullif(count(*), 0), 1)                               AS pct_of_mentions_that_pay_by_upi
FROM v_student_budget_analysis
GROUP BY upi_app
ORDER BY respondents DESC, upi_app;


-- =============================================================================
-- SECTION 3 - BUDGET UTILISATION AND SAVINGS BY ACCOMMODATION TYPE
-- =============================================================================
-- Note on scope: the brief asks for Hostel vs Day Scholar. Accommodation has
-- three values in this data, and the third - PG, 53 students - is a quarter of
-- the sample. Dropping it would quietly report a two-way comparison of 160 of
-- 213 rows, so Q10 reports all three and Q11 makes the requested head-to-head
-- explicit. PG is not a special case of either of the other two.

-- Q10. All three accommodation types, both utilisation measures, and the deficit.
-- Seed data (aggregate utilisation, the defensible measure):
--            Hostel       43 students, 10,709.30 avg spend, 826.01%, deficit 9,412.79
--            Day Scholar 117 students, 11,972.22 avg spend, 724.84%, deficit 10,320.51
--            PG           53 students, 10,943.40 avg spend, 664.76%, deficit 9,297.17
-- THE HEADLINE, AND IT IS COUNTERINTUITIVE: hostel residents spend LESS in
-- absolute terms than day scholars (10,709.30 vs 11,972.22) yet are the WORST
-- performers on utilisation (826.01% vs 724.84%). The reason is the allowance,
-- not the spending: hostel students report a mean allowance of 1,296.51 against
-- 1,651.71 for day scholars, so the same-ish outlay is measured against a much
-- smaller budget. Any conclusion of the form "hostellers are worse with money"
-- is unsupported - the data says their reported budgets are smaller.
-- Hostel is worst on BOTH measures (826.01% aggregate, 1714.08% mean-of-ratios),
-- so the headline survives the aggregation choice. The other two do not: PG is
-- the mildest group on the aggregate (664.76%) but sits ABOVE Day Scholar on the
-- mean-of-ratios (1568.12 vs 1450.63). Any claim about PG relative to day
-- scholars is therefore not safe to make from this data.
SELECT
    accommodation,
    count(*)                                                          AS students,
    round(100.0 * count(*) / sum(count(*)) OVER (), 1)                 AS share_of_students_pct,
    sum(total_expenses)                                               AS total_spend,
    round(avg(total_expenses), 2)                                     AS avg_total_expenses,
    percentile_disc(0.5) WITHIN GROUP (ORDER BY total_expenses)       AS median_total_expenses,
    round(avg(allowance), 2)                                          AS avg_allowance,
    round(100.0 * sum(total_expenses) / nullif(sum(allowance), 0), 2) AS aggregate_utilisation_pct,
    round(avg(budget_usage_pct), 2)                                   AS mean_of_ratios_pct,
    -- Reported as a deficit. savings is the negation of this column and is
    -- negative for all 213 rows, so it cannot be ranked or averaged as
    -- "savings" - a "top savers" query on this table returns nobody.
    round(avg(overspend_amount), 2)                                   AS avg_deficit,
    min(overspend_amount)                                             AS best_case_deficit,
    max(overspend_amount)                                             AS worst_case_deficit,
    count(*) FILTER (WHERE is_overspending)                           AS overspending_students
FROM v_student_budget_analysis
GROUP BY accommodation
ORDER BY avg_total_expenses DESC, accommodation;


-- Q11. The requested Hostel vs Day Scholar head-to-head, with the deltas made
-- explicit so the comparison can be read without re-deriving it.
-- Seed data: hostel spend is 10.5% LOWER (10,709.30 vs 11,972.22) while hostel
-- utilisation is 14.0% HIGHER (826.01% vs 724.84%), because the hostel allowance
-- is 21.5% smaller (1,296.51 vs 1,651.71). Spend and utilisation therefore point
-- in OPPOSITE directions here, which is the whole point of reporting both.
--
-- The ratios are computed with FULL OUTER JOIN so that a missing accommodation
-- type yields NULL rather than silently dropping a row from the comparison.
WITH hostel AS (
    SELECT count(*) AS students,
           sum(total_expenses) AS total_spend,
           avg(total_expenses) AS avg_total_expenses,
           avg(allowance) AS avg_allowance,
           100.0 * sum(total_expenses) / nullif(sum(allowance), 0) AS aggregate_utilisation_pct,
           avg(overspend_amount) AS avg_deficit
    FROM v_student_budget_analysis
    WHERE accommodation = 'Hostel'
),
day_scholar AS (
    SELECT count(*) AS students,
           sum(total_expenses) AS total_spend,
           avg(total_expenses) AS avg_total_expenses,
           avg(allowance) AS avg_allowance,
           100.0 * sum(total_expenses) / nullif(sum(allowance), 0) AS aggregate_utilisation_pct,
           avg(overspend_amount) AS avg_deficit
    FROM v_student_budget_analysis
    WHERE accommodation = 'Day Scholar'
)
SELECT
    h.students                                                          AS hostel_students,
    d.students                                                          AS day_scholar_students,
    round(h.avg_total_expenses, 2)                                      AS hostel_avg_spend,
    round(d.avg_total_expenses, 2)                                      AS day_scholar_avg_spend,
    round(100.0 * (h.avg_total_expenses - d.avg_total_expenses)
              / nullif(d.avg_total_expenses, 0), 1)                      AS spend_delta_pct,
    round(h.avg_allowance, 2)                                           AS hostel_avg_allowance,
    round(d.avg_allowance, 2)                                           AS day_scholar_avg_allowance,
    round(100.0 * (h.avg_allowance - d.avg_allowance)
              / nullif(d.avg_allowance, 0), 1)                           AS allowance_delta_pct,
    round(h.aggregate_utilisation_pct, 2)                               AS hostel_aggregate_utilisation_pct,
    round(d.aggregate_utilisation_pct, 2)                               AS day_scholar_aggregate_utilisation_pct,
    round(100.0 * (h.aggregate_utilisation_pct - d.aggregate_utilisation_pct)
              / nullif(d.aggregate_utilisation_pct, 0), 1)               AS utilisation_delta_pct,
    round(h.avg_deficit, 2)                                             AS hostel_avg_deficit,
    round(d.avg_deficit, 2)                                             AS day_scholar_avg_deficit
FROM hostel AS h
FULL OUTER JOIN day_scholar AS d ON true;


-- Q12. Where the money goes within each accommodation type.
-- The spend MIX is strikingly stable: Academic is the largest category for all
-- three groups (29.6% / 31.2% / 27.2%) and every group sits within a few points
-- of the same four-way split. So accommodation changes the SIZE of the bill, not
-- its composition - and the honest summary of this table is that spending is
-- close to evenly split four ways, not that any one area dominates.
-- Unpivoted with a LATERAL (VALUES ...) join: the four expense columns are
-- stored side by side, and this is the idiomatic PostgreSQL way to turn them back
-- into rows without hard-coding four near-identical UNION ALL branches.
WITH spending AS (
    SELECT v.accommodation, v.total_expenses, area.area, area.amount
    FROM v_student_budget_analysis AS v
    CROSS JOIN LATERAL (
        VALUES
            ('Food',     v.food_expenses),
            ('Transport', v.transport_expenses),
            ('Academic',  v.academic_expenses),
            ('Other',     v.other_expenses)
    ) AS area(area, amount)
)
SELECT
    accommodation,
    area,
    round(avg(amount), 2)                                              AS avg_per_student,
    round(100.0 * sum(amount) / nullif(sum(sum(amount)) OVER (PARTITION BY accommodation), 0), 1)
                                                                          AS share_of_group_spend_pct,
    round(avg(amount) / nullif(avg(total_expenses), 0) * 100, 1)        AS share_of_own_bill_pct
FROM spending
GROUP BY accommodation, area
ORDER BY accommodation, avg_per_student DESC;


-- =============================================================================
-- SECTION 4 - TOP SPENDING AREAS
-- =============================================================================

-- Q13. The four expense categories, ranked, for the whole sample.
-- Seed data: Academic 715,500 (29.3%), Food 601,000 (24.6%), Other 571,000
--            (23.4%), Transport 553,750 (22.7%)
-- THE HONEST READING IS "EVENLY SPLIT", NOT "ACADEMIC DOMINATES". Academic leads
-- by 6.6 percentage points over Transport and 4.7 over Food, on 213 respondents
-- whose category amounts are coarse bucket midpoints - each category takes only
-- 6 or 7 distinct values across the whole sample (distinct_amounts below). A
-- spread this narrow is not separable from the granularity of the instrument.
-- "Academic is the largest single category" is defensible; "students prioritise
-- academics over transport" is not.
-- Every category's max is exactly 6250 (Q14), which is the censored ceiling and
-- the reason the ranking is so compressed.
WITH spending AS (
    SELECT area.area, area.amount
    FROM v_student_budget_analysis AS v
    CROSS JOIN LATERAL (
        VALUES
            ('Food',     v.food_expenses),
            ('Transport', v.transport_expenses),
            ('Academic',  v.academic_expenses),
            ('Other',     v.other_expenses)
    ) AS area(area, amount)
)
SELECT
    area,
    count(*)                                                          AS respondents,
    sum(amount)                                                       AS total_spend,
    round(100.0 * sum(amount) / sum(sum(amount)) OVER (), 1)          AS share_of_total_spend_pct,
    round(avg(amount), 2)                                             AS avg_per_student,
    percentile_disc(0.5) WITHIN GROUP (ORDER BY amount)                AS median_per_student,
    min(amount)                                                       AS min_amount,
    max(amount)                                                       AS max_amount,
    count(DISTINCT amount)                                            AS distinct_amounts
FROM spending
GROUP BY area
ORDER BY total_spend DESC, area;
-- distinct_amounts is 6 for Food and 7 for each of the other three: the entire
-- observed spread of "academic spending" across 213 students is seven survey
-- buckets, from 250 to the censored 6250. Read every mean above with that in
-- mind, and with the caveat that 6250 is a floor.


-- Q14. Confirming the censoring that caps every category.
-- The survey's open-ended top bucket was recorded as its midpoint, so 6250 means
-- "more than 5000", not "6250". This query measures how much of each category
-- sits at that ceiling; the resulting ceiling effect is why Q13's shares are
-- compressed and why every max() is identical.
-- Seed data: Academic 47 respondents (22.1%), Other 26 (12.2%),
--            Transport 23 (10.8%), Food 18 (8.5%); 76 respondents (35.7%) touch
--            the ceiling in at least one category.
-- Academic is the most censored category by a wide margin - nearly a quarter of
-- academic figures are a floor rather than a measurement - so its position at
-- the top of Q13 is the LEAST trustworthy part of that ranking.
SELECT
    count(*)                                                          AS respondents,
    round(100.0 * count(*) FILTER (WHERE academic_expenses  = 6250)
              / count(*), 1)                                           AS academic_at_ceiling_pct,
    round(100.0 * count(*) FILTER (WHERE food_expenses      = 6250)
              / count(*), 1)                                           AS food_at_ceiling_pct,
    round(100.0 * count(*) FILTER (WHERE transport_expenses = 6250)
              / count(*), 1)                                           AS transport_at_ceiling_pct,
    round(100.0 * count(*) FILTER (WHERE other_expenses     = 6250)
              / count(*), 1)                                           AS other_at_ceiling_pct,
    count(*) FILTER (WHERE academic_expenses  = 6250
                       OR food_expenses      = 6250
                       OR transport_expenses = 6250
                       OR other_expenses     = 6250)                  AS rows_touching_the_ceiling,
    round(100.0 * count(*) FILTER (WHERE academic_expenses  = 6250
                                    OR food_expenses      = 6250
                                    OR transport_expenses = 6250
                                    OR other_expenses     = 6250)
              / count(*), 1)                                           AS rows_touching_ceiling_pct
FROM v_student_budget_analysis;
-- Q19 measures the same censoring per CELL rather than per respondent. The two
-- rates are not interchangeable: 13.4% of the 852 expense cells are censored,
-- but that touches 35.7% of respondents, because one respondent can be censored
-- in up to four categories at once. Quote whichever one the claim needs.


-- Q15. Each student's own largest category, with ties counted rather than hidden.
-- Seed data, the category each student leads on (213 students, ties included):
--            Food 68, Academic 58, Transport 50, Other 37
-- 69 of the 213 students (32%) have a TIE for largest category, and 6 students
-- are tied across all four categories at once. The categories draw on only 6 or 7
-- shared bucket values (Q13), so two categories recorded at the same bucket tie by
-- construction - a student who picked the same bucket twice is credited with a
-- "top category" they did not actually single out. This is why the honest
-- per-student question is "how many students have a clear single largest area" -
-- 144 - and not the ranking above.
-- rank() rather than row_number() is deliberate: it returns every tied maximum
-- with rank 1 instead of arbitrarily picking one of them.
WITH spending AS (
    SELECT v.student_id, area.area, area.amount
    FROM v_student_budget_analysis AS v
    CROSS JOIN LATERAL (
        VALUES
            ('Food',     v.food_expenses),
            ('Transport', v.transport_expenses),
            ('Academic',  v.academic_expenses),
            ('Other',     v.other_expenses)
    ) AS area(area, amount)
),
ranked AS (
    SELECT student_id, area, amount,
           rank() OVER (PARTITION BY student_id ORDER BY amount DESC) AS area_rank
    FROM spending
),
top_areas AS (
    SELECT student_id, area
    FROM ranked
    WHERE area_rank = 1
),
tie_counts AS (
    SELECT student_id, count(*) AS areas_tied
    FROM top_areas
    GROUP BY student_id
)
SELECT
    t.area                                                           AS top_area,
    count(*)                                                         AS students_naming_it_top,
    -- The subset for whom it is unambiguously the largest.
    count(*) FILTER (WHERE tc.areas_tied = 1)                        AS students_with_unique_top,
    round(100.0 * count(*) / sum(count(*)) OVER (), 1)                AS share_of_students_pct,
    max(tc.areas_tied)                                               AS worst_case_tie_size
FROM top_areas AS t
JOIN tie_counts AS tc ON tc.student_id = t.student_id
GROUP BY t.area
ORDER BY students_naming_it_top DESC, t.area;


-- Q16. How clear-cut each student's largest area actually is.
-- Seed data: 144 students have one clear largest category, 47 have a two-way tie,
--            16 a three-way tie, 6 a four-way tie (nothing distinguishes them).
-- Read together with Q15, this is the guard rail on any "top spending area per
-- student" claim built on this dataset.
WITH spending AS (
    SELECT v.student_id, area.amount
    FROM v_student_budget_analysis AS v
    CROSS JOIN LATERAL (
        VALUES
            ('Food',     v.food_expenses),
            ('Transport', v.transport_expenses),
            ('Academic',  v.academic_expenses),
            ('Other',     v.other_expenses)
    ) AS area(area, amount)
),
per_student AS (
    -- Count, per student, how many of the four categories sit at that student's
    -- own maximum. Comparing each cell to the row maximum rather than to a fixed
    -- threshold is what makes a 2-way tie and a 4-way tie distinguishable.
    SELECT student_id, count(*) FILTER (WHERE amount = row_max) AS areas_tied
    FROM (
        SELECT student_id, amount,
               max(amount) OVER (PARTITION BY student_id) AS row_max
        FROM spending
    ) AS per_category
    GROUP BY student_id
)
SELECT
    areas_tied                                                        AS categories_tied_for_largest,
    count(*)                                                          AS students,
    round(100.0 * count(*) / sum(count(*)) OVER (), 1)                 AS share_of_students_pct
FROM per_student
GROUP BY areas_tied
ORDER BY areas_tied;


-- Q17. Spending by declared study focus - the other sense of "spending area".
-- "Top spending areas" can also mean what students say they spend on, which is
-- the categorical study_focus dimension rather than the four numeric expense
-- columns. Seed data: Other 13,620.69 (n=29) > Internet & Data Packs 12,486.49
-- (n=37) > Online Learning Platforms 11,528.69 (n=61) > Software Tools
-- 10,795.45 (n=33) > Books & Study Materials 9,900.94 (n=53).
-- The apparent "Other spends most" is a 29-student bucket of undefined content
-- and should not be reported as a finding. Among the four defined areas the
-- spread is 2,586 rupees, about 26% of the lowest, on groups of 33 to 61 -
-- suggestive, not significant, and no test is run here.
SELECT
    study_focus,
    count(*)                                                          AS students,
    round(100.0 * count(*) / sum(count(*)) OVER (), 1)                 AS share_of_students_pct,
    round(avg(total_expenses), 2)                                     AS avg_total_expenses,
    percentile_disc(0.5) WITHIN GROUP (ORDER BY total_expenses)       AS median_total_expenses,
    round(avg(academic_expenses), 2)                                  AS avg_academic_expenses,
    round(100.0 * sum(total_expenses) / nullif(sum(allowance), 0), 2) AS aggregate_utilisation_pct
FROM v_student_budget_analysis
GROUP BY study_focus
ORDER BY avg_total_expenses DESC, study_focus;


-- =============================================================================
-- SECTION 5 - CROSS-CHECKS AGAINST THE PIPELINE'S OWN VERIFIED RANGES
-- =============================================================================
-- src/data_loader.py (Stage 6) and src/data_quality.py (Stage 7) both assert
-- specific ranges for this table. Re-asserting them here means a wrong column
-- mapping or a partial load is caught by the analytics themselves, and not only
-- by the loaders - an aggregate that looks plausible over the wrong column is
-- exactly the failure mode a row count cannot see.

-- Q18. The verification ranges, as a single pass/fail row.
-- Expected: row_count 213, usage 125.00 .. 8600.00, savings -24000 .. -250,
--           overspending 213, savers 0, generated-column mismatches 0.
SELECT
    count(*)                                                          AS row_count,
    count(*) = 213                                                    AS row_count_ok,
    min(budget_usage_pct)                                             AS min_usage_pct,
    max(budget_usage_pct)                                             AS max_usage_pct,
    (min(budget_usage_pct) = 125.00 AND max(budget_usage_pct) = 8600.00) AS usage_range_ok,
    min(savings)                                                      AS min_savings,
    max(savings)                                                      AS max_savings,
    (min(savings) = -24000.00 AND max(savings) = -250.00)              AS savings_range_ok,
    count(*) FILTER (WHERE is_overspending)                           AS overspending_students,
    count(*) FILTER (WHERE NOT is_overspending)                       AS savers,
    -- total_expenses and savings are GENERATED in the schema, so these cannot
    -- drift; the check is here to prove the view is reading the same definitions
    -- the schema enforces.
    count(*) FILTER (
        WHERE total_expenses <> food_expenses + transport_expenses
                            + academic_expenses + other_expenses
           OR savings <> allowance - total_expenses
    )                                                                 AS generated_column_mismatches
FROM v_student_budget_analysis;


-- Q19. Censoring measured per CELL, which is the unit the averages in this file
-- are actually built from. Quote this number whenever an average from Q2, Q3,
-- Q13 or Q17 is presented as an estimate of what students really spend.
-- The four expense columns are unpivoted so the rate is counted over 852 cells.
-- A respondent-level count cannot answer this question: one student can sit at
-- the ceiling in up to four categories, so the cell rate (13.4%) and the
-- respondent rate (35.7%, Q14) are genuinely different numbers.
WITH cells AS (
    SELECT area.area, area.amount
    FROM v_student_budget_analysis AS v
    CROSS JOIN LATERAL (
        VALUES
            ('Food',      v.food_expenses),
            ('Transport', v.transport_expenses),
            ('Academic',  v.academic_expenses),
            ('Other',     v.other_expenses)
    ) AS area(area, amount)
)
SELECT
    count(*)                                                          AS expense_cells,
    count(*) FILTER (WHERE amount = 6250)                             AS censored_cells,
    round(100.0 * count(*) FILTER (WHERE amount = 6250) / count(*), 1) AS censored_cells_pct
FROM cells;
-- Expected: 852 cells, 114 censored, 13.4%. Those 114 cells are FLOORS, not
-- measurements: the true figure for each is somewhere above 5000 and unknown.
-- Every average in this file that includes one is therefore biased downwards,
-- and the bias is not uniform across categories (Q14).


-- Q20. Survey coverage and response mix - the context every result above sits in.
-- Seed data: a 2026-02-21 to 2026-03-21 collection window (28 days), 44 distinct
--            specialization strings, 120 tracking digitally, 126 with a budget
--            plan, 85 in work or an internship, 102 rural / 111 urban.
-- The specialization cardinality is why sql/schema.sql applies no CHECK to that
-- column, and the counts here are why no slice in this file is normalised for
-- background or employment: those splits would be 40-60 rows each, which Q4
-- already shows is the point at which a mean stops being interpretable.
SELECT
    count(*)                                                          AS respondents,
    min(submitted_at)                                                 AS first_response,
    max(submitted_at)                                                 AS last_response,
    max(submitted_at)::date - min(submitted_at)::date                  AS collection_window_days,
    count(DISTINCT specialization)                                     AS distinct_specializations,
    count(*) FILTER (WHERE track_digitally = 'Yes')                    AS track_digitally_yes,
    count(*) FILTER (WHERE budget_plan = 'Yes')                        AS has_budget_plan,
    count(*) FILTER (WHERE work_internship = 'Yes')                    AS in_work_or_internship,
    count(*) FILTER (WHERE background = 'Rural')                       AS rural,
    count(*) FILTER (WHERE background = 'Urban')                       AS urban
FROM v_student_budget_analysis;


-- =============================================================================
-- WHAT THIS FILE DELIBERATELY DOES NOT CONTAIN
-- =============================================================================
-- * No significance tests. With 213 self-selected respondents, only 21 of the 24
--   degree-by-year cells populated at all, and 6 or 7 discrete bucket values per
--   expense category, a p-value would lend false precision to differences the
--   instrument cannot resolve. The cell sizes are reported instead (Q4) and thin
--   cells are flagged there.
-- * No "top savers" or "best budgeters" ranking. savings is negative for all
--   213 rows, so that leaderboard would be empty; Q1 and Q10 report the deficit
--   with a positive sign instead.
-- * No imputation of the empty degree-by-year cells (Q5), and no per-student
--   "top area" claim without its tie count (Q15, Q16).
-- * No claim that any of these differences is causal (header, point 4).
-- =============================================================================
