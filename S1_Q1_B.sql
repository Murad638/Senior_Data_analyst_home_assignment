-- ==============================================
-- Query: Weekly Outstanding Balance Evolution
-- Purpose:
--   1. Find top 10 borrowers by latest outstanding balance.
--   2. Track their weekly end-of-week balances for last 6 weeks.
--   3. Compute week-over-week absolute and % changes.
-- ==============================================

WITH base AS (
    -- ==========================================
    -- Step 1: Clean and normalize raw loan data
    -- ==========================================
    SELECT
        TRIM(borrower_id) AS borrower_id,        -- ensure borrower_id has no leading/trailing spaces
        TRIM(loan_id) AS loan_id,                -- clean loan_id
        DATE(loan_issued_at) AS loan_start,      -- ensure proper date format
        DATE(report_date_local) AS report_date,  -- ensure proper date format

        -- normalize balances (no NULLs or negative values)
        CASE 
            WHEN outstanding_balance IS NULL OR outstanding_balance < 0 THEN 0 
            ELSE CAST(outstanding_balance AS REAL) 
        END AS normalized_balance,

        -- normalize repayments (no NULLs or negative values)
        CASE 
            WHEN repaid_amount_day IS NULL OR repaid_amount_day < 0 THEN 0 
            ELSE CAST(repaid_amount_day AS FLOAT) 
        END AS repayment,

        -- calculate days past due = difference between report date and loan issue date
        CASE 
            WHEN loan_issued_at IS NOT NULL AND report_date_local IS NOT NULL
            THEN CAST(JULIANDAY(report_date_local) - JULIANDAY(loan_issued_at) AS INT)
        END AS days_due
    FROM data
    WHERE borrower_id IS NOT NULL 
      AND TRIM(borrower_id) <> ''
      AND loan_id IS NOT NULL 
      AND TRIM(loan_id) <> ''
      AND report_date_local IS NOT NULL 
      AND report_date_local <> ''
      AND loan_issued_at IS NOT NULL 
      AND loan_issued_at <> ''
),

max_date AS (
    -- Step 2: Anchor date = latest available report date in dataset
    SELECT MAX(report_date) AS latest_date
    FROM base
),

latest_balances AS (
    -- Step 3: Get latest total outstanding balance per borrower
    SELECT 
        d.borrower_id,
        SUM(d.normalized_balance) AS total_outstanding
    FROM base d
    JOIN (
        SELECT 
            borrower_id,
            MAX(report_date) AS latest_date
        FROM base
        GROUP BY borrower_id
    ) latest_snapshot 
      ON d.borrower_id = latest_snapshot.borrower_id
     AND d.report_date = latest_snapshot.latest_date
    GROUP BY d.borrower_id
),

top10_borrowers AS (
    -- Step 4: Restrict analysis to top 10 borrowers
    --         by their latest total outstanding balance
    SELECT borrower_id
    FROM latest_balances
    ORDER BY total_outstanding DESC
    LIMIT 10
),

week_end_balance AS (
    -- Step 5: For each borrower + loan + week:
    --         find the latest report date within that week
    --         (acts as the weekly snapshot)
    SELECT 
        d.borrower_id,
        d.loan_id,
        strftime('%Y-%W', d.report_date) AS year_week,  -- ISO year-week identifier
        MAX(d.report_date) AS week_end_date
    FROM base d
    JOIN top10_borrowers t ON d.borrower_id = t.borrower_id
    WHERE CAST(strftime('%W', d.report_date) AS INTEGER) 
              >= CAST(strftime('%W', (SELECT latest_date FROM max_date)) AS INTEGER) - 6
      AND strftime('%Y', d.report_date) = strftime('%Y', (SELECT latest_date FROM max_date))
    GROUP BY d.borrower_id, d.loan_id, strftime('%Y-%W', d.report_date)
),

weekly_totals AS (
    -- Step 6: Aggregate loan-level balances into 
    --         borrower-level weekly totals
    SELECT 
        w.borrower_id,
        w.year_week,
        w.week_end_date,
        SUM(d.normalized_balance) AS total_outstanding
    FROM week_end_balance w
    JOIN base d
      ON w.borrower_id = d.borrower_id
     AND w.loan_id = d.loan_id
     AND w.week_end_date = d.report_date
    GROUP BY w.borrower_id, w.year_week, w.week_end_date
)

-- ==============================================
-- Step 7: Final output
--   - Total balance per borrower per week
--   - Absolute change vs. prior week
--   - % change vs. prior week
-- ==============================================
SELECT 
    wt.*,
    ROUND(
        wt.total_outstanding - LAG(wt.total_outstanding) OVER (
            PARTITION BY wt.borrower_id ORDER BY wt.week_end_date
        ), 2
    ) AS change_abs,
    CASE 
        WHEN LAG(wt.total_outstanding) OVER (
                 PARTITION BY wt.borrower_id ORDER BY wt.week_end_date
             ) > 0
        THEN ROUND(
            100.0 * (wt.total_outstanding - LAG(wt.total_outstanding) OVER (
                         PARTITION BY wt.borrower_id ORDER BY wt.week_end_date
                     )) 
            / LAG(wt.total_outstanding) OVER (
                         PARTITION BY wt.borrower_id ORDER BY wt.week_end_date
                     ), 
            2
        )
        ELSE NULL
    END AS change_pct
FROM weekly_totals wt
ORDER BY wt.borrower_id, wt.week_end_date;
