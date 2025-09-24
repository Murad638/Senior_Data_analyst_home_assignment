-- =====================================================
-- Query: Daily Recovery Rate with 3-Week Moving Average
-- Purpose:
--   1. Clean and normalize raw loan data (balances & repayments).
--   2. Aggregate daily total repayments and outstanding balances.
--   3. Compute daily recovery rate (%) per day.
--   4. Calculate 21-day moving average of recovery rate (~3 weeks).
-- =====================================================


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


daily_totals AS (
    -- ==========================================
    -- Step 2: Aggregate daily totals
    --   - Sum repayments and balances per day
    -- ==========================================
    SELECT
        report_date,
        SUM(repayment) AS daily_repayments,
        SUM(normalized_balance) AS daily_balance
    FROM base
    GROUP BY report_date
),

recovery_rates AS (
    -- ==========================================
    -- Step 3: Compute daily recovery rate (%)
    --   - Formula: daily_recovery_rate = daily_repayments ÷ daily_balance * 100
    --   - If daily_balance = 0, recovery_rate = 0
    -- ==========================================
    SELECT
        report_date,
        daily_repayments,
        daily_balance,
        CASE 
            WHEN daily_balance > 0 THEN ROUND(daily_repayments * 100.0 / daily_balance, 4)
            ELSE 0
        END AS recovery_rate
    FROM daily_totals
)

-- ==========================================
-- Step 4: Final output
--   - Compute 21-day moving average of recovery rate (~3 weeks)
--   - Count the number of days in the moving average window
--   - Order output by report date
-- ==========================================
SELECT
    report_date,
    daily_repayments,
    daily_balance,
    recovery_rate,
    ROUND(
        AVG(recovery_rate) OVER (
            ORDER BY report_date
            ROWS BETWEEN 20 PRECEDING AND CURRENT ROW
        ), 4
    ) AS recovery_rate_3week_ma,
    COUNT(*) OVER (
        ORDER BY report_date
        ROWS BETWEEN 20 PRECEDING AND CURRENT ROW
    ) AS window_size
FROM recovery_rates
ORDER BY report_date;
