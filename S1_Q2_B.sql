-- =====================================================
-- Query: Daily Recovery Rate per Borrower & Loan
-- Purpose:
--   1. Compute daily recovery rate per borrower and loan.
--   2. Track running totals of repayments and days past due.
--   3. Flag days when recovery rate worsens (vs previous day).
-- =====================================================

WITH cleaned AS (
    -- ==========================================
    -- Step 1: Clean and normalize raw loan data
    -- ==========================================
    SELECT
        TRIM(borrower_id) AS borrower_id,
        TRIM(loan_id) AS loan_id,
        DATE(report_date_local) AS report_date,
        
        -- normalize outstanding balance
        CASE 
            WHEN outstanding_balance IS NULL OR outstanding_balance < 0
                THEN 0.0
            ELSE CAST(outstanding_balance AS REAL)
        END AS balance,

        -- normalize repayment
        CASE
            WHEN repaid_amount_day IS NULL OR repaid_amount_day < 0
                THEN 0.0
            ELSE CAST(repaid_amount_day AS REAL)
        END AS repayment,

        -- calculate days past due
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
      AND loan_issued_at IS NOT NULL
      --AND loan_id = '0d2dbdcce8fb3573d436062ed57e49e9bc6f816863e9ccb60bdd40cde71ad178'

),

daily_rates AS (
    -- ==========================================
    -- Step 2: Compute recovery rate per borrower + loan + day
    -- ==========================================
    SELECT
        borrower_id,
        loan_id,
        report_date,
        repayment,
        balance,
        days_due,
        CASE
            WHEN balance > 0 THEN ROUND(repayment * 100.0 / balance, 4)
            ELSE 0
        END AS recovery_rate
    FROM cleaned
),

running_totals AS (
    -- ==========================================
    -- Step 3: Running totals per borrower over time
    --   - Cumulative repayments
    --   - Cumulative days past due
    -- ==========================================
    SELECT
        borrower_id,
        loan_id,
        report_date,
        repayment,
        balance,
        days_due,
        recovery_rate,

        SUM(repayment) OVER (
            PARTITION BY borrower_id 
            ORDER BY report_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_repayments,

        SUM(days_due) OVER (
            PARTITION BY borrower_id 
            ORDER BY report_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_days_due
    FROM daily_rates
)

-- ==========================================
-- Step 4: Add recovery rate deterioration flag
--   - Flag = 1 if today's recovery rate < yesterday's recovery rate
-- ==========================================
SELECT
    borrower_id,
    loan_id,
    report_date,
    repayment,
    balance,
    days_due,
    recovery_rate,
    cumulative_repayments,
    cumulative_days_due,

    CASE
        WHEN recovery_rate < LAG(recovery_rate) OVER (
                 PARTITION BY borrower_id, loan_id ORDER BY report_date
             ) THEN 1
        ELSE 0
    END AS recovery_worsened_flag
FROM running_totals
ORDER BY borrower_id, loan_id, report_date;
