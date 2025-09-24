-- ==============================================
-- Query: Loan Portfolio Monitoring & Risk Flags
-- Purpose:
--   1. Identify currently active loans (overdue + high balance).
--   2. Compare balances with 3 months ago.
--   3. Track repayment trends (last two payments).
--   4. Flag payment drop or balance increase risk indicators.
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

latest_snapshot AS (
    -- ==========================================
    -- Step 2: Identify the latest loan record per borrower
    -- ==========================================
    SELECT
        borrower_id,
        loan_id,
        normalized_balance AS balance,   -- use cleaned balance
        days_due,
        report_date,
        repayment,
        ROW_NUMBER() OVER (PARTITION BY borrower_id ORDER BY report_date DESC) AS rn
        -- rn = 1 means most recent report date for that borrower
    FROM base
),

active_loans AS (
    -- ==========================================
    -- Step 3: Filter to "active risky loans"
    --   Conditions:
    --     - latest snapshot (rn = 1)
    --     - overdue by more than 10 days
    --     - balance > 2000
    -- ==========================================
    SELECT *
    FROM latest_snapshot
    WHERE rn = 1
      AND days_due > 10
      AND balance > 2000
),

three_months_ago AS (
    -- ==========================================
    -- Step 4: Get baseline balance ~3 months ago
    --   - Take the minimum report_date within the last 3 months
    --   - This gives earliest available snapshot within the window
    -- ==========================================
    SELECT 
        t.borrower_id,
        b.report_date AS date_3m_ago,
        b.normalized_balance AS balance_3m_ago
    FROM base b
    INNER JOIN (
        SELECT borrower_id, MIN(report_date) AS min_date
        FROM base
        WHERE report_date >= DATE((SELECT MAX(report_date) FROM base), '-3 months')
        GROUP BY borrower_id
    ) t 
      ON b.borrower_id = t.borrower_id 
     AND b.report_date = t.min_date
),

recent_two_payments AS (
    -- ==========================================
    -- Step 5: Identify last two non-zero repayments per borrower
    --   - last_payment = most recent repayment
    --   - prev_payment = one before that
    -- ==========================================
    SELECT
        borrower_id,
        MAX(repayment) FILTER (WHERE rn = 1) AS last_payment,
        MAX(repayment) FILTER (WHERE rn = 2) AS prev_payment
    FROM (
        SELECT
            b.borrower_id,
            b.repayment,
            ROW_NUMBER() OVER (PARTITION BY b.borrower_id ORDER BY b.report_date DESC) AS rn
        FROM base b
        WHERE b.repayment > 0
    )
    WHERE rn <= 2
    GROUP BY borrower_id
)

-- ==========================================
-- Step 6: Final result set with monitoring flags
-- ==========================================
SELECT
    al.borrower_id,
    al.balance AS latest_balance,          -- latest outstanding balance
    tma.balance_3m_ago,                    -- balance ~3 months ago
    tma.date_3m_ago,                       -- date of that balance
    al.days_due,                           -- days since loan issuance
    al.report_date AS latest_report,       -- latest available report date
    
    rtp.last_payment,                      -- most recent repayment amount
    rtp.prev_payment,                      -- previous repayment amount
    
    -- repayment decrease percentage
    ROUND(
        CASE 
            WHEN rtp.prev_payment > 0 
            THEN ((rtp.prev_payment - rtp.last_payment) / rtp.prev_payment) * 100
            ELSE 0 
        END, 2
    ) AS percent_payment_drop,
    
    -- repayment decrease flag (last < 95% of previous)
    CASE 
        WHEN rtp.last_payment < rtp.prev_payment * 0.95 THEN 1 
        ELSE 0 
    END AS payment_drop_flag,
   
    -- balance increase flag (balance grew vs. 3 months ago)
    CASE
        WHEN al.balance IS NOT NULL 
         AND tma.balance_3m_ago IS NOT NULL 
         AND al.balance > tma.balance_3m_ago
        THEN 1
        ELSE 0
    END AS balance_increased_flag

FROM active_loans al
LEFT JOIN recent_two_payments rtp ON al.borrower_id = rtp.borrower_id
LEFT JOIN three_months_ago tma ON al.borrower_id = tma.borrower_id
ORDER BY al.balance DESC;
