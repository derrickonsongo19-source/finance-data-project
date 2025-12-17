{{ config(materialized='table') }}

WITH transactions AS (
    SELECT * FROM {{ ref('stg_transactions') }}
),

daily_summary AS (
    SELECT
        transaction_date,
        COUNT(*) as transaction_count,
        SUM(amount) as net_flow,
        SUM(CASE WHEN amount < 0 THEN amount ELSE 0 END) as total_expenses,
        SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) as total_income,
        AVG(amount) as avg_transaction
    FROM transactions
    GROUP BY transaction_date
),

category_analysis AS (
    SELECT
        category,
        COUNT(*) as transaction_count,
        SUM(amount) as total_amount,
        AVG(amount) as avg_amount,
        MIN(amount) as min_amount,
        MAX(amount) as max_amount,
        SUM(CASE WHEN amount < 0 THEN 1 ELSE 0 END) as expense_count,
        SUM(CASE WHEN amount > 0 THEN 1 ELSE 0 END) as income_count
    FROM transactions
    GROUP BY category
),

account_summary AS (
    SELECT
        account,
        COUNT(*) as transaction_count,
        SUM(amount) as current_balance,
        MIN(transaction_date) as first_transaction,
        MAX(transaction_date) as last_transaction
    FROM transactions
    GROUP BY account
)

SELECT
    d.transaction_date,
    d.transaction_count as daily_transactions,
    d.total_expenses,
    d.total_income,
    d.net_flow,
    c.category,
    c.total_amount as category_total,
    a.account,
    a.current_balance,
    CURRENT_TIMESTAMP as generated_at
FROM daily_summary d
CROSS JOIN category_analysis c
CROSS JOIN account_summary a
ORDER BY d.transaction_date DESC, c.total_amount
LIMIT 100  -- Limit for demonstration
