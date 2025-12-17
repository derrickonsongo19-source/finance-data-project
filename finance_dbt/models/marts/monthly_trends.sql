{{ config(materialized='table') }}

WITH transactions AS (
    SELECT * FROM {{ ref('stg_transactions') }}
),

monthly_data AS (
    SELECT
        DATE_TRUNC('month', transaction_date) as month,
        EXTRACT(YEAR FROM transaction_date) as year,
        EXTRACT(MONTH FROM transaction_date) as month_num,
        category,
        account,
        COUNT(*) as transaction_count,
        SUM(amount) as total_amount,
        SUM(CASE WHEN amount < 0 THEN amount ELSE 0 END) as monthly_expenses,
        SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) as monthly_income
    FROM transactions
    GROUP BY 1, 2, 3, 4, 5
),

monthly_summary AS (
    SELECT
        month,
        year,
        month_num,
        'All' as category,
        'All' as account,
        SUM(transaction_count) as transaction_count,
        SUM(total_amount) as total_amount,
        SUM(monthly_expenses) as monthly_expenses,
        SUM(monthly_income) as monthly_income
    FROM monthly_data
    GROUP BY 1, 2, 3
)

SELECT
    month,
    year,
    month_num,
    category,
    account,
    transaction_count,
    total_amount,
    monthly_expenses,
    monthly_income,
    LAG(monthly_expenses, 1) OVER (PARTITION BY category, account ORDER BY month) as prev_month_expenses,
    monthly_expenses - LAG(monthly_expenses, 1) OVER (PARTITION BY category, account ORDER BY month) as expense_change,
    CASE 
        WHEN LAG(monthly_expenses, 1) OVER (PARTITION BY category, account ORDER BY month) IS NOT NULL
        THEN ROUND(
            (monthly_expenses - LAG(monthly_expenses, 1) OVER (PARTITION BY category, account ORDER BY month)) / 
            ABS(LAG(monthly_expenses, 1) OVER (PARTITION BY category, account ORDER BY month)) * 100, 
            2
        )
        ELSE NULL 
    END as expense_change_percent
FROM monthly_data

UNION ALL

SELECT
    month,
    year,
    month_num,
    category,
    account,
    transaction_count,
    total_amount,
    monthly_expenses,
    monthly_income,
    LAG(monthly_expenses, 1) OVER (ORDER BY month) as prev_month_expenses,
    monthly_expenses - LAG(monthly_expenses, 1) OVER (ORDER BY month) as expense_change,
    CASE 
        WHEN LAG(monthly_expenses, 1) OVER (ORDER BY month) IS NOT NULL
        THEN ROUND(
            (monthly_expenses - LAG(monthly_expenses, 1) OVER (ORDER BY month)) / 
            ABS(LAG(monthly_expenses, 1) OVER (ORDER BY month)) * 100, 
            2
        )
        ELSE NULL 
    END as expense_change_percent
FROM monthly_summary

ORDER BY month DESC, category, account
