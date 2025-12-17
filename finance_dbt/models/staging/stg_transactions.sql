{{ config(materialized='table') }}

WITH raw_transactions AS (
    SELECT * 
    FROM {{ source('bronze', 'bronze_transactions') }}
),

cleaned_transactions AS (
    SELECT
        id,
        date::DATE as transaction_date,
        amount::FLOAT as amount,
        TRIM(category) as category,
        TRIM(account) as account,
        CURRENT_TIMESTAMP as loaded_at
    FROM raw_transactions
    WHERE 
        date IS NOT NULL 
        AND amount IS NOT NULL
        AND category IS NOT NULL
),

deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_date, amount, category, account 
            ORDER BY loaded_at DESC
        ) as row_num
    FROM cleaned_transactions
)

SELECT 
    id,
    transaction_date,
    amount,
    category,
    account,
    loaded_at
FROM deduplicated
WHERE row_num = 1
