{{ config(
    materialized='incremental',
    unique_key=['customer_id', 'day_time']
) }}


WITH today_transactions AS (
    -- Get today’s new transactions (or all transactions in the case of full-refresh)
    SELECT
        customer_id,
        SUM(CASE WHEN transaction_type = 'deposit' THEN transaction_amount ELSE 0 END) AS total_deposit,
        SUM(CASE WHEN transaction_type = 'withdrawal' THEN transaction_amount ELSE 0 END) AS total_withdrawal,
        SUM(balance_change) AS balance_change
    FROM {{ source('my_database', 'customer_transactions') }}
    {% if is_incremental() %}
        WHERE transaction_date >= (SELECT MAX(dbt_updated_at) FROM {{ this }})
    {% endif %}
    GROUP BY customer_id
),

previous_day AS (
    -- Handle case where there is no previous day data in the first run
    {% if is_incremental() %}
        SELECT 
            customer_id,
            balance,
            deposit_amount,
            withdrawal_amount,
            day_time
        FROM {{ this }}
        WHERE day_time = (SELECT MAX(day_time) FROM {{ this }})
    {% else %}
        SELECT
            NULL::VARCHAR AS customer_id,
            NULL::FLOAT AS balance,
            NULL::FLOAT AS deposit_amount,
            NULL::FLOAT AS withdrawal_amount,
            NULL::DATE AS day_time
        LIMIT 0
    {% endif %}
),

daily_customer_tracking AS (
    -- Track daily data, merging previous day's data with today’s transactions
    SELECT
        COALESCE(prev.customer_id, today.customer_id) AS customer_id,
        CURRENT_DATE AS day_time,
        COALESCE(prev.balance, 0) + COALESCE(today.balance_change, 0) AS balance,
        COALESCE(prev.deposit_amount, 0) + COALESCE(today.total_deposit, 0) AS deposit_amount,
        COALESCE(prev.withdrawal_amount, 0) + COALESCE(today.total_withdrawal, 0) AS withdrawal_amount,
        CURRENT_TIMESTAMP AS dbt_updated_at
    FROM previous_day prev
        FULL JOIN today_transactions today ON prev.customer_id = today.customer_id
)

SELECT * FROM daily_customer_tracking
