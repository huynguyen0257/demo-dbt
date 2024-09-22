{{ config(
    materialized='incremental',
    unique_key='customer_id, day_time'
) }}

WITH min_transaction_dates AS (
  SELECT 
    customer_id, 
    MIN(transaction_date)::date AS min_transaction_date
  FROM 
    {{ ref('customer_transactions') }}
  GROUP BY 
    customer_id
), date_series AS (
  SELECT 
    customer_id,
    generate_series(min_transaction_date, CURRENT_DATE, '1 day')::date AS day_time
  FROM 
    min_transaction_dates
), customer_transactions_filtered AS (
  SELECT * FROM {{ ref('customer_transactions_filtered') }}
), daily_balances AS (
  SELECT
    ds.customer_id,
    ds.day_time,
    COALESCE(SUM(ctf.balance), LAG(SUM(ctf.balance)) OVER (PARTITION BY ds.customer_id ORDER BY ds.day_time)) AS balance,
    COALESCE(SUM(ctf.deposit_amount), 0) AS deposit_amount,
    COALESCE(SUM(ctf.withdrawal_amount), 0) AS withdrawal_amount
  FROM 
    date_series ds
  LEFT JOIN 
    customer_transactions_filtered ctf 
  ON 
    ctf.customer_id = ds.customer_id AND ctf.transaction_date = ds.day_time
  GROUP BY 
    ds.customer_id, ds.day_time
)
SELECT
  customer_id,
  day_time,
  balance,
  deposit_amount,
  withdrawal_amount
FROM 
  daily_balances
{% if is_incremental() %}
WHERE day_time >= (SELECT MAX(day_time) FROM {{ this }})
{% endif %}
ORDER BY 
  customer_id, day_time;