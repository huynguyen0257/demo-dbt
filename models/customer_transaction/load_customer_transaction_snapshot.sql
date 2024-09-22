{{ config(materialized='table') }}

WITH date_series AS (
  SELECT 
    (getdate()::date - generate_series)::date AS day_time
  FROM 
    generate_series(1, 365 * 5, 1) -- Generates last 5 years of dates
), customer_transactions_filtered AS (
  SELECT
    customer_id,
    transaction_date::date as transaction_date,
    CASE WHEN transaction_type = 'deposit' AND status = 'completed' THEN transaction_amount ELSE 0 END AS deposit_amount,
    CASE WHEN transaction_type = 'withdrawal' AND status = 'completed' THEN transaction_amount ELSE 0 END AS withdrawal_amount,
    running_balance as balance
  FROM 
    customer_transactions
  WHERE
    status = 'completed'
)
SELECT
  c.customer_id,
  d.day_time,
  COALESCE(SUM(c.balance), LAG(SUM(c.balance)) OVER (PARTITION BY c.customer_id ORDER BY d.day_time)) AS balance,
  COALESCE(SUM(c.deposit_amount), 0) AS deposit_amount,
  COALESCE(SUM(c.withdrawal_amount), 0) AS withdrawal_amount
FROM 
  date_series d
LEFT JOIN 
  customer_transactions_filtered c 
ON 
  c.transaction_date = d.day_time
GROUP BY 
  c.customer_id, d.day_time
ORDER BY 
  c.customer_id, d.day_time;
