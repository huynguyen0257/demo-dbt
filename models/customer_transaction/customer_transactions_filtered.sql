{{ config(materialized='view') }}

SELECT
  customer_id,
  transaction_date::date as transaction_date,
  CASE WHEN transaction_type = 'deposit' AND status = 'completed' THEN transaction_amount ELSE 0 END AS deposit_amount,
  CASE WHEN transaction_type = 'withdrawal' AND status = 'completed' THEN transaction_amount ELSE 0 END AS withdrawal_amount,
  running_balance as balance
FROM 
  {{ ref('customer_transactions') }}
WHERE
  status = 'completed';