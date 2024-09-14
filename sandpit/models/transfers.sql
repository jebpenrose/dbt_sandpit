{{config(materialized='table')}}

WITH base AS (
    SELECT * FROM {{ref('stg_transactions')}}
)
SELECT righty.temp_row_id AS from_row_id
    ,lefty.temp_row_id AS to_row_id
    ,ABS(lefty.amount) AS amount
    ,lefty.recorded_date AS transfer_date
    ,righty.spending_method_id AS from_spending_method_id
    ,lefty.spending_method_id AS to_spending_method_id
FROM base AS lefty 
JOIN base AS righty ON lefty.recorded_date=righty.recorded_date 
    AND ((lefty.credit_amount-righty.debit_amount=0))
    AND lefty.spending_method_id != righty.spending_method_id
    AND lefty.temp_row_id != righty.temp_row_id
WHERE lefty.credit_amount IS NOT NULL AND lefty.credit_amount > 0
    AND righty.debit_amount IS NOT NULL
    AND righty.spending_method_id NOT IN (
            SELECT id 
            FROM {{ref('spending_method')}} 
            WHERE spending_method IN ('Credit Card', 'Cash')
    )