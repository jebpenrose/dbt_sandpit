{{config(materialized='table')}}

SELECT recorded_date, narrative, amount, debit_amount, credit_amount, balance
    ,category, spending_method_id
FROM {{ ref('stg_transactions')}}
WHERE temp_row_id NOT IN (
    SELECT to_row_id FROM {{ref('transfers')}}
    UNION
    SELECT from_row_id FROM {{ref('transfers')}}
)
