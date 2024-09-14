


{{ config(materialized='table') }}

SELECT id
    ,spending_method_id
    ,TO_DATE(recorded_date, 'dd/mm/yyyy') AS recorded_date, narrative
    ,{{clean_numeric('debit_amount')}}
    ,{{clean_numeric('credit_amount')}}
    ,{{clean_numeric('balance')}}
    ,categories AS category
FROM {{source('ingest', 'spending')}} AS spending
JOIN {{ref('spending_sources')}} AS spending_srcs ON spending.bank_account=spending_srcs.spending_text


