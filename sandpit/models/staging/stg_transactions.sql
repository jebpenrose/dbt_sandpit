{{config(materialized='table')}}

{% set sql_query %}
SELECT pattern, category, precedence, valid_from, valid_to
FROM {{ref('category_assignment')}}
{% endset %}

{%- set category_patterns = dbt_utils.get_query_results_as_dict(sql_query) -%}

WITH gather AS (
    -- This CTE will:
    --  a) read from each transaction source
    --  b) conform all names
    --  c) ensure amount column always populated
    --  d) ensure debit_amount column always positive
    SELECT id, recorded_date, narrative
        , COALESCE((0-debit_amount),credit_amount) AS amount
        , ABS(debit_amount) AS debit_amount, credit_amount, balance, category, spending_method_id
    FROM {{ref('stg_westpac')}}

    UNION ALL 

    SELECT 
        id, recorded_date, narrative
        , COALESCE((0-debit_amount),credit_amount) AS amount
        , ABS(debit_amount) AS debit_amount
        , credit_amount
        ,balance
        ,category
        ,spending_method_id
    FROM {{ref('stg_spending')}}

), collated AS (
    -- This CTE creates temp_row_id to help with filtering and join'ing in later steps
    SELECT *
        ,ROW_NUMBER() OVER (ORDER BY id, recorded_date, narrative, amount, debit_amount, credit_amount, balance, category, spending_method_id) AS temp_row_id -- it will reset every time we run!
    FROM gather
), assignments AS (
    -- Generic assignments from seed: category_assignment.csv
    -- There may be multiple matches out of this list - or none.
    SELECT temp_row_id, category, precedence, ROW_NUMBER() OVER(PARTITION BY temp_row_id ORDER BY precedence DESC, pattern_len DESC) AS reverse_precedence
    FROM (
        {% for pattern in category_patterns.pattern %}
        SELECT temp_row_id, CASE WHEN narrative ILIKE '{{ pattern }}' THEN '{{ category_patterns.category[loop.index0] }}' ELSE NULL END AS category
            , {{ category_patterns.precedence[loop.index0] }} AS precedence, LENGTH('{{ pattern }}') AS pattern_len 
            FROM collated
        {%- if not loop.last %} UNION ALL{% endif -%}
        {% endfor %}
    ) AS P WHERE category IS NOT NULL
), transaction_categories_applied AS (
    -- Merge "assignments" CTE back in. Many records won't have an entry from it.
    SELECT collated.id, collated.recorded_date, collated.narrative, amount, debit_amount, credit_amount, balance
        ,COALESCE(assignments.category, CASE WHEN collated.category IN (SELECT name FROM {{ref('category')}}) THEN collated.category ELSE NULL END) AS category
        ,spending_method_id
        ,collated.temp_row_id -- it will reset every time we run!
    FROM collated
    LEFT OUTER JOIN assignments ON collated.temp_row_id=assignments.temp_row_id AND assignments.reverse_precedence=1
)
    -- Use this block to replace old category names.
    SELECT id, recorded_date, narrative, amount, debit_amount, credit_amount, balance
        ,CASE WHEN category ILIKE 'MEMBERSHIPS' THEN 'SUBSCRIPTIONS'
            ELSE category 
            END AS category
        ,spending_method_id
        ,temp_row_id
    FROM transaction_categories_applied
