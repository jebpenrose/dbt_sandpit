

{{ config(materialized='incremental') }}

{% set sql_query %}
SELECT pattern, category, precedence, valid_from, valid_to
FROM {{ref('category_assignment')}}
{% endset %}

{%- set category_patterns = dbt_utils.get_query_results_as_dict(sql_query) -%}

WITH base AS (
    SELECT *
        -- the line below identifies transaction that are contained in 2 different uploads - eg. we extracted from the bank accounts and include April 4th on 2 occasions
        -- we use the file_id to tell them apart
        -- Don't get much more aggressive than this - if I buy 2 coffees from the same shop on the same day, there is no way to tell if the records are duplicates or I just bought 2 coffees.
        ,ROW_NUMBER() OVER(PARTITION BY recorded_date, narrative, debit_amount, credit_amount, balance ORDER BY file_id ASC) AS dedupe_rnk
    FROM {{source('ingest', 'txns_westpac')}} AS transactions
    {% if is_incremental() %}
    WHERE id>(SELECT MAX(id) FROM {{this}})
    {% endif %}
), legacy_categories as (
    SELECT id
        ,TO_DATE(recorded_date, 'dd/mm/yyyy') AS recorded_date, base.narrative
        ,{{clean_numeric('debit_amount')}}
        ,{{clean_numeric('credit_amount')}}
        ,{{clean_numeric('balance')}}
        ,CASE WHEN UPPER(base.categories) != 'OTHER' AND base.categories IS NOT NULL AND LENGTH(base.categories)>0 THEN base.categories ELSE category_lkp.category END AS category
        ,file_id
        ,bank_account
    FROM base
    LEFT OUTER JOIN {{ref('legacy_categories')}} AS category_lkp ON base.narrative=category_lkp.narrative
    WHERE dedupe_rnk=1
), assignments AS (
    SELECT id, category, precedence, ROW_NUMBER() OVER(PARTITION BY id ORDER BY precedence DESC, pattern_len DESC) AS reverse_precedence
    FROM (
        {% for pattern in category_patterns.pattern %}
        SELECT id, CASE WHEN narrative ILIKE '{{ pattern }}' THEN '{{ category_patterns.category[loop.index0] }}' ELSE NULL END AS category, {{ category_patterns.precedence[loop.index0] }} AS precedence, LENGTH('{{ pattern }}') AS pattern_len FROM legacy_categories
        {%- if not loop.last %} UNION ALL{% endif -%}
        {% endfor %}
    ) AS P WHERE category IS NOT NULL
)
SELECT legacy.id, recorded_date, narrative, debit_amount, credit_amount, balance, COALESCE(assignments.category, legacy.category) AS category,spending_method_id
FROM legacy_categories AS legacy
LEFT OUTER JOIN assignments ON legacy.id=assignments.id AND assignments.reverse_precedence=1
LEFT OUTER JOIN {{ref('spending_sources')}} AS srcs ON legacy.bank_account=srcs.spending_text




