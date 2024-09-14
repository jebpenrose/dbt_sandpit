WITH date_spine AS (
    {{dbt_utils.date_spine(datepart="day", start_date="'2011-01-01'::date", end_date="'2029-12-31'")}}
), date_expanded AS (
    -- Add useful columns
    SELECT date_day::date AS date
        ,to_char(date_day, 'Day') AS day_of_week
        ,CAST(to_char(date_day, 'IW') AS INT) AS week_of_year
        ,TO_CHAR(date_day, 'IYYY-IW') AS year_week -- use this for cumulative-years
        ,TO_CHAR(date_day, 'IYYY-Mon') AS year_month
        ,TO_CHAR(date_day, 'Month') AS month_name
        ,TO_CHAR(date_day, 'YYYY') AS year
        ,{{dbt.dateadd(datepart="day", interval=1, from_date_or_timestamp='date_day')}} AS next_date
    FROM date_spine
), ordering AS (
    -- This number is used to order week numbers from one year to the next.
    -- Can be used by BI tools to get last weeks counts even if the year changes
    SELECT date_expanded.*, week_order_cumulative_years
    FROM date_expanded
    JOIN (
        SELECT year_week, ROW_NUMBER() OVER(ORDER BY year_week ASC) AS week_order_cumulative_years
        FROM date_expanded GROUP BY year_week
    ) AS ywo ON date_expanded.year_week=ywo.year_week
)
SELECT * 
FROM ordering